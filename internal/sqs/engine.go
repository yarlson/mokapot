package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// redrivePolicy is the parsed form of a queue's RedrivePolicy attribute.
type redrivePolicy struct {
	DeadLetterTargetARN string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

// parseRedrivePolicy parses a RedrivePolicy JSON string.
// Returns nil if the string is empty.
func parseRedrivePolicy(s string) (*redrivePolicy, error) {
	var rp redrivePolicy
	if err := json.Unmarshal([]byte(s), &rp); err != nil {
		return nil, fmt.Errorf("invalid RedrivePolicy JSON: %w", err)
	}
	if rp.MaxReceiveCount < 1 {
		return nil, fmt.Errorf("maxReceiveCount must be >= 1, got %d", rp.MaxReceiveCount)
	}
	if rp.DeadLetterTargetARN == "" {
		return nil, fmt.Errorf("deadLetterTargetArn is required")
	}
	return &rp, nil
}

// MessageAttribute represents a typed SQS message attribute.
type MessageAttribute struct {
	DataType    string // "String", "Number", "Binary", "String.custom", etc.
	StringValue string
	BinaryValue []byte
}

// Message represents an SQS message in the engine.
type Message struct {
	MessageID              string
	Body                   string
	MD5OfBody              string
	MD5OfMessageAttributes string
	MessageAttributes      map[string]MessageAttribute
	SentTimestamp          int64
	ReceiveCount           int
	FirstReceivedAt        int64

	ReceiptHandle  string
	InvisibleUntil time.Time
	AvailableAt    time.Time // zero value means immediately available
}

// waiter represents a long-polling ReceiveMessage caller waiting for messages.
type waiter struct {
	ch chan struct{}
}

// Queue represents an in-memory SQS queue.
//
// Attributes must not be mutated directly after queue creation.
// Use SetAttribute to modify attributes safely under the queue lock.
type Queue struct {
	Name       string
	URL        string
	ARN        string
	Attributes map[string]string

	mu           sync.Mutex
	available    []*Message
	inflight     map[string]*Message // receiptHandle -> message
	waiters      []*waiter
	createdAt    time.Time
	lastPurgedAt time.Time
}

// SetAttribute updates a single queue attribute under the queue lock.
func (q *Queue) SetAttribute(key, value string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.Attributes[key] = value
}

// Engine manages all SQS queues in memory.
type Engine struct {
	mu          sync.RWMutex
	queues      map[string]*Queue // queueName -> Queue
	queuesByARN map[string]*Queue // queueARN -> Queue

	region    string
	accountID string
	host      string
	now       func() time.Time
}

// NewEngine creates a new SQS engine.
func NewEngine(region, accountID, host string) *Engine {
	return &Engine{
		queues:      make(map[string]*Queue),
		queuesByARN: make(map[string]*Queue),
		region:      region,
		accountID:   accountID,
		host:        host,
		now:         time.Now,
	}
}

// Lock acquires the engine write lock.
// Use with SnapshotLocked for coordinated cross-engine snapshots.
func (e *Engine) Lock() { e.mu.Lock() }

// Unlock releases the engine write lock.
func (e *Engine) Unlock() { e.mu.Unlock() }

// SetClock overrides the time source used by the engine.
// This is intended for tests that need deterministic time control.
func (e *Engine) SetClock(fn func() time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = fn
}

// SetHost updates the host used for generating queue URLs.
// This is intended for tests where the listen address is not known at construction time.
func (e *Engine) SetHost(host string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.host = host
}

// CreateQueue creates a new queue or returns existing if attributes match.
func (e *Engine) CreateQueue(name string) (*Queue, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if q, exists := e.queues[name]; exists {
		return q, nil
	}

	q := &Queue{
		Name: name,
		URL:  fmt.Sprintf("http://%s/%s/%s", e.host, e.accountID, name),
		ARN:  fmt.Sprintf("arn:aws:sqs:%s:%s:%s", e.region, e.accountID, name),
		Attributes: map[string]string{
			"VisibilityTimeout":             "30",
			"ReceiveMessageWaitTimeSeconds": "0",
			"DelaySeconds":                  "0",
			"MessageRetentionPeriod":        "345600",
			"CreatedTimestamp":              fmt.Sprintf("%d", e.now().Unix()),
			"LastModifiedTimestamp":         fmt.Sprintf("%d", e.now().Unix()),
			"QueueArn":                      fmt.Sprintf("arn:aws:sqs:%s:%s:%s", e.region, e.accountID, name),
		},
		inflight:  make(map[string]*Message),
		createdAt: e.now(),
	}

	e.queues[name] = q
	e.queuesByARN[q.ARN] = q
	return q, nil
}

// ListQueues returns the URLs of all queues, optionally filtered by a name prefix.
func (e *Engine) ListQueues(prefix string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	urls := make([]string, 0, len(e.queues))
	for name, q := range e.queues {
		if prefix == "" || strings.HasPrefix(name, prefix) {
			urls = append(urls, q.URL)
		}
	}
	sort.Strings(urls)
	return urls
}

// DeleteQueue removes a queue by name.
func (e *Engine) DeleteQueue(name string) error {
	e.mu.Lock()
	q, exists := e.queues[name]
	if !exists {
		e.mu.Unlock()
		return ErrQueueDoesNotExist
	}
	delete(e.queues, name)
	delete(e.queuesByARN, q.ARN)
	e.mu.Unlock()

	// Acquire queue lock as a barrier to ensure no in-flight operations remain.
	q.mu.Lock()
	defer q.mu.Unlock()

	slog.Info("queue deleted", "queue", name)
	return nil
}

// GetQueueURL returns the URL for a queue by name.
func (e *Engine) GetQueueURL(name string) (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	q, exists := e.queues[name]
	if !exists {
		return "", ErrQueueDoesNotExist
	}
	return q.URL, nil
}

// GetQueueVisibilityTimeout returns the default visibility timeout for a queue.
func (e *Engine) GetQueueVisibilityTimeout(name string) (int, error) {
	e.mu.RLock()
	q, exists := e.queues[name]
	e.mu.RUnlock()

	if !exists {
		return 0, ErrQueueDoesNotExist
	}
	q.mu.Lock()
	vt, err := strconv.Atoi(q.Attributes["VisibilityTimeout"])
	q.mu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("invalid VisibilityTimeout: %w", err)
	}
	return vt, nil
}

// GetQueueWaitTimeSeconds returns the default receive wait time for a queue.
func (e *Engine) GetQueueWaitTimeSeconds(name string) (int, error) {
	e.mu.RLock()
	q, exists := e.queues[name]
	e.mu.RUnlock()

	if !exists {
		return 0, ErrQueueDoesNotExist
	}
	q.mu.Lock()
	wt, err := strconv.Atoi(q.Attributes["ReceiveMessageWaitTimeSeconds"])
	q.mu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("invalid ReceiveMessageWaitTimeSeconds: %w", err)
	}
	return wt, nil
}

// queueByARN returns a queue by its ARN. Must be called with e.mu held (read or write).
func (e *Engine) queueByARN(arn string) *Queue {
	return e.queuesByARN[arn]
}

// GetQueueAttributes returns the requested attributes for a queue.
// If attrNames contains "All", all attributes are returned.
func (e *Engine) GetQueueAttributes(queueName string, attrNames []string) (map[string]string, error) {
	e.mu.RLock()
	q, exists := e.queues[queueName]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrQueueDoesNotExist
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	all := false
	for _, n := range attrNames {
		if n == "All" {
			all = true
			break
		}
	}

	result := make(map[string]string)
	if all {
		for k, v := range q.Attributes {
			result[k] = v
		}
	} else {
		for _, n := range attrNames {
			if v, ok := q.Attributes[n]; ok {
				result[n] = v
			}
		}
	}
	return result, nil
}

// mutableAttributes defines which queue attributes can be set via SetQueueAttributes.
var mutableAttributes = map[string]bool{
	"VisibilityTimeout":             true,
	"ReceiveMessageWaitTimeSeconds": true,
	"DelaySeconds":                  true,
	"MessageRetentionPeriod":        true,
	"RedrivePolicy":                 true,
}

// numericAttributeRanges defines valid ranges for numeric queue attributes.
var numericAttributeRanges = map[string][2]int{
	"VisibilityTimeout":             {0, 43200},
	"ReceiveMessageWaitTimeSeconds": {0, 20},
	"DelaySeconds":                  {0, 900},
	"MessageRetentionPeriod":        {60, 1209600},
}

// SetQueueAttributes sets attributes on a queue.
// Uses a write lock to make DLQ validation and attribute set atomic,
// preventing the DLQ from being deleted between validation and set.
func (e *Engine) SetQueueAttributes(queueName string, attrs map[string]string) error {
	// Validate attribute keys and numeric ranges before acquiring locks.
	for k := range attrs {
		if !mutableAttributes[k] {
			return fmt.Errorf("%w: %s is not a settable attribute", ErrInvalidParameterValue, k)
		}
	}

	for attr, bounds := range numericAttributeRanges {
		if v, ok := attrs[attr]; ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < bounds[0] || n > bounds[1] {
				return fmt.Errorf("%w: Value for parameter %s is invalid. Reason: Must be between %d and %d", ErrInvalidParameterValue, attr, bounds[0], bounds[1])
			}
		}
	}

	// Parse RedrivePolicy early to fail fast on invalid JSON.
	var rp *redrivePolicy
	if rpJSON, ok := attrs["RedrivePolicy"]; ok {
		var err error
		rp, err = parseRedrivePolicy(rpJSON)
		if err != nil {
			return fmt.Errorf("%w: RedrivePolicy: %w", ErrInvalidParameterValue, err)
		}
	}

	// Hold engine lock for the DLQ existence check + attribute set to prevent
	// the DLQ from being deleted between validation and set (TOCTOU).
	e.mu.Lock()
	q, exists := e.queues[queueName]
	if !exists {
		e.mu.Unlock()
		return ErrQueueDoesNotExist
	}

	if rp != nil {
		dlq := e.queueByARN(rp.DeadLetterTargetARN)
		if dlq == nil {
			e.mu.Unlock()
			return fmt.Errorf("%w: deadLetterTargetArn %s does not exist", ErrInvalidParameterValue, rp.DeadLetterTargetARN)
		}
	}

	q.mu.Lock()
	for k, v := range attrs {
		q.Attributes[k] = v
	}
	q.mu.Unlock()
	e.mu.Unlock()
	return nil
}

// SendMessage adds a message to a queue.
// delaySeconds overrides the queue-level DelaySeconds. Pass -1 to use the queue default.
// attrs may be nil if no message attributes are provided.
//
// Note: the engine read-lock is released before acquiring the queue lock.
// If DeleteQueue is added, it must ensure in-flight operations on the queue
// complete before removing it from the map (e.g. use queue.mu as a barrier).
func (e *Engine) SendMessage(queueName, body string, delaySeconds int, attrs map[string]MessageAttribute) (*Message, error) {
	e.mu.RLock()
	q, exists := e.queues[queueName]
	nowFn := e.now
	e.mu.RUnlock()

	if !exists {
		return nil, ErrQueueDoesNotExist
	}

	if delaySeconds > 900 {
		return nil, fmt.Errorf("%w: Value for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900", ErrInvalidParameterValue)
	}

	now := nowFn()

	if delaySeconds < 0 {
		q.mu.Lock()
		ds, err := strconv.Atoi(q.Attributes["DelaySeconds"])
		q.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("invalid DelaySeconds: %w", err)
		}
		delaySeconds = ds
	}

	msg := &Message{
		MessageID:         uuid.New().String(),
		Body:              body,
		MD5OfBody:         md5Hash(body),
		MessageAttributes: attrs,
		SentTimestamp:     now.UnixMilli(),
	}
	if len(attrs) > 0 {
		msg.MD5OfMessageAttributes = md5OfMessageAttributes(attrs)
	}

	if delaySeconds > 0 {
		msg.AvailableAt = now.Add(time.Duration(delaySeconds) * time.Second)
	}

	q.mu.Lock()
	if len(q.available)+len(q.inflight) >= MaxMessagesPerQueue {
		q.mu.Unlock()
		return nil, fmt.Errorf("%w: queue %s has reached maximum capacity (%d messages)", ErrOverLimit, queueName, MaxMessagesPerQueue)
	}
	q.available = append(q.available, msg)
	q.notifyWaiters()
	q.mu.Unlock()

	return msg, nil
}

// nextInflightExpiry returns the earliest InvisibleUntil time among inflight messages,
// or the zero value if there are none. Used to wake long-polling waiters when visibility expires.
func (q *Queue) nextInflightExpiry() time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()

	var earliest time.Time
	for _, msg := range q.inflight {
		if earliest.IsZero() || msg.InvisibleUntil.Before(earliest) {
			earliest = msg.InvisibleUntil
		}
	}
	return earliest
}

// nextDelayedAvailability returns the earliest future AvailableAt time among delayed
// messages in the available slice, or the zero value if there are none. Used to wake
// long-polling waiters when delayed messages become receivable.
func (q *Queue) nextDelayedAvailability(now time.Time) time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()

	var earliest time.Time
	for _, msg := range q.available {
		if msg.AvailableAt.IsZero() || !msg.AvailableAt.After(now) {
			continue
		}
		if earliest.IsZero() || msg.AvailableAt.Before(earliest) {
			earliest = msg.AvailableAt
		}
	}
	return earliest
}

// notifyWaiters wakes all long-polling waiters. Must be called with q.mu held.
func (q *Queue) notifyWaiters() {
	for _, w := range q.waiters {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

// ReceiveMessage retrieves up to maxMessages from a queue.
// If waitTimeSeconds > 0 and no messages are available, it blocks until messages
// arrive or the wait time elapses (long polling). The context can cancel the wait.
func (e *Engine) ReceiveMessage(ctx context.Context, queueName string, maxMessages, visibilityTimeout, waitTimeSeconds int) ([]*Message, error) {
	e.mu.RLock()
	q, exists := e.queues[queueName]
	nowFn := e.now
	e.mu.RUnlock()

	if !exists {
		return nil, ErrQueueDoesNotExist
	}

	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		maxMessages = 10
	}

	// Try to receive immediately.
	result := e.receiveFromQueue(q, maxMessages, visibilityTimeout, nowFn)
	if len(result) > 0 || waitTimeSeconds <= 0 {
		return result, nil
	}

	// Long polling: register a waiter and block.
	w := &waiter{ch: make(chan struct{}, 1)}
	q.mu.Lock()
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()

	defer func() {
		q.mu.Lock()
		for i, existing := range q.waiters {
			if existing == w {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		q.mu.Unlock()
	}()

	deadline := nowFn().Add(time.Duration(waitTimeSeconds) * time.Second)
	for {
		now := nowFn()
		remaining := deadline.Sub(now)
		if remaining <= 0 {
			return nil, nil
		}

		// If inflight messages will expire before the deadline, wake early to requeue them.
		if nextExpiry := q.nextInflightExpiry(); !nextExpiry.IsZero() {
			if d := nextExpiry.Sub(now); d > 0 && d < remaining {
				remaining = d
			}
		}

		// If delayed messages will become available before the deadline, wake early.
		// Note: notifyWaiters() is called on every SendMessage, so even if a new
		// delayed message arrives with a shorter delay after this check, the waiter
		// will be woken, re-loop, and recalculate the timer.
		if nextDelay := q.nextDelayedAvailability(now); !nextDelay.IsZero() {
			if d := nextDelay.Sub(now); d > 0 && d < remaining {
				remaining = d
			}
		}

		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil
		case <-w.ch:
			timer.Stop()
		case <-timer.C:
		}

		result = e.receiveFromQueue(q, maxMessages, visibilityTimeout, nowFn)
		if len(result) > 0 {
			return result, nil
		}

		// Recompute deadline from engine clock (supports test clock injection).
		if !nowFn().Before(deadline) {
			return nil, nil
		}
	}
}

// isMessageExpired checks whether a message has exceeded its queue's retention period.
func isMessageExpired(msg *Message, retentionSeconds int, now time.Time) bool {
	sentAt := time.UnixMilli(msg.SentTimestamp)
	return now.After(sentAt.Add(time.Duration(retentionSeconds) * time.Second))
}

// retentionSeconds returns the MessageRetentionPeriod for a queue in seconds.
// Must be called with q.mu held.
func (q *Queue) retentionSeconds() int {
	if s := q.Attributes["MessageRetentionPeriod"]; s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return 345600 // default 4 days
}

// removeExpiredMessages removes messages exceeding the retention period from
// both available and inflight pools. Returns the number of messages removed.
func (q *Queue) removeExpiredMessages(now time.Time) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	retention := q.retentionSeconds()
	removed := 0

	remaining := make([]*Message, 0, len(q.available))
	for _, msg := range q.available {
		if isMessageExpired(msg, retention, now) {
			removed++
			continue
		}
		remaining = append(remaining, msg)
	}
	q.available = remaining

	for handle, msg := range q.inflight {
		if isMessageExpired(msg, retention, now) {
			delete(q.inflight, handle)
			removed++
		}
	}

	if removed > 0 {
		slog.Debug("expired messages removed", "queue", q.Name, "count", removed)
	}

	return removed
}

// CleanupExpiredMessages removes messages that have exceeded their queue's
// MessageRetentionPeriod from all queues. Returns the total number of messages removed.
// This is intended to be called periodically to prevent memory accumulation.
func (e *Engine) CleanupExpiredMessages() int {
	e.mu.RLock()
	queues := make([]*Queue, 0, len(e.queues))
	for _, q := range e.queues {
		queues = append(queues, q)
	}
	nowFn := e.now
	e.mu.RUnlock()

	now := nowFn()
	total := 0
	for _, q := range queues {
		total += q.removeExpiredMessages(now)
	}
	return total
}

// receiveFromQueue attempts to dequeue messages from a queue without blocking.
// If a RedrivePolicy is configured and a message exceeds maxReceiveCount,
// it is moved to the dead-letter queue instead of being returned.
//
// The DLQ reference is resolved outside the queue lock (under engine RLock),
// then the DLQ insertion is performed while still holding the source queue lock.
// This eliminates the window where messages exist in neither queue.
// Lock nesting: source queue.mu → dlq.mu (no engine lock held during nesting).
func (e *Engine) receiveFromQueue(q *Queue, maxMessages, visibilityTimeout int, nowFn func() time.Time) []*Message {
	now := nowFn()

	// Pre-resolve DLQ reference so we can move messages atomically.
	q.mu.Lock()
	rpStr := q.Attributes["RedrivePolicy"]
	q.mu.Unlock()

	var rp *redrivePolicy
	var dlq *Queue
	if rpStr != "" {
		var err error
		rp, err = parseRedrivePolicy(rpStr)
		if err != nil {
			slog.Warn("invalid RedrivePolicy on queue, ignoring", "queue", q.Name, "err", err)
		} else {
			e.mu.RLock()
			dlq = e.queueByARN(rp.DeadLetterTargetARN)
			e.mu.RUnlock()
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	retention := q.retentionSeconds()

	// Requeue expired inflight messages; discard retention-expired ones.
	for handle, msg := range q.inflight {
		if isMessageExpired(msg, retention, now) {
			delete(q.inflight, handle)
			continue
		}
		if now.After(msg.InvisibleUntil) {
			msg.ReceiptHandle = ""
			q.available = append(q.available, msg)
			delete(q.inflight, handle)
		}
	}

	var result []*Message
	var dlqMessages []*Message
	remaining := make([]*Message, 0, len(q.available))

	for _, msg := range q.available {
		// Discard retention-expired messages.
		if isMessageExpired(msg, retention, now) {
			continue
		}

		// Skip messages that are still delayed
		if !msg.AvailableAt.IsZero() && now.Before(msg.AvailableAt) {
			remaining = append(remaining, msg)
			continue
		}

		if len(result) >= maxMessages {
			remaining = append(remaining, msg)
			continue
		}

		msg.ReceiveCount++
		if msg.FirstReceivedAt == 0 {
			msg.FirstReceivedAt = now.UnixMilli()
		}

		// Check DLQ threshold: if receive count exceeds maxReceiveCount, move to DLQ.
		if rp != nil && msg.ReceiveCount > rp.MaxReceiveCount {
			dlqMessages = append(dlqMessages, msg)
			continue
		}

		handle := uuid.New().String()
		msg.ReceiptHandle = handle
		msg.InvisibleUntil = now.Add(time.Duration(visibilityTimeout) * time.Second)

		q.inflight[handle] = msg
		result = append(result, msg)
	}

	q.available = remaining

	// Move DLQ messages atomically while still holding the source queue lock.
	if len(dlqMessages) > 0 {
		if dlq != nil {
			dlq.mu.Lock()
			for _, msg := range dlqMessages {
				msg.ReceiptHandle = ""
				msg.InvisibleUntil = time.Time{}
				msg.AvailableAt = time.Time{}
				dlq.available = append(dlq.available, msg)
				slog.Info("message moved to DLQ", "messageId", msg.MessageID, "dlq", dlq.Name, "receiveCount", msg.ReceiveCount)
			}
			dlq.notifyWaiters()
			dlq.mu.Unlock()
		} else {
			ids := make([]string, len(dlqMessages))
			for i, m := range dlqMessages {
				ids[i] = m.MessageID
			}
			slog.Warn("DLQ not found, dropping messages", "dlqArn", rp.DeadLetterTargetARN, "count", len(dlqMessages), "messageIds", ids)
		}
	}

	return result
}

// PurgeQueue removes all messages (available and inflight) from the named queue.
// Like real SQS, it enforces a 60-second cooldown between purges on the same queue.
func (e *Engine) PurgeQueue(queueName string) error {
	e.mu.RLock()
	q, exists := e.queues[queueName]
	nowFn := e.now
	e.mu.RUnlock()

	if !exists {
		return ErrQueueDoesNotExist
	}

	now := nowFn()

	q.mu.Lock()
	if !q.lastPurgedAt.IsZero() && now.Sub(q.lastPurgedAt) < 60*time.Second {
		q.mu.Unlock()
		return ErrPurgeQueueInProgress
	}
	q.available = nil
	q.inflight = make(map[string]*Message)
	q.lastPurgedAt = now
	q.notifyWaiters()
	q.mu.Unlock()

	slog.Info("queue purged", "queue", queueName)
	return nil
}

// ChangeMessageVisibility updates the visibility timeout of an inflight message.
// If visibilityTimeout is 0, the message becomes immediately available again.
// The visibilityTimeout must be between 0 and 43200 seconds.
func (e *Engine) ChangeMessageVisibility(queueName, receiptHandle string, visibilityTimeout int) error {
	if visibilityTimeout < 0 || visibilityTimeout > 43200 {
		return fmt.Errorf("%w: Value for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200", ErrInvalidParameterValue)
	}

	e.mu.RLock()
	q, exists := e.queues[queueName]
	nowFn := e.now
	e.mu.RUnlock()

	if !exists {
		return ErrQueueDoesNotExist
	}

	now := nowFn()

	q.mu.Lock()
	msg, ok := q.inflight[receiptHandle]
	if !ok {
		q.mu.Unlock()
		return ErrReceiptHandleIsInvalid
	}

	if visibilityTimeout == 0 {
		// Move message back to available immediately.
		delete(q.inflight, receiptHandle)
		msg.ReceiptHandle = ""
		msg.InvisibleUntil = time.Time{}
		q.available = append(q.available, msg)
		q.notifyWaiters()
	} else {
		msg.InvisibleUntil = now.Add(time.Duration(visibilityTimeout) * time.Second)
	}
	q.mu.Unlock()

	return nil
}

// ChangeMessageVisibilityBatchEntry is a single entry in a ChangeMessageVisibilityBatch request.
type ChangeMessageVisibilityBatchEntry struct {
	ID                string
	ReceiptHandle     string
	VisibilityTimeout int
}

// ChangeMessageVisibilityBatchResult holds the results of a ChangeMessageVisibilityBatch call.
type ChangeMessageVisibilityBatchResult struct {
	Successful []BatchResultEntry
	Failed     []BatchError
}

// ChangeMessageVisibilityBatch changes the visibility timeout of up to 10 messages in one call.
func (e *Engine) ChangeMessageVisibilityBatch(queueName string, entries []ChangeMessageVisibilityBatchEntry) (*ChangeMessageVisibilityBatchResult, error) {
	e.mu.RLock()
	_, exists := e.queues[queueName]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrQueueDoesNotExist
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: The batch request must contain at least one entry", ErrEmptyBatchRequest)
	}
	if len(entries) > 10 {
		return nil, fmt.Errorf("%w: Maximum number of entries per request is 10", ErrTooManyEntriesInBatchRequest)
	}

	// Check for duplicate IDs.
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if entry.ID == "" {
			return nil, fmt.Errorf("%w: A batch entry id is required for each message in the batch", ErrInvalidParameterValue)
		}
		if seen[entry.ID] {
			return nil, fmt.Errorf("%w: Id %s is not unique within the request", ErrBatchEntryIdsNotDistinct, entry.ID)
		}
		seen[entry.ID] = true
	}

	result := &ChangeMessageVisibilityBatchResult{}
	for _, entry := range entries {
		err := e.ChangeMessageVisibility(queueName, entry.ReceiptHandle, entry.VisibilityTimeout)
		if err != nil {
			code := "InternalError"
			msg := err.Error()
			senderFault := false
			if errors.Is(err, ErrReceiptHandleIsInvalid) {
				code = "ReceiptHandleIsInvalid"
				msg = "The input receipt handle is invalid."
				senderFault = true
			} else if errors.Is(err, ErrInvalidParameterValue) {
				code = "InvalidParameterValue"
				msg = sanitizeErrorMessage(err)
				senderFault = true
			}
			result.Failed = append(result.Failed, BatchError{
				ID:          entry.ID,
				SenderFault: senderFault,
				Code:        code,
				Message:     msg,
			})
			continue
		}

		result.Successful = append(result.Successful, BatchResultEntry{
			ID: entry.ID,
		})
	}

	return result, nil
}

// DeleteMessage removes an inflight message by receipt handle.
func (e *Engine) DeleteMessage(queueName, receiptHandle string) error {
	e.mu.RLock()
	q, exists := e.queues[queueName]
	e.mu.RUnlock()

	if !exists {
		return ErrQueueDoesNotExist
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.inflight[receiptHandle]; !ok {
		return ErrReceiptHandleIsInvalid
	}

	delete(q.inflight, receiptHandle)
	return nil
}

// BatchResultEntry represents a successful entry in a batch response.
type BatchResultEntry struct {
	ID                     string // caller-supplied entry ID
	MessageID              string
	MD5OfBody              string
	MD5OfMessageAttributes string
}

// BatchError represents a failed entry in a batch response.
type BatchError struct {
	ID          string
	SenderFault bool
	Code        string
	Message     string
}

// SendMessageBatchEntry is a single entry in a SendMessageBatch request.
type SendMessageBatchEntry struct {
	ID                string
	Body              string
	DelaySeconds      int // -1 means use queue default
	MessageAttributes map[string]MessageAttribute
}

// SendMessageBatchResult holds the results of a SendMessageBatch call.
type SendMessageBatchResult struct {
	Successful []BatchResultEntry
	Failed     []BatchError
}

// SendMessageBatch sends up to 10 messages to a queue in one call.
func (e *Engine) SendMessageBatch(queueName string, entries []SendMessageBatchEntry) (*SendMessageBatchResult, error) {
	e.mu.RLock()
	_, exists := e.queues[queueName]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrQueueDoesNotExist
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: The batch request must contain at least one entry", ErrEmptyBatchRequest)
	}
	if len(entries) > 10 {
		return nil, fmt.Errorf("%w: Maximum number of entries per request is 10", ErrTooManyEntriesInBatchRequest)
	}

	// Check for duplicate IDs.
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if entry.ID == "" {
			return nil, fmt.Errorf("%w: A batch entry id is required for each message in the batch", ErrInvalidParameterValue)
		}
		if seen[entry.ID] {
			return nil, fmt.Errorf("%w: Id %s is not unique within the request", ErrBatchEntryIdsNotDistinct, entry.ID)
		}
		seen[entry.ID] = true
	}

	result := &SendMessageBatchResult{}
	for _, entry := range entries {
		if entry.DelaySeconds > 900 {
			result.Failed = append(result.Failed, BatchError{
				ID:          entry.ID,
				SenderFault: true,
				Code:        "InvalidParameterValue",
				Message:     "Value for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900.",
			})
			continue
		}

		msg, err := e.SendMessage(queueName, entry.Body, entry.DelaySeconds, entry.MessageAttributes)
		if err != nil {
			result.Failed = append(result.Failed, BatchError{
				ID:          entry.ID,
				SenderFault: true,
				Code:        "InternalError",
				Message:     err.Error(),
			})
			continue
		}

		result.Successful = append(result.Successful, BatchResultEntry{
			ID:                     entry.ID,
			MessageID:              msg.MessageID,
			MD5OfBody:              msg.MD5OfBody,
			MD5OfMessageAttributes: msg.MD5OfMessageAttributes,
		})
	}

	return result, nil
}

// DeleteMessageBatchEntry is a single entry in a DeleteMessageBatch request.
type DeleteMessageBatchEntry struct {
	ID            string
	ReceiptHandle string
}

// DeleteMessageBatchResult holds the results of a DeleteMessageBatch call.
type DeleteMessageBatchResult struct {
	Successful []BatchResultEntry
	Failed     []BatchError
}

// DeleteMessageBatch deletes up to 10 messages from a queue in one call.
func (e *Engine) DeleteMessageBatch(queueName string, entries []DeleteMessageBatchEntry) (*DeleteMessageBatchResult, error) {
	e.mu.RLock()
	_, exists := e.queues[queueName]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrQueueDoesNotExist
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: The batch request must contain at least one entry", ErrEmptyBatchRequest)
	}
	if len(entries) > 10 {
		return nil, fmt.Errorf("%w: Maximum number of entries per request is 10", ErrTooManyEntriesInBatchRequest)
	}

	// Check for duplicate IDs.
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if entry.ID == "" {
			return nil, fmt.Errorf("%w: A batch entry id is required for each message in the batch", ErrInvalidParameterValue)
		}
		if seen[entry.ID] {
			return nil, fmt.Errorf("%w: Id %s is not unique within the request", ErrBatchEntryIdsNotDistinct, entry.ID)
		}
		seen[entry.ID] = true
	}

	result := &DeleteMessageBatchResult{}
	for _, entry := range entries {
		err := e.DeleteMessage(queueName, entry.ReceiptHandle)
		if err != nil {
			code := "InternalError"
			msg := err.Error()
			senderFault := false
			if errors.Is(err, ErrReceiptHandleIsInvalid) {
				code = "ReceiptHandleIsInvalid"
				msg = "The input receipt handle is invalid."
				senderFault = true
			}
			result.Failed = append(result.Failed, BatchError{
				ID:          entry.ID,
				SenderFault: senderFault,
				Code:        code,
				Message:     msg,
			})
			continue
		}

		result.Successful = append(result.Successful, BatchResultEntry{
			ID: entry.ID,
		})
	}

	return result, nil
}
