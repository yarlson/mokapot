package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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

// Message represents an SQS message in the engine.
type Message struct {
	MessageID       string
	Body            string
	MD5OfBody       string
	SentTimestamp   int64
	ReceiveCount    int
	FirstReceivedAt int64

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
	mu     sync.RWMutex
	queues map[string]*Queue // queueName -> Queue

	region    string
	accountID string
	host      string
	now       func() time.Time
}

// NewEngine creates a new SQS engine.
func NewEngine(region, accountID, host string) *Engine {
	return &Engine{
		queues:    make(map[string]*Queue),
		region:    region,
		accountID: accountID,
		host:      host,
		now:       time.Now,
	}
}

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
	return q, nil
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
	for _, q := range e.queues {
		if q.ARN == arn {
			return q
		}
	}
	return nil
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
func (e *Engine) SetQueueAttributes(queueName string, attrs map[string]string) error {
	e.mu.RLock()
	q, exists := e.queues[queueName]
	e.mu.RUnlock()

	if !exists {
		return ErrQueueDoesNotExist
	}

	// Validate all attribute keys are mutable.
	for k := range attrs {
		if !mutableAttributes[k] {
			return fmt.Errorf("%w: %s is not a settable attribute", ErrInvalidParameterValue, k)
		}
	}

	// Validate numeric attributes.
	for attr, bounds := range numericAttributeRanges {
		if v, ok := attrs[attr]; ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < bounds[0] || n > bounds[1] {
				return fmt.Errorf("%w: Value for parameter %s is invalid. Reason: Must be between %d and %d", ErrInvalidParameterValue, attr, bounds[0], bounds[1])
			}
		}
	}

	// Validate RedrivePolicy if present.
	if rpJSON, ok := attrs["RedrivePolicy"]; ok {
		rp, err := parseRedrivePolicy(rpJSON)
		if err != nil {
			return fmt.Errorf("%w: RedrivePolicy: %w", ErrInvalidParameterValue, err)
		}
		// Verify the DLQ target exists
		e.mu.RLock()
		dlq := e.queueByARN(rp.DeadLetterTargetARN)
		e.mu.RUnlock()
		if dlq == nil {
			return fmt.Errorf("%w: deadLetterTargetArn %s does not exist", ErrInvalidParameterValue, rp.DeadLetterTargetARN)
		}
	}

	q.mu.Lock()
	for k, v := range attrs {
		q.Attributes[k] = v
	}
	q.mu.Unlock()
	return nil
}

// SendMessage adds a message to a queue.
// delaySeconds overrides the queue-level DelaySeconds. Pass -1 to use the queue default.
//
// Note: the engine read-lock is released before acquiring the queue lock.
// If DeleteQueue is added, it must ensure in-flight operations on the queue
// complete before removing it from the map (e.g. use queue.mu as a barrier).
func (e *Engine) SendMessage(queueName, body string, delaySeconds int) (*Message, error) {
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
		MessageID:     uuid.New().String(),
		Body:          body,
		MD5OfBody:     md5Hash(body),
		SentTimestamp: now.UnixMilli(),
	}

	if delaySeconds > 0 {
		msg.AvailableAt = now.Add(time.Duration(delaySeconds) * time.Second)
	}

	q.mu.Lock()
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

// receiveFromQueue attempts to dequeue messages from a queue without blocking.
// If a RedrivePolicy is configured and a message exceeds maxReceiveCount,
// it is moved to the dead-letter queue instead of being returned.
func (e *Engine) receiveFromQueue(q *Queue, maxMessages, visibilityTimeout int, nowFn func() time.Time) []*Message {
	now := nowFn()

	q.mu.Lock()

	// Parse RedrivePolicy under the queue lock.
	var rp *redrivePolicy
	if rpStr := q.Attributes["RedrivePolicy"]; rpStr != "" {
		var rpErr error
		rp, rpErr = parseRedrivePolicy(rpStr)
		if rpErr != nil {
			slog.Warn("invalid RedrivePolicy on queue, ignoring", "queue", q.Name, "err", rpErr)
		}
	}

	// Requeue expired inflight messages
	for handle, msg := range q.inflight {
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

	// Unlock source queue before touching the DLQ to avoid lock ordering issues.
	if len(dlqMessages) > 0 {
		q.mu.Unlock()
		e.moveToDLQ(rp.DeadLetterTargetARN, dlqMessages)
	} else {
		q.mu.Unlock()
	}

	return result
}

// moveToDLQ enqueues messages into the dead-letter queue identified by ARN.
func (e *Engine) moveToDLQ(dlqARN string, messages []*Message) {
	e.mu.RLock()
	dlq := e.queueByARN(dlqARN)
	e.mu.RUnlock()

	if dlq == nil {
		ids := make([]string, len(messages))
		for i, m := range messages {
			ids[i] = m.MessageID
		}
		slog.Warn("DLQ not found, dropping messages", "dlqArn", dlqARN, "count", len(messages), "messageIds", ids)
		return
	}

	dlq.mu.Lock()
	for _, msg := range messages {
		// Reset inflight state; preserve body, attributes, and receive count.
		msg.ReceiptHandle = ""
		msg.InvisibleUntil = time.Time{}
		msg.AvailableAt = time.Time{}
		dlq.available = append(dlq.available, msg)
		slog.Info("message moved to DLQ", "messageId", msg.MessageID, "dlq", dlq.Name, "receiveCount", msg.ReceiveCount)
	}
	dlq.notifyWaiters()
	dlq.mu.Unlock()
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
	ID        string // caller-supplied entry ID
	MessageID string
	MD5OfBody string
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
	ID           string
	Body         string
	DelaySeconds int // -1 means use queue default
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

		msg, err := e.SendMessage(queueName, entry.Body, entry.DelaySeconds)
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
			ID:        entry.ID,
			MessageID: msg.MessageID,
			MD5OfBody: msg.MD5OfBody,
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
