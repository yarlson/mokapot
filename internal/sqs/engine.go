package sqs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

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
}

// waiter represents a long-polling ReceiveMessage caller waiting for messages.
type waiter struct {
	ch chan struct{}
}

// Queue represents an in-memory SQS queue.
//
// Attributes is immutable after queue creation. Do not modify directly;
// if SetQueueAttributes is added, it must hold q.mu.
type Queue struct {
	Name       string
	URL        string
	ARN        string
	Attributes map[string]string

	mu        sync.Mutex
	available []*Message
	inflight  map[string]*Message // receiptHandle -> message
	waiters   []*waiter
	createdAt time.Time
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
	defer e.mu.RUnlock()

	q, exists := e.queues[name]
	if !exists {
		return 0, ErrQueueDoesNotExist
	}
	vt, _ := strconv.Atoi(q.Attributes["VisibilityTimeout"])
	return vt, nil
}

// GetQueueWaitTimeSeconds returns the default receive wait time for a queue.
func (e *Engine) GetQueueWaitTimeSeconds(name string) (int, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	q, exists := e.queues[name]
	if !exists {
		return 0, ErrQueueDoesNotExist
	}
	wt, _ := strconv.Atoi(q.Attributes["ReceiveMessageWaitTimeSeconds"])
	return wt, nil
}

// SendMessage adds a message to a queue.
//
// Note: the engine read-lock is released before acquiring the queue lock.
// If DeleteQueue is added, it must ensure in-flight operations on the queue
// complete before removing it from the map (e.g. use queue.mu as a barrier).
func (e *Engine) SendMessage(queueName, body string) (*Message, error) {
	e.mu.RLock()
	q, exists := e.queues[queueName]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrQueueDoesNotExist
	}

	msg := &Message{
		MessageID:     uuid.New().String(),
		Body:          body,
		MD5OfBody:     md5Hash(body),
		SentTimestamp: e.now().UnixMilli(),
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
	result := e.receiveFromQueue(q, maxMessages, visibilityTimeout)
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

	deadline := e.now().Add(time.Duration(waitTimeSeconds) * time.Second)
	for {
		remaining := deadline.Sub(e.now())
		if remaining <= 0 {
			return nil, nil
		}

		// If inflight messages will expire before the deadline, wake early to requeue them.
		if nextExpiry := q.nextInflightExpiry(); !nextExpiry.IsZero() {
			if d := nextExpiry.Sub(e.now()); d > 0 && d < remaining {
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

		result = e.receiveFromQueue(q, maxMessages, visibilityTimeout)
		if len(result) > 0 {
			return result, nil
		}

		// Recompute deadline from engine clock (supports test clock injection).
		if !e.now().Before(deadline) {
			return nil, nil
		}
	}
}

// receiveFromQueue attempts to dequeue messages from a queue without blocking.
func (e *Engine) receiveFromQueue(q *Queue, maxMessages, visibilityTimeout int) []*Message {
	now := e.now()

	q.mu.Lock()
	defer q.mu.Unlock()

	// Requeue expired inflight messages
	for handle, msg := range q.inflight {
		if now.After(msg.InvisibleUntil) {
			msg.ReceiptHandle = ""
			q.available = append(q.available, msg)
			delete(q.inflight, handle)
		}
	}

	var result []*Message
	remaining := make([]*Message, 0, len(q.available))

	for _, msg := range q.available {
		if len(result) >= maxMessages {
			remaining = append(remaining, msg)
			continue
		}

		msg.ReceiveCount++
		if msg.FirstReceivedAt == 0 {
			msg.FirstReceivedAt = now.UnixMilli()
		}

		handle := uuid.New().String()
		msg.ReceiptHandle = handle
		msg.InvisibleUntil = now.Add(time.Duration(visibilityTimeout) * time.Second)

		q.inflight[handle] = msg
		result = append(result, msg)
	}

	q.available = remaining
	return result
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
