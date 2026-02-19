package sqs

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Message represents an SQS message in the engine.
type Message struct {
	MessageID    string
	Body         string
	MD5OfBody    string
	SentTimestamp int64
	ReceiveCount int
	FirstReceivedAt int64

	ReceiptHandle  string
	InvisibleUntil time.Time
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
	createdAt time.Time
}

// Engine manages all SQS queues in memory.
type Engine struct {
	mu     sync.RWMutex
	queues map[string]*Queue // queueName -> Queue

	region    string
	accountID string
	host      string
}

// NewEngine creates a new SQS engine.
func NewEngine(region, accountID, host string) *Engine {
	return &Engine{
		queues:    make(map[string]*Queue),
		region:    region,
		accountID: accountID,
		host:      host,
	}
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
			"CreatedTimestamp":              fmt.Sprintf("%d", time.Now().Unix()),
			"LastModifiedTimestamp":         fmt.Sprintf("%d", time.Now().Unix()),
			"QueueArn":                      fmt.Sprintf("arn:aws:sqs:%s:%s:%s", e.region, e.accountID, name),
		},
		inflight:  make(map[string]*Message),
		createdAt: time.Now(),
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
		SentTimestamp: time.Now().UnixMilli(),
	}

	q.mu.Lock()
	q.available = append(q.available, msg)
	q.mu.Unlock()

	return msg, nil
}

// ReceiveMessage retrieves up to maxMessages from a queue.
func (e *Engine) ReceiveMessage(queueName string, maxMessages int, visibilityTimeout int) ([]*Message, error) {
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

	now := time.Now()

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
