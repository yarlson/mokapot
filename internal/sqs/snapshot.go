package sqs

import (
	"encoding/json"
	"fmt"
	"time"
)

type queueSnapshot struct {
	Name         string            `json:"name"`
	URL          string            `json:"url"`
	ARN          string            `json:"arn"`
	Attributes   map[string]string `json:"attributes"`
	CreatedAt    time.Time         `json:"created_at"`
	LastPurgedAt time.Time         `json:"last_purged_at"`
	Messages     []messageSnapshot `json:"messages"`
}

type messageAttributeSnapshot struct {
	DataType    string `json:"data_type"`
	StringValue string `json:"string_value,omitempty"`
	BinaryValue []byte `json:"binary_value,omitempty"`
}

type messageSnapshot struct {
	MessageID              string                              `json:"message_id"`
	Body                   string                              `json:"body"`
	MD5OfBody              string                              `json:"md5_of_body"`
	MD5OfMessageAttributes string                              `json:"md5_of_message_attributes,omitempty"`
	MessageAttributes      map[string]messageAttributeSnapshot `json:"message_attributes,omitempty"`
	SentTimestamp          int64                               `json:"sent_timestamp"`
	ReceiveCount           int                                 `json:"receive_count"`
	FirstReceivedAt        int64                               `json:"first_received_at"`
	ReceiptHandle          string                              `json:"receipt_handle"`
	InvisibleUntil         time.Time                           `json:"invisible_until"`
	AvailableAt            time.Time                           `json:"available_at"`
}

// Snapshot serializes the entire SQS engine state to JSON.
func (e *Engine) Snapshot() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	queues := make([]queueSnapshot, 0, len(e.queues))
	for _, q := range e.queues {
		qs := snapshotQueue(q)
		queues = append(queues, qs)
	}

	return json.Marshal(queues)
}

func snapshotQueue(q *Queue) queueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()

	qs := queueSnapshot{
		Name:         q.Name,
		URL:          q.URL,
		ARN:          q.ARN,
		Attributes:   make(map[string]string, len(q.Attributes)),
		CreatedAt:    q.createdAt,
		LastPurgedAt: q.lastPurgedAt,
	}

	for k, v := range q.Attributes {
		qs.Attributes[k] = v
	}

	for _, msg := range q.available {
		qs.Messages = append(qs.Messages, snapshotMessage(msg))
	}
	for _, msg := range q.inflight {
		qs.Messages = append(qs.Messages, snapshotMessage(msg))
	}

	return qs
}

func snapshotMessage(msg *Message) messageSnapshot {
	ms := messageSnapshot{
		MessageID:              msg.MessageID,
		Body:                   msg.Body,
		MD5OfBody:              msg.MD5OfBody,
		MD5OfMessageAttributes: msg.MD5OfMessageAttributes,
		SentTimestamp:          msg.SentTimestamp,
		ReceiveCount:           msg.ReceiveCount,
		FirstReceivedAt:        msg.FirstReceivedAt,
		ReceiptHandle:          msg.ReceiptHandle,
		InvisibleUntil:         msg.InvisibleUntil,
		AvailableAt:            msg.AvailableAt,
	}
	if len(msg.MessageAttributes) > 0 {
		ms.MessageAttributes = make(map[string]messageAttributeSnapshot, len(msg.MessageAttributes))
		for k, v := range msg.MessageAttributes {
			ms.MessageAttributes[k] = messageAttributeSnapshot(v)
		}
	}
	return ms
}

// Restore loads engine state from a JSON snapshot.
// Existing state is replaced entirely.
// Messages whose visibility has expired are moved back to available.
func (e *Engine) Restore(data []byte) error {
	var queues []queueSnapshot
	if err := json.Unmarshal(data, &queues); err != nil {
		return fmt.Errorf("unmarshal SQS snapshot: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.now()
	e.queues = make(map[string]*Queue, len(queues))
	e.queuesByARN = make(map[string]*Queue, len(queues))

	for _, qs := range queues {
		attrs := qs.Attributes
		if attrs == nil {
			attrs = make(map[string]string)
		}
		q := &Queue{
			Name:         qs.Name,
			URL:          qs.URL,
			ARN:          qs.ARN,
			Attributes:   attrs,
			inflight:     make(map[string]*Message),
			createdAt:    qs.CreatedAt,
			lastPurgedAt: qs.LastPurgedAt,
		}

		for _, ms := range qs.Messages {
			msg := &Message{
				MessageID:              ms.MessageID,
				Body:                   ms.Body,
				MD5OfBody:              ms.MD5OfBody,
				MD5OfMessageAttributes: ms.MD5OfMessageAttributes,
				SentTimestamp:          ms.SentTimestamp,
				ReceiveCount:           ms.ReceiveCount,
				FirstReceivedAt:        ms.FirstReceivedAt,
				ReceiptHandle:          ms.ReceiptHandle,
				InvisibleUntil:         ms.InvisibleUntil,
				AvailableAt:            ms.AvailableAt,
			}
			if len(ms.MessageAttributes) > 0 {
				msg.MessageAttributes = make(map[string]MessageAttribute, len(ms.MessageAttributes))
				for k, v := range ms.MessageAttributes {
					msg.MessageAttributes[k] = MessageAttribute(v)
				}
			}

			// Classify message: expired inflight → available, active inflight → inflight, else → available.
			switch {
			case msg.ReceiptHandle != "" && now.After(msg.InvisibleUntil):
				msg.ReceiptHandle = ""
				msg.InvisibleUntil = time.Time{}
				q.available = append(q.available, msg)
			case msg.ReceiptHandle != "":
				q.inflight[msg.ReceiptHandle] = msg
			default:
				q.available = append(q.available, msg)
			}
		}

		e.queues[qs.Name] = q
		e.queuesByARN[q.ARN] = q
	}

	return nil
}
