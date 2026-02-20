package sns

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

type topicSnapshot struct {
	Name          string                 `json:"name"`
	ARN           string                 `json:"arn"`
	Attributes    map[string]string      `json:"attributes"`
	Subscriptions []subscriptionSnapshot `json:"subscriptions"`
}

type subscriptionSnapshot struct {
	SubscriptionARN string            `json:"subscription_arn"`
	TopicARN        string            `json:"topic_arn"`
	Protocol        string            `json:"protocol"`
	Endpoint        string            `json:"endpoint"`
	Attributes      map[string]string `json:"attributes"`
}

// Snapshot serializes the entire SNS engine state to JSON.
func (e *Engine) Snapshot() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	topics := make([]topicSnapshot, 0, len(e.topics))
	for _, t := range e.topics {
		ts := snapshotTopic(t)
		topics = append(topics, ts)
	}

	return json.Marshal(topics)
}

func snapshotTopic(t *Topic) topicSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	ts := topicSnapshot{
		Name:       t.Name,
		ARN:        t.ARN,
		Attributes: make(map[string]string, len(t.Attributes)),
	}

	for k, v := range t.Attributes {
		ts.Attributes[k] = v
	}

	for _, sub := range t.Subscriptions {
		sub.mu.RLock()
		ss := subscriptionSnapshot{
			SubscriptionARN: sub.SubscriptionARN,
			TopicARN:        sub.TopicARN,
			Protocol:        sub.Protocol,
			Endpoint:        sub.Endpoint,
			Attributes:      make(map[string]string, len(sub.Attributes)),
		}
		for k, v := range sub.Attributes {
			ss.Attributes[k] = v
		}
		sub.mu.RUnlock()
		ts.Subscriptions = append(ts.Subscriptions, ss)
	}

	return ts
}

// Restore loads engine state from a JSON snapshot.
// Existing state is replaced entirely.
// Filter policies are re-parsed from subscription attributes.
func (e *Engine) Restore(data []byte) error {
	var topics []topicSnapshot
	if err := json.Unmarshal(data, &topics); err != nil {
		return fmt.Errorf("unmarshal SNS snapshot: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.topics = make(map[string]*Topic, len(topics))
	e.topicsByARN = make(map[string]*Topic, len(topics))
	e.subscriptionsByARN = make(map[string]*Subscription)

	for _, ts := range topics {
		t := &Topic{
			Name:       ts.Name,
			ARN:        ts.ARN,
			Attributes: ts.Attributes,
		}

		for _, ss := range ts.Subscriptions {
			sub := &Subscription{
				SubscriptionARN: ss.SubscriptionARN,
				TopicARN:        ss.TopicARN,
				Protocol:        ss.Protocol,
				Endpoint:        ss.Endpoint,
				Attributes:      ss.Attributes,
			}

			// Re-parse cached filter policy.
			if fpJSON := sub.Attributes["FilterPolicy"]; fpJSON != "" {
				parsed, err := parseFilterPolicy(fpJSON)
				if err != nil {
					slog.Warn("invalid FilterPolicy on restore, ignoring", "subscriptionArn", sub.SubscriptionARN, "err", err)
				} else {
					sub.cachedFilterPolicy = parsed
				}
			}

			t.Subscriptions = append(t.Subscriptions, sub)
			e.subscriptionsByARN[sub.SubscriptionARN] = sub
		}

		e.topics[ts.Name] = t
		e.topicsByARN[t.ARN] = t
	}

	return nil
}
