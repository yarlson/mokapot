package sns

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// EnqueueFunc delivers a message body to a named SQS queue.
// Used by the SNS engine for fanout delivery without depending on the sqs package.
type EnqueueFunc func(queueName, body string) error

// Topic represents an in-memory SNS topic.
type Topic struct {
	Name          string
	ARN           string
	Attributes    map[string]string
	Subscriptions []*Subscription

	mu sync.Mutex
}

// Subscription represents an SNS subscription to an SQS queue.
type Subscription struct {
	SubscriptionARN string
	TopicARN        string
	Protocol        string // "sqs" only
	Endpoint        string // queue ARN

	mu                 sync.RWMutex
	Attributes         map[string]string
	cachedFilterPolicy filterPolicy
}

// Engine manages all SNS topics and subscriptions in memory.
type Engine struct {
	mu                 sync.RWMutex
	topics             map[string]*Topic        // topicName -> Topic
	topicsByARN        map[string]*Topic        // topicARN -> Topic
	subscriptionsByARN map[string]*Subscription // subscriptionARN -> Subscription

	region    string
	accountID string
	enqueue   EnqueueFunc
	now       func() time.Time
}

// NewEngine creates a new SNS engine.
func NewEngine(region, accountID string, enqueue EnqueueFunc) *Engine {
	return &Engine{
		topics:             make(map[string]*Topic),
		topicsByARN:        make(map[string]*Topic),
		subscriptionsByARN: make(map[string]*Subscription),
		region:             region,
		accountID:          accountID,
		enqueue:            enqueue,
		now:                time.Now,
	}
}

// SetClock overrides the time source used by the engine.
func (e *Engine) SetClock(fn func() time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = fn
}

// CreateTopic creates a new topic or returns existing if name matches.
func (e *Engine) CreateTopic(name string) (*Topic, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Topic name is required", ErrInvalidParameter)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if t, exists := e.topics[name]; exists {
		return t, nil
	}

	t := &Topic{
		Name: name,
		ARN:  fmt.Sprintf("arn:aws:sns:%s:%s:%s", e.region, e.accountID, name),
		Attributes: map[string]string{
			"TopicArn": fmt.Sprintf("arn:aws:sns:%s:%s:%s", e.region, e.accountID, name),
		},
	}

	e.topics[name] = t
	e.topicsByARN[t.ARN] = t
	return t, nil
}

// topicByARN returns a topic by its ARN. Must be called with e.mu held.
func (e *Engine) topicByARN(arn string) *Topic {
	return e.topicsByARN[arn]
}

// Subscribe adds an SQS subscription to a topic.
func (e *Engine) Subscribe(topicARN, protocol, endpoint string) (*Subscription, error) {
	if protocol != "sqs" {
		return nil, fmt.Errorf("%w: Only sqs protocol is supported", ErrInvalidParameter)
	}
	if endpoint == "" {
		return nil, fmt.Errorf("%w: Endpoint is required", ErrInvalidParameter)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	topic := e.topicByARN(topicARN)
	if topic == nil {
		return nil, ErrTopicNotFound
	}

	sub := &Subscription{
		SubscriptionARN: fmt.Sprintf("%s:%s", topicARN, uuid.New().String()),
		TopicARN:        topicARN,
		Protocol:        protocol,
		Endpoint:        endpoint,
		Attributes:      make(map[string]string),
	}

	topic.mu.Lock()
	topic.Subscriptions = append(topic.Subscriptions, sub)
	topic.mu.Unlock()

	e.subscriptionsByARN[sub.SubscriptionARN] = sub

	return sub, nil
}

// PublishResult holds the result of a Publish call.
type PublishResult struct {
	MessageID string
}

// Publish sends a message to all subscribers of a topic.
func (e *Engine) Publish(topicARN, message, subject string, messageAttributes map[string]MessageAttribute) (*PublishResult, error) {
	if message == "" {
		return nil, fmt.Errorf("%w: Message is required", ErrInvalidParameter)
	}

	e.mu.RLock()
	topic := e.topicByARN(topicARN)
	nowFn := e.now
	if topic == nil {
		e.mu.RUnlock()
		return nil, ErrTopicNotFound
	}

	messageID := uuid.New().String()
	now := nowFn()

	topic.mu.Lock()
	subs := make([]*Subscription, len(topic.Subscriptions))
	copy(subs, topic.Subscriptions)
	topic.mu.Unlock()
	e.mu.RUnlock()

	envelope := buildSNSEnvelope(messageID, topicARN, subject, message, now, messageAttributes)

	delivered := 0
	for _, sub := range subs {
		if sub.Protocol != "sqs" {
			continue
		}

		queueName := queueNameFromARN(sub.Endpoint)
		if queueName == "" {
			slog.Warn("cannot extract queue name from subscription endpoint", "endpoint", sub.Endpoint, "subscriptionArn", sub.SubscriptionARN)
			continue
		}

		sub.mu.RLock()
		raw := strings.EqualFold(sub.Attributes["RawMessageDelivery"], "true")
		policy := sub.cachedFilterPolicy
		sub.mu.RUnlock()

		// Apply filter policy
		if !matchesFilterPolicy(policy, messageAttributes) {
			continue
		}

		body := envelope
		if raw {
			body = message
		}

		if err := e.enqueue(queueName, body); err != nil {
			slog.Warn("failed to deliver SNS message to SQS queue", "queue", queueName, "topicArn", topicARN, "err", err)
			continue
		}
		delivered++
	}

	slog.Info("SNS publish", "topicArn", topicARN, "messageId", messageID, "subscriptions", len(subs), "delivered", delivered)

	return &PublishResult{MessageID: messageID}, nil
}

// buildSNSEnvelope creates the standard SNS JSON envelope for non-raw delivery.
func buildSNSEnvelope(messageID, topicARN, subject, message string, timestamp time.Time, messageAttributes map[string]MessageAttribute) string {
	envelope := map[string]any{
		"Type":             "Notification",
		"MessageId":        messageID,
		"TopicArn":         topicARN,
		"Message":          message,
		"Timestamp":        timestamp.UTC().Format(time.RFC3339Nano),
		"SignatureVersion": "1",
		"Signature":        "EXAMPLE",
		"SigningCertURL":   "https://sns.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem",
		"UnsubscribeURL":   "https://sns.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=EXAMPLE",
	}
	if subject != "" {
		envelope["Subject"] = subject
	}
	if len(messageAttributes) > 0 {
		attrs := make(map[string]map[string]string, len(messageAttributes))
		for k, v := range messageAttributes {
			attrs[k] = map[string]string{
				"Type":  v.DataType,
				"Value": v.StringValue,
			}
		}
		envelope["MessageAttributes"] = attrs
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		slog.Warn("failed to marshal SNS envelope", "err", err)
		return message
	}
	return string(data)
}

// SetSubscriptionAttributes sets a single attribute on a subscription.
func (e *Engine) SetSubscriptionAttributes(subscriptionARN, attrName, attrValue string) error {
	if subscriptionARN == "" {
		return fmt.Errorf("%w: SubscriptionArn is required", ErrInvalidParameter)
	}
	if attrName == "" {
		return fmt.Errorf("%w: AttributeName is required", ErrInvalidParameter)
	}

	e.mu.RLock()
	sub := e.subscriptionsByARN[subscriptionARN]
	e.mu.RUnlock()

	if sub == nil {
		return ErrSubscriptionNotFound
	}

	sub.mu.Lock()
	defer sub.mu.Unlock()

	switch attrName {
	case "RawMessageDelivery":
		lower := strings.ToLower(attrValue)
		if lower != "true" && lower != "false" {
			return fmt.Errorf("%w: Invalid value for RawMessageDelivery. Must be true or false", ErrInvalidParameterValue)
		}
		sub.Attributes[attrName] = lower
	case "FilterPolicy":
		var parsed filterPolicy
		if attrValue != "" {
			var err error
			parsed, err = parseFilterPolicy(attrValue)
			if err != nil {
				return err
			}
		}
		sub.Attributes[attrName] = attrValue
		sub.cachedFilterPolicy = parsed
	default:
		sub.Attributes[attrName] = attrValue
	}

	return nil
}

// GetSubscriptionAttributes returns the attributes for a subscription.
func (e *Engine) GetSubscriptionAttributes(subscriptionARN string) (map[string]string, error) {
	if subscriptionARN == "" {
		return nil, fmt.Errorf("%w: SubscriptionArn is required", ErrInvalidParameter)
	}

	e.mu.RLock()
	sub := e.subscriptionsByARN[subscriptionARN]
	e.mu.RUnlock()

	if sub == nil {
		return nil, ErrSubscriptionNotFound
	}

	sub.mu.RLock()
	attrs := map[string]string{
		"SubscriptionArn":    sub.SubscriptionARN,
		"TopicArn":           sub.TopicARN,
		"Protocol":           sub.Protocol,
		"Endpoint":           sub.Endpoint,
		"RawMessageDelivery": sub.Attributes["RawMessageDelivery"],
	}
	if attrs["RawMessageDelivery"] == "" {
		attrs["RawMessageDelivery"] = "false"
	}

	for k, v := range sub.Attributes {
		if _, exists := attrs[k]; !exists {
			attrs[k] = v
		}
	}
	sub.mu.RUnlock()

	return attrs, nil
}

// queueNameFromARN extracts the queue name from an SQS queue ARN.
// ARN format: arn:aws:sqs:region:accountId:queueName.
func queueNameFromARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	return parts[5]
}
