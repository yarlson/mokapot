package sns_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/sns"
)

// enqueueRecorder records messages delivered to SQS queues during publish.
type enqueueRecorder struct {
	deliveries []delivery
	err        error // if non-nil, returned by the enqueue function
}

type delivery struct {
	QueueName string
	Body      string
}

func (r *enqueueRecorder) enqueue(queueName, body string) error {
	r.deliveries = append(r.deliveries, delivery{QueueName: queueName, Body: body})
	return r.err
}

func newEngine(rec *enqueueRecorder) *sns.Engine {
	return sns.NewEngine("eu-central-1", "000000000000", rec.enqueue)
}

func TestCreateTopic(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	assert.Equal(t, "my-topic", topic.Name)
	assert.Equal(t, "arn:aws:sns:eu-central-1:000000000000:my-topic", topic.ARN)
}

func TestCreateTopicIdempotent(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	t1, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	t2, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	assert.Equal(t, t1.ARN, t2.ARN)
}

func TestCreateTopicEmptyName(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	_, err := e.CreateTopic("")
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

func TestSubscribe(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:my-queue")
	require.NoError(t, err)

	assert.Contains(t, sub.SubscriptionARN, topic.ARN)
	assert.Equal(t, topic.ARN, sub.TopicARN)
	assert.Equal(t, "sqs", sub.Protocol)
	assert.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:my-queue", sub.Endpoint)
}

func TestSubscribeTopicNotFound(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	_, err := e.Subscribe("arn:aws:sns:eu-central-1:000000000000:nonexistent", "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestSubscribeUnsupportedProtocol(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	_, err = e.Subscribe(topic.ARN, "http", "https://example.com")
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

func TestPublish_SingleSubscriber(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:my-queue")
	require.NoError(t, err)

	result, err := e.Publish(topic.ARN, "hello world", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.MessageID)

	// Verify message was delivered to SQS
	require.Len(t, rec.deliveries, 1)
	assert.Equal(t, "my-queue", rec.deliveries[0].QueueName)

	// Verify envelope structure
	var envelope map[string]any
	err = json.Unmarshal([]byte(rec.deliveries[0].Body), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, result.MessageID, envelope["MessageId"])
	assert.Equal(t, topic.ARN, envelope["TopicArn"])
	assert.Equal(t, "hello world", envelope["Message"])
	assert.NotEmpty(t, envelope["Timestamp"])
}

func TestPublish_WithSubject(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	_, err = e.Publish(topic.ARN, "msg body", "Test Subject", nil)
	require.NoError(t, err)

	require.Len(t, rec.deliveries, 1)

	var envelope map[string]any
	err = json.Unmarshal([]byte(rec.deliveries[0].Body), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "Test Subject", envelope["Subject"])
}

func TestPublish_MultipleSubscribers(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("fanout-topic")
	require.NoError(t, err)

	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:queue-a")
	require.NoError(t, err)
	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:queue-b")
	require.NoError(t, err)

	_, err = e.Publish(topic.ARN, "fanout message", "", nil)
	require.NoError(t, err)

	// Both queues should receive the message
	require.Len(t, rec.deliveries, 2)

	queueNames := map[string]bool{
		rec.deliveries[0].QueueName: true,
		rec.deliveries[1].QueueName: true,
	}
	assert.True(t, queueNames["queue-a"])
	assert.True(t, queueNames["queue-b"])
}

func TestPublish_NoSubscribers(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("empty-topic")
	require.NoError(t, err)

	result, err := e.Publish(topic.ARN, "message to nobody", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.MessageID)
	assert.Empty(t, rec.deliveries)
}

func TestPublish_TopicNotFound(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	_, err := e.Publish("arn:aws:sns:eu-central-1:000000000000:nonexistent", "msg", "", nil)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestPublish_EmptyMessage(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	_, err = e.Publish(topic.ARN, "", "", nil)
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

func TestPublish_DeliveryFailureSilent(t *testing.T) {
	rec := &enqueueRecorder{err: assert.AnError}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:bad-queue")
	require.NoError(t, err)

	// Publish should succeed even when delivery fails
	result, err := e.Publish(topic.ARN, "msg", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.MessageID)
}

// --- RawMessageDelivery tests ---

func TestPublish_RawMessageDelivery(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("raw-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:raw-queue")
	require.NoError(t, err)

	// Enable raw delivery
	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "RawMessageDelivery", "true")
	require.NoError(t, err)

	_, err = e.Publish(topic.ARN, "raw body content", "", nil)
	require.NoError(t, err)

	require.Len(t, rec.deliveries, 1)
	assert.Equal(t, "raw-queue", rec.deliveries[0].QueueName)
	// Body should be the raw message, NOT an SNS envelope
	assert.Equal(t, "raw body content", rec.deliveries[0].Body)

	// Verify it's NOT JSON-parseable as an SNS envelope
	var envelope map[string]any
	err = json.Unmarshal([]byte(rec.deliveries[0].Body), &envelope)
	assert.Error(t, err, "raw body should not be a JSON envelope")
}

func TestPublish_MixedRawAndEnvelope(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("mixed-topic")
	require.NoError(t, err)

	// Subscribe two queues: one raw, one envelope
	rawSub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:raw-queue")
	require.NoError(t, err)

	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:envelope-queue")
	require.NoError(t, err)

	// Enable raw delivery on only the first subscription
	err = e.SetSubscriptionAttributes(rawSub.SubscriptionARN, "RawMessageDelivery", "true")
	require.NoError(t, err)

	_, err = e.Publish(topic.ARN, "test message", "", nil)
	require.NoError(t, err)

	require.Len(t, rec.deliveries, 2)

	// Find which delivery is raw vs envelope
	var rawBody, envelopeBody string
	for _, d := range rec.deliveries {
		if d.QueueName == "raw-queue" {
			rawBody = d.Body
		} else {
			envelopeBody = d.Body
		}
	}

	// Raw queue gets plain message
	assert.Equal(t, "test message", rawBody)

	// Envelope queue gets SNS JSON envelope
	var envelope map[string]any
	err = json.Unmarshal([]byte(envelopeBody), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, "test message", envelope["Message"])
}

func TestPublish_RawDeliveryDisabled(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("disabled-raw-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	// Explicitly set to false
	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "RawMessageDelivery", "false")
	require.NoError(t, err)

	_, err = e.Publish(topic.ARN, "envelope message", "", nil)
	require.NoError(t, err)

	require.Len(t, rec.deliveries, 1)

	// Should be an SNS envelope
	var envelope map[string]any
	err = json.Unmarshal([]byte(rec.deliveries[0].Body), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, "envelope message", envelope["Message"])
}

// --- SetSubscriptionAttributes tests ---

func TestSetSubscriptionAttributes_RawMessageDelivery(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("attr-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "RawMessageDelivery", "true")
	require.NoError(t, err)

	attrs, err := e.GetSubscriptionAttributes(sub.SubscriptionARN)
	require.NoError(t, err)
	assert.Equal(t, "true", attrs["RawMessageDelivery"])
}

func TestSetSubscriptionAttributes_InvalidRawMessageDeliveryValue(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("invalid-val-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "RawMessageDelivery", "yes")
	assert.ErrorIs(t, err, sns.ErrInvalidParameterValue)
}

func TestSetSubscriptionAttributes_SubscriptionNotFound(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	err := e.SetSubscriptionAttributes("arn:aws:sns:eu-central-1:000000000000:topic:nonexistent", "RawMessageDelivery", "true")
	assert.ErrorIs(t, err, sns.ErrSubscriptionNotFound)
}

func TestSetSubscriptionAttributes_EmptySubscriptionARN(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	err := e.SetSubscriptionAttributes("", "RawMessageDelivery", "true")
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

func TestSetSubscriptionAttributes_EmptyAttributeName(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("t")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "", "value")
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// --- GetSubscriptionAttributes tests ---

func TestGetSubscriptionAttributes_DefaultValues(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("get-attr-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:my-queue")
	require.NoError(t, err)

	attrs, err := e.GetSubscriptionAttributes(sub.SubscriptionARN)
	require.NoError(t, err)

	assert.Equal(t, sub.SubscriptionARN, attrs["SubscriptionArn"])
	assert.Equal(t, topic.ARN, attrs["TopicArn"])
	assert.Equal(t, "sqs", attrs["Protocol"])
	assert.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:my-queue", attrs["Endpoint"])
	assert.Equal(t, "false", attrs["RawMessageDelivery"])
}

func TestGetSubscriptionAttributes_SubscriptionNotFound(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	_, err := e.GetSubscriptionAttributes("arn:aws:sns:eu-central-1:000000000000:topic:nonexistent")
	assert.ErrorIs(t, err, sns.ErrSubscriptionNotFound)
}

func TestGetSubscriptionAttributes_EmptyARN(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	_, err := e.GetSubscriptionAttributes("")
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// --- FilterPolicy tests ---

func TestPublish_FilterPolicy_ExactStringMatch(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("filter-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:filter-queue")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"event_type": ["order_created"]}`)
	require.NoError(t, err)

	// Matching message — should be delivered
	_, err = e.Publish(topic.ARN, "matching msg", "", map[string]sns.MessageAttribute{
		"event_type": {DataType: "String", StringValue: "order_created"},
	})
	require.NoError(t, err)
	require.Len(t, rec.deliveries, 1)

	// Non-matching message — should be filtered out
	_, err = e.Publish(topic.ARN, "non-matching msg", "", map[string]sns.MessageAttribute{
		"event_type": {DataType: "String", StringValue: "user_created"},
	})
	require.NoError(t, err)
	assert.Len(t, rec.deliveries, 1) // still 1, no new delivery
}

func TestPublish_FilterPolicy_NoAttributes(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("filter-topic-no-attrs")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"event_type": ["order_created"]}`)
	require.NoError(t, err)

	// No message attributes — should be filtered out
	_, err = e.Publish(topic.ARN, "no attrs msg", "", nil)
	require.NoError(t, err)
	assert.Empty(t, rec.deliveries)
}

func TestPublish_FilterPolicy_NoPolicy_AllDelivered(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("no-filter-topic")
	require.NoError(t, err)

	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	// No filter policy — all messages should be delivered
	_, err = e.Publish(topic.ARN, "any msg", "", map[string]sns.MessageAttribute{
		"event_type": {DataType: "String", StringValue: "anything"},
	})
	require.NoError(t, err)
	assert.Len(t, rec.deliveries, 1)
}

func TestPublish_FilterPolicy_MixedSubscriptions(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("mixed-filter-topic")
	require.NoError(t, err)

	// Subscription with filter
	filtered, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:filtered-queue")
	require.NoError(t, err)
	err = e.SetSubscriptionAttributes(filtered.SubscriptionARN, "FilterPolicy", `{"event_type": ["order_created"]}`)
	require.NoError(t, err)

	// Subscription without filter
	_, err = e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:unfiltered-queue")
	require.NoError(t, err)

	// Publish non-matching message
	_, err = e.Publish(topic.ARN, "msg", "", map[string]sns.MessageAttribute{
		"event_type": {DataType: "String", StringValue: "user_created"},
	})
	require.NoError(t, err)

	// Only unfiltered queue should receive
	require.Len(t, rec.deliveries, 1)
	assert.Equal(t, "unfiltered-queue", rec.deliveries[0].QueueName)
}

func TestPublish_FilterPolicy_Exists(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("exists-filter-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"customer_id": [{"exists": true}]}`)
	require.NoError(t, err)

	// Message with the attribute — delivered
	_, err = e.Publish(topic.ARN, "has attr", "", map[string]sns.MessageAttribute{
		"customer_id": {DataType: "String", StringValue: "123"},
	})
	require.NoError(t, err)
	assert.Len(t, rec.deliveries, 1)

	// Message without the attribute — filtered
	_, err = e.Publish(topic.ARN, "no attr", "", map[string]sns.MessageAttribute{
		"other": {DataType: "String", StringValue: "val"},
	})
	require.NoError(t, err)
	assert.Len(t, rec.deliveries, 1) // still 1
}

func TestPublish_FilterPolicy_NumericBetween(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("numeric-filter-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"price": [{"numeric": [">=", 100, "<=", 200]}]}`)
	require.NoError(t, err)

	// In range
	_, err = e.Publish(topic.ARN, "in range", "", map[string]sns.MessageAttribute{
		"price": {DataType: "Number", StringValue: "150"},
	})
	require.NoError(t, err)
	assert.Len(t, rec.deliveries, 1)

	// Out of range
	_, err = e.Publish(topic.ARN, "out of range", "", map[string]sns.MessageAttribute{
		"price": {DataType: "Number", StringValue: "300"},
	})
	require.NoError(t, err)
	assert.Len(t, rec.deliveries, 1) // still 1
}

func TestSetSubscriptionAttributes_FilterPolicy_Valid(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("fp-valid-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"event_type": ["order_created"]}`)
	require.NoError(t, err)

	attrs, err := e.GetSubscriptionAttributes(sub.SubscriptionARN)
	require.NoError(t, err)
	assert.Equal(t, `{"event_type": ["order_created"]}`, attrs["FilterPolicy"])
}

func TestSetSubscriptionAttributes_FilterPolicy_Invalid(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("fp-invalid-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `not json`)
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

func TestSetSubscriptionAttributes_FilterPolicy_Empty(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("fp-empty-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q")
	require.NoError(t, err)

	// Set a filter policy first
	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"key": ["val"]}`)
	require.NoError(t, err)

	// Clear filter policy with empty string
	err = e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", "")
	require.NoError(t, err)

	attrs, err := e.GetSubscriptionAttributes(sub.SubscriptionARN)
	require.NoError(t, err)
	assert.Equal(t, "", attrs["FilterPolicy"])
}
