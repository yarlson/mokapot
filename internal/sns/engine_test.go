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

	result, err := e.Publish(topic.ARN, "hello world", "")
	require.NoError(t, err)
	assert.NotEmpty(t, result.MessageID)

	// Verify message was delivered to SQS
	require.Len(t, rec.deliveries, 1)
	assert.Equal(t, "my-queue", rec.deliveries[0].QueueName)

	// Verify envelope structure
	var envelope map[string]string
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

	_, err = e.Publish(topic.ARN, "msg body", "Test Subject")
	require.NoError(t, err)

	require.Len(t, rec.deliveries, 1)

	var envelope map[string]string
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

	_, err = e.Publish(topic.ARN, "fanout message", "")
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

	result, err := e.Publish(topic.ARN, "message to nobody", "")
	require.NoError(t, err)
	assert.NotEmpty(t, result.MessageID)
	assert.Empty(t, rec.deliveries)
}

func TestPublish_TopicNotFound(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	_, err := e.Publish("arn:aws:sns:eu-central-1:000000000000:nonexistent", "msg", "")
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestPublish_EmptyMessage(t *testing.T) {
	rec := &enqueueRecorder{}
	e := newEngine(rec)

	topic, err := e.CreateTopic("my-topic")
	require.NoError(t, err)

	_, err = e.Publish(topic.ARN, "", "")
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
	result, err := e.Publish(topic.ARN, "msg", "")
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

	_, err = e.Publish(topic.ARN, "raw body content", "")
	require.NoError(t, err)

	require.Len(t, rec.deliveries, 1)
	assert.Equal(t, "raw-queue", rec.deliveries[0].QueueName)
	// Body should be the raw message, NOT an SNS envelope
	assert.Equal(t, "raw body content", rec.deliveries[0].Body)

	// Verify it's NOT JSON-parseable as an SNS envelope
	var envelope map[string]string
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

	_, err = e.Publish(topic.ARN, "test message", "")
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
	var envelope map[string]string
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

	_, err = e.Publish(topic.ARN, "envelope message", "")
	require.NoError(t, err)

	require.Len(t, rec.deliveries, 1)

	// Should be an SNS envelope
	var envelope map[string]string
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
