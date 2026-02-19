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
