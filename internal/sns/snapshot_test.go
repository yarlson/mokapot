package sns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/sns"
)

func newTestEngine() *sns.Engine {
	return sns.NewEngine("eu-central-1", "000000000000", func(_, _ string) error { return nil })
}

func TestSNS_SnapshotAndRestoreEmptyEngine(t *testing.T) {
	e := newTestEngine()

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newTestEngine()
	require.NoError(t, e2.Restore(data))
}

func TestSNS_SnapshotAndRestoreTopics(t *testing.T) {
	e := newTestEngine()

	t1, err := e.CreateTopic("topic-a")
	require.NoError(t, err)

	_, err = e.CreateTopic("topic-b")
	require.NoError(t, err)

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newTestEngine()
	require.NoError(t, e2.Restore(data))

	// Verify topics exist by subscribing (requires topic to exist).
	_, err = e2.Subscribe(t1.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:queue1")
	require.NoError(t, err)
}

func TestSNS_SnapshotAndRestoreSubscriptions(t *testing.T) {
	e := newTestEngine()

	topic, err := e.CreateTopic("sub-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q1")
	require.NoError(t, err)

	// Set subscription attributes.
	require.NoError(t, e.SetSubscriptionAttributes(sub.SubscriptionARN, "RawMessageDelivery", "true"))

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newTestEngine()
	require.NoError(t, e2.Restore(data))

	attrs, err := e2.GetSubscriptionAttributes(sub.SubscriptionARN)
	require.NoError(t, err)
	assert.Equal(t, "true", attrs["RawMessageDelivery"])
	assert.Equal(t, "sqs", attrs["Protocol"])
	assert.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:q1", attrs["Endpoint"])
}

func TestSNS_SnapshotAndRestoreFilterPolicy(t *testing.T) {
	delivered := make([]string, 0)
	enqueue := func(queueName, body string) error {
		delivered = append(delivered, queueName)
		return nil
	}
	e := sns.NewEngine("eu-central-1", "000000000000", enqueue)

	topic, err := e.CreateTopic("filter-topic")
	require.NoError(t, err)

	sub, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:filter-q")
	require.NoError(t, err)

	require.NoError(t, e.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"type":["order"]}`))

	data, err := e.Snapshot()
	require.NoError(t, err)

	// Restore into engine with our delivery tracker.
	delivered2 := make([]string, 0)
	enqueue2 := func(queueName, body string) error {
		delivered2 = append(delivered2, queueName)
		return nil
	}
	e2 := sns.NewEngine("eu-central-1", "000000000000", enqueue2)
	require.NoError(t, e2.Restore(data))

	// Publish with matching attribute — should deliver.
	_, err = e2.Publish(topic.ARN, "order message", "", map[string]sns.MessageAttribute{
		"type": {DataType: "String", StringValue: "order"},
	})
	require.NoError(t, err)
	assert.Len(t, delivered2, 1)

	// Publish without matching attribute — should NOT deliver.
	_, err = e2.Publish(topic.ARN, "other message", "", map[string]sns.MessageAttribute{
		"type": {DataType: "String", StringValue: "refund"},
	})
	require.NoError(t, err)
	assert.Len(t, delivered2, 1) // Still 1 — not delivered.
}

func TestSNS_SnapshotAndRestoreMultipleSubscriptions(t *testing.T) {
	e := newTestEngine()

	topic, err := e.CreateTopic("multi-sub-topic")
	require.NoError(t, err)

	sub1, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q1")
	require.NoError(t, err)

	sub2, err := e.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:q2")
	require.NoError(t, err)

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newTestEngine()
	require.NoError(t, e2.Restore(data))

	// Both subscriptions should be accessible.
	attrs1, err := e2.GetSubscriptionAttributes(sub1.SubscriptionARN)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:q1", attrs1["Endpoint"])

	attrs2, err := e2.GetSubscriptionAttributes(sub2.SubscriptionARN)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:q2", attrs2["Endpoint"])
}
