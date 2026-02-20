package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/sns"
	"github.com/yarlson/mokapot/internal/sqs"
	"github.com/yarlson/mokapot/internal/store"
)

// TestPersistence_SQSSurvivesRestart simulates a container restart:
// create queues and messages, snapshot to bbolt, close, reopen, restore, verify.
func TestPersistence_SQSSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	ctx := context.Background()

	// Phase 1: populate state and persist.
	e1 := sqs.NewEngine("eu-central-1", "000000000000", "localhost:4566")

	_, err := e1.CreateQueue("orders")
	require.NoError(t, err)
	_, err = e1.CreateQueue("notifications")
	require.NoError(t, err)

	require.NoError(t, e1.SetQueueAttributes("orders", map[string]string{
		"VisibilityTimeout": "120",
	}))

	_, err = e1.SendMessage("orders", "order-1", -1, nil)
	require.NoError(t, err)
	_, err = e1.SendMessage("orders", "order-2", -1, nil)
	require.NoError(t, err)
	_, err = e1.SendMessage("notifications", "notify-1", -1, nil)
	require.NoError(t, err)

	data, err := e1.Snapshot()
	require.NoError(t, err)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.SaveSQSState(data))
	require.NoError(t, s.Close())

	// Phase 2: simulate restart — fresh engine, restore from bbolt.
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	sqsData, err := s2.LoadSQSState()
	require.NoError(t, err)
	require.NotNil(t, sqsData)

	e2 := sqs.NewEngine("eu-central-1", "000000000000", "localhost:4566")
	require.NoError(t, e2.Restore(sqsData))

	// Verify queues exist.
	url, err := e2.GetQueueURL("orders")
	require.NoError(t, err)
	assert.Contains(t, url, "orders")

	url, err = e2.GetQueueURL("notifications")
	require.NoError(t, err)
	assert.Contains(t, url, "notifications")

	// Verify queue attributes survived.
	attrs, err := e2.GetQueueAttributes("orders", []string{"VisibilityTimeout"})
	require.NoError(t, err)
	assert.Equal(t, "120", attrs["VisibilityTimeout"])

	// Verify messages survived.
	msgs, err := e2.ReceiveMessage(ctx, "orders", 10, 30, 0)
	require.NoError(t, err)
	assert.Len(t, msgs, 2)

	msgs, err = e2.ReceiveMessage(ctx, "notifications", 10, 30, 0)
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, "notify-1", msgs[0].Body)
}

// TestPersistence_SNSSurvivesRestart simulates a container restart for SNS state.
func TestPersistence_SNSSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")

	// Phase 1: populate and persist.
	delivered1 := make([]string, 0)
	enqueue1 := func(queueName, body string) error {
		delivered1 = append(delivered1, queueName+":"+body)
		return nil
	}
	e1 := sns.NewEngine("eu-central-1", "000000000000", enqueue1)

	topic, err := e1.CreateTopic("events")
	require.NoError(t, err)

	sub, err := e1.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:events-q")
	require.NoError(t, err)

	require.NoError(t, e1.SetSubscriptionAttributes(sub.SubscriptionARN, "RawMessageDelivery", "true"))
	require.NoError(t, e1.SetSubscriptionAttributes(sub.SubscriptionARN, "FilterPolicy", `{"category":["important"]}`))

	data, err := e1.Snapshot()
	require.NoError(t, err)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.SaveSNSState(data))
	require.NoError(t, s.Close())

	// Phase 2: restart.
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	snsData, err := s2.LoadSNSState()
	require.NoError(t, err)
	require.NotNil(t, snsData)

	delivered2 := make([]string, 0)
	enqueue2 := func(queueName, body string) error {
		delivered2 = append(delivered2, queueName+":"+body)
		return nil
	}
	e2 := sns.NewEngine("eu-central-1", "000000000000", enqueue2)
	require.NoError(t, e2.Restore(snsData))

	// Verify subscription attributes survived.
	attrs, err := e2.GetSubscriptionAttributes(sub.SubscriptionARN)
	require.NoError(t, err)
	assert.Equal(t, "true", attrs["RawMessageDelivery"])
	assert.Equal(t, `{"category":["important"]}`, attrs["FilterPolicy"])

	// Verify filter policy is active after restore.
	// Publish with matching attribute — should deliver.
	_, err = e2.Publish(topic.ARN, "hello", "", map[string]sns.MessageAttribute{
		"category": {DataType: "String", StringValue: "important"},
	})
	require.NoError(t, err)
	assert.Len(t, delivered2, 1)

	// Publish without matching attribute — should NOT deliver.
	_, err = e2.Publish(topic.ARN, "boring", "", map[string]sns.MessageAttribute{
		"category": {DataType: "String", StringValue: "spam"},
	})
	require.NoError(t, err)
	assert.Len(t, delivered2, 1) // Still 1.
}

// TestPersistence_FullStackSurvivesRestart tests both SQS and SNS state together.
func TestPersistence_FullStackSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	ctx := context.Background()

	// Phase 1: set up SQS queue, SNS topic with subscription, send via SNS.
	sqsEngine1 := sqs.NewEngine("eu-central-1", "000000000000", "localhost:4566")
	enqueue1 := func(queueName, body string) error {
		_, err := sqsEngine1.SendMessage(queueName, body, 0, nil)
		return err
	}
	snsEngine1 := sns.NewEngine("eu-central-1", "000000000000", enqueue1)

	_, err := sqsEngine1.CreateQueue("fan-q")
	require.NoError(t, err)

	topic, err := snsEngine1.CreateTopic("fan-topic")
	require.NoError(t, err)

	_, err = snsEngine1.Subscribe(topic.ARN, "sqs", "arn:aws:sqs:eu-central-1:000000000000:fan-q")
	require.NoError(t, err)

	// Publish a message via SNS — it should land in the SQS queue.
	_, err = snsEngine1.Publish(topic.ARN, "fanout-msg", "", nil)
	require.NoError(t, err)

	// Also send a direct SQS message.
	_, err = sqsEngine1.SendMessage("fan-q", "direct-msg", -1, nil)
	require.NoError(t, err)

	// Snapshot and persist both engines.
	sqsData, err := sqsEngine1.Snapshot()
	require.NoError(t, err)
	snsData, err := snsEngine1.Snapshot()
	require.NoError(t, err)

	s, err := store.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.SaveSQSState(sqsData))
	require.NoError(t, s.SaveSNSState(snsData))
	require.NoError(t, s.Close())

	// Phase 2: restart both engines.
	s2, err := store.Open(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	sqsData2, err := s2.LoadSQSState()
	require.NoError(t, err)
	snsData2, err := s2.LoadSNSState()
	require.NoError(t, err)

	sqsEngine2 := sqs.NewEngine("eu-central-1", "000000000000", "localhost:4566")
	require.NoError(t, sqsEngine2.Restore(sqsData2))

	enqueue2 := func(queueName, body string) error {
		_, err := sqsEngine2.SendMessage(queueName, body, 0, nil)
		return err
	}
	snsEngine2 := sns.NewEngine("eu-central-1", "000000000000", enqueue2)
	require.NoError(t, snsEngine2.Restore(snsData2))

	// Both messages should be receivable from the restored SQS engine.
	msgs, err := sqsEngine2.ReceiveMessage(ctx, "fan-q", 10, 30, 0)
	require.NoError(t, err)
	assert.Len(t, msgs, 2)

	// Publishing via the restored SNS engine should still work.
	_, err = snsEngine2.Publish(topic.ARN, "post-restart-msg", "", nil)
	require.NoError(t, err)

	msgs, err = sqsEngine2.ReceiveMessage(ctx, "fan-q", 10, 30, 0)
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Contains(t, msgs[0].Body, "post-restart-msg")
}
