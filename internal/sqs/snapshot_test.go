package sqs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/sqs"
)

func TestSnapshotAndRestoreEmptyEngine(t *testing.T) {
	e := newEngine()

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newEngine()
	require.NoError(t, e2.Restore(data))

	// No queues should exist.
	_, err = e2.GetQueueURL("nonexistent")
	assert.ErrorIs(t, err, sqs.ErrQueueDoesNotExist)
}

func TestSnapshotAndRestoreQueues(t *testing.T) {
	e := newEngine()
	ctx := context.Background()

	_, err := e.CreateQueue("queue-a")
	require.NoError(t, err)
	_, err = e.CreateQueue("queue-b")
	require.NoError(t, err)

	// Send a message to queue-a.
	_, err = e.SendMessage("queue-a", "hello", -1, nil)
	require.NoError(t, err)

	data, err := e.Snapshot()
	require.NoError(t, err)

	// Restore into a fresh engine.
	e2 := newEngine()
	require.NoError(t, e2.Restore(data))

	// Both queues should exist.
	url, err := e2.GetQueueURL("queue-a")
	require.NoError(t, err)
	assert.Contains(t, url, "queue-a")

	url, err = e2.GetQueueURL("queue-b")
	require.NoError(t, err)
	assert.Contains(t, url, "queue-b")

	// Message should be receivable from restored engine.
	msgs, err := e2.ReceiveMessage(ctx, "queue-a", 1, 30, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "hello", msgs[0].Body)
}

func TestSnapshotAndRestoreQueueAttributes(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("attr-queue")
	require.NoError(t, err)
	require.NoError(t, e.SetQueueAttributes("attr-queue", map[string]string{
		"VisibilityTimeout": "60",
		"DelaySeconds":      "5",
	}))

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newEngine()
	require.NoError(t, e2.Restore(data))

	attrs, err := e2.GetQueueAttributes("attr-queue", []string{"All"})
	require.NoError(t, err)
	assert.Equal(t, "60", attrs["VisibilityTimeout"])
	assert.Equal(t, "5", attrs["DelaySeconds"])
}

func TestSnapshotAndRestoreInflightMessages(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	e := newEngine()
	e.SetClock(func() time.Time { return now })
	ctx := context.Background()

	_, err := e.CreateQueue("inflight-queue")
	require.NoError(t, err)

	_, err = e.SendMessage("inflight-queue", "msg1", -1, nil)
	require.NoError(t, err)

	// Receive to make it inflight.
	msgs, err := e.ReceiveMessage(ctx, "inflight-queue", 1, 300, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	data, err := e.Snapshot()
	require.NoError(t, err)

	// Restore at the same time — message should still be inflight.
	e2 := newEngine()
	e2.SetClock(func() time.Time { return now })
	require.NoError(t, e2.Restore(data))

	// Should not be receivable (still inflight).
	msgs2, err := e2.ReceiveMessage(ctx, "inflight-queue", 1, 30, 0)
	require.NoError(t, err)
	assert.Empty(t, msgs2)
}

func TestSnapshotAndRestoreExpiredInflightMessages(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	e := newEngine()
	e.SetClock(func() time.Time { return now })
	ctx := context.Background()

	_, err := e.CreateQueue("expire-queue")
	require.NoError(t, err)

	_, err = e.SendMessage("expire-queue", "will-expire", -1, nil)
	require.NoError(t, err)

	// Receive with short visibility timeout (10s).
	msgs, err := e.ReceiveMessage(ctx, "expire-queue", 1, 10, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	data, err := e.Snapshot()
	require.NoError(t, err)

	// Restore at a time after the visibility expired.
	later := now.Add(30 * time.Second)
	e2 := newEngine()
	e2.SetClock(func() time.Time { return later })
	require.NoError(t, e2.Restore(data))

	// Message should now be available again (expired inflight moved to available on restore).
	msgs2, err := e2.ReceiveMessage(ctx, "expire-queue", 1, 30, 0)
	require.NoError(t, err)
	require.Len(t, msgs2, 1)
	assert.Equal(t, "will-expire", msgs2[0].Body)
}

func TestSnapshotAndRestoreDelayedMessages(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	e := newEngine()
	e.SetClock(func() time.Time { return now })
	ctx := context.Background()

	_, err := e.CreateQueue("delay-queue")
	require.NoError(t, err)

	// Send with 60-second delay.
	_, err = e.SendMessage("delay-queue", "delayed-msg", 60, nil)
	require.NoError(t, err)

	data, err := e.Snapshot()
	require.NoError(t, err)

	// Restore at now (before delay expires).
	e2 := newEngine()
	e2.SetClock(func() time.Time { return now })
	require.NoError(t, e2.Restore(data))

	// Not yet receivable.
	msgs, err := e2.ReceiveMessage(ctx, "delay-queue", 1, 30, 0)
	require.NoError(t, err)
	assert.Empty(t, msgs)

	// Advance clock past delay.
	later := now.Add(90 * time.Second)
	e2.SetClock(func() time.Time { return later })

	msgs, err = e2.ReceiveMessage(ctx, "delay-queue", 1, 30, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "delayed-msg", msgs[0].Body)
}

func TestSnapshotAndRestoreRedrivePolicy(t *testing.T) {
	e := newEngine()
	ctx := context.Background()

	// Create DLQ and source queue with RedrivePolicy.
	_, err := e.CreateQueue("dlq")
	require.NoError(t, err)
	_, err = e.CreateQueue("src-queue")
	require.NoError(t, err)
	require.NoError(t, e.SetQueueAttributes("src-queue", map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:eu-central-1:000000000000:dlq","maxReceiveCount":2}`,
	}))

	_, err = e.SendMessage("src-queue", "dlq-msg", -1, nil)
	require.NoError(t, err)

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newEngine()
	require.NoError(t, e2.Restore(data))

	// Verify RedrivePolicy survived.
	attrs, err := e2.GetQueueAttributes("src-queue", []string{"RedrivePolicy"})
	require.NoError(t, err)
	assert.Contains(t, attrs["RedrivePolicy"], "dlq")
	assert.Contains(t, attrs["RedrivePolicy"], "maxReceiveCount")

	// Message should be receivable and DLQ behavior should work.
	msgs, err := e2.ReceiveMessage(ctx, "src-queue", 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "dlq-msg", msgs[0].Body)
}

func TestSnapshotAndRestoreReceiveCount(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	e := newEngine()
	e.SetClock(func() time.Time { return now })
	ctx := context.Background()

	_, err := e.CreateQueue("rc-queue")
	require.NoError(t, err)

	_, err = e.SendMessage("rc-queue", "counted-msg", -1, nil)
	require.NoError(t, err)

	// Receive to increment ReceiveCount to 1, then let it expire.
	msgs, err := e.ReceiveMessage(ctx, "rc-queue", 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].ReceiveCount)

	// Advance past visibility timeout so message returns to available.
	now = now.Add(10 * time.Second)
	e.SetClock(func() time.Time { return now })

	data, err := e.Snapshot()
	require.NoError(t, err)

	// Restore and receive again — ReceiveCount should be preserved and incremented.
	e2 := newEngine()
	e2.SetClock(func() time.Time { return now })
	require.NoError(t, e2.Restore(data))

	msgs2, err := e2.ReceiveMessage(ctx, "rc-queue", 1, 30, 0)
	require.NoError(t, err)
	require.Len(t, msgs2, 1)
	assert.Equal(t, 2, msgs2[0].ReceiveCount)
}

func TestSnapshotAndRestoreFiltersExpiredRetention(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	e := newEngine()
	e.SetClock(func() time.Time { return now })
	ctx := context.Background()

	_, err := e.CreateQueue("ret-queue")
	require.NoError(t, err)
	require.NoError(t, e.SetQueueAttributes("ret-queue", map[string]string{
		"MessageRetentionPeriod": "60",
	}))

	// Send two messages.
	_, err = e.SendMessage("ret-queue", "old-msg", -1, nil)
	require.NoError(t, err)

	// Advance 30 seconds, then send another.
	now = now.Add(30 * time.Second)
	e.SetClock(func() time.Time { return now })
	_, err = e.SendMessage("ret-queue", "new-msg", -1, nil)
	require.NoError(t, err)

	data, err := e.Snapshot()
	require.NoError(t, err)

	// Restore 61 seconds after the first message was sent (31 seconds after the second).
	restoreTime := time.Date(2025, 1, 1, 0, 1, 1, 0, time.UTC) // 61s after initial
	e2 := newEngine()
	e2.SetClock(func() time.Time { return restoreTime })
	require.NoError(t, e2.Restore(data))

	// Only the newer message should survive restore.
	msgs, err := e2.ReceiveMessage(ctx, "ret-queue", 10, 30, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "new-msg", msgs[0].Body)
}

func TestSnapshotAndRestoreMultipleMessages(t *testing.T) {
	e := newEngine()
	ctx := context.Background()

	_, err := e.CreateQueue("multi-queue")
	require.NoError(t, err)

	for i := range 5 {
		_, err = e.SendMessage("multi-queue", fmt.Sprintf("msg-%d", i), -1, nil)
		require.NoError(t, err)
	}

	data, err := e.Snapshot()
	require.NoError(t, err)

	e2 := newEngine()
	require.NoError(t, e2.Restore(data))

	msgs, err := e2.ReceiveMessage(ctx, "multi-queue", 10, 30, 0)
	require.NoError(t, err)
	assert.Len(t, msgs, 5)
}
