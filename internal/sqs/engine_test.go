package sqs_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/devstack/internal/sqs"
)

func newEngine() *sqs.Engine {
	return sqs.NewEngine("eu-central-1", "000000000000", "localhost:4566")
}

func TestCreateQueue(t *testing.T) {
	e := newEngine()

	q, err := e.CreateQueue("test-queue")
	require.NoError(t, err)

	assert.Equal(t, "test-queue", q.Name)
	assert.Equal(t, "http://localhost:4566/000000000000/test-queue", q.URL)
	assert.Equal(t, "arn:aws:sqs:eu-central-1:000000000000:test-queue", q.ARN)
	assert.Equal(t, "30", q.Attributes["VisibilityTimeout"])
}

func TestCreateQueueIdempotent(t *testing.T) {
	e := newEngine()

	q1, err := e.CreateQueue("test-queue")
	require.NoError(t, err)

	q2, err := e.CreateQueue("test-queue")
	require.NoError(t, err)

	assert.Equal(t, q1.URL, q2.URL)
}

func TestGetQueueURL(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("my-queue")
	require.NoError(t, err)

	url, err := e.GetQueueURL("my-queue")
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:4566/000000000000/my-queue", url)
}

func TestGetQueueURLNotFound(t *testing.T) {
	e := newEngine()

	_, err := e.GetQueueURL("nonexistent")
	assert.ErrorIs(t, err, sqs.ErrQueueDoesNotExist)
}

func TestSendAndReceiveMessage(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	msg, err := e.SendMessage("q", "hello world")
	require.NoError(t, err)
	assert.NotEmpty(t, msg.MessageID)
	assert.Equal(t, "hello world", msg.Body)
	assert.NotEmpty(t, msg.MD5OfBody)

	received, err := e.ReceiveMessage(ctx, "q", 1, 30, 0)
	require.NoError(t, err)
	require.Len(t, received, 1)
	assert.Equal(t, msg.MessageID, received[0].MessageID)
	assert.Equal(t, "hello world", received[0].Body)
	assert.Equal(t, 1, received[0].ReceiveCount)
	assert.NotEmpty(t, received[0].ReceiptHandle)
}

func TestReceiveMessageEmptyQueue(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	received, err := e.ReceiveMessage(ctx, "q", 1, 30, 0)
	require.NoError(t, err)
	assert.Empty(t, received)
}

func TestReceiveMessageNonExistentQueue(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.ReceiveMessage(ctx, "nope", 1, 30, 0)
	assert.ErrorIs(t, err, sqs.ErrQueueDoesNotExist)
}

func TestDeleteMessage(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "msg1")
	require.NoError(t, err)

	received, err := e.ReceiveMessage(ctx, "q", 1, 30, 0)
	require.NoError(t, err)
	require.Len(t, received, 1)

	err = e.DeleteMessage("q", received[0].ReceiptHandle)
	require.NoError(t, err)

	// After delete, message should not reappear even with visibility=0
	received2, err := e.ReceiveMessage(ctx, "q", 1, 0, 0)
	require.NoError(t, err)
	assert.Empty(t, received2)
}

func TestDeleteMessageInvalidHandle(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	err = e.DeleteMessage("q", "bogus-handle")
	assert.ErrorIs(t, err, sqs.ErrReceiptHandleIsInvalid)
}

func TestSendToNonExistentQueue(t *testing.T) {
	e := newEngine()

	_, err := e.SendMessage("nope", "body")
	assert.ErrorIs(t, err, sqs.ErrQueueDoesNotExist)
}

func TestReceiveMaxMessages(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	for i := range 5 {
		_, err = e.SendMessage("q", fmt.Sprintf("msg%d", i))
		require.NoError(t, err)
	}

	received, err := e.ReceiveMessage(ctx, "q", 3, 30, 0)
	require.NoError(t, err)
	assert.Len(t, received, 3)

	// Remaining 2 still available
	received2, err := e.ReceiveMessage(ctx, "q", 10, 30, 0)
	require.NoError(t, err)
	assert.Len(t, received2, 2)
}

func TestMessageInvisibleAfterReceive(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "msg")
	require.NoError(t, err)

	// Receive with a long visibility timeout
	received, err := e.ReceiveMessage(ctx, "q", 1, 300, 0)
	require.NoError(t, err)
	assert.Len(t, received, 1)

	// Should not be receivable again immediately
	received2, err := e.ReceiveMessage(ctx, "q", 1, 30, 0)
	require.NoError(t, err)
	assert.Empty(t, received2)
}

func TestMessageReappearsAfterVisibilityTimeout(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	sent, err := e.SendMessage("q", "retry-me")
	require.NoError(t, err)

	// Receive with zero visibility timeout — message becomes available on next call
	received, err := e.ReceiveMessage(ctx, "q", 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, received, 1)
	assert.Equal(t, "retry-me", received[0].Body)
	assert.Equal(t, sent.MessageID, received[0].MessageID)
	assert.Equal(t, 1, received[0].ReceiveCount)
	firstHandle := received[0].ReceiptHandle

	// Next ReceiveMessage should find the message again (visibility expired)
	received2, err := e.ReceiveMessage(ctx, "q", 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, received2, 1)

	// Same message (same ID and body), but new receipt handle and incremented count
	assert.Equal(t, sent.MessageID, received2[0].MessageID)
	assert.Equal(t, "retry-me", received2[0].Body)
	assert.Equal(t, 2, received2[0].ReceiveCount)
	assert.NotEqual(t, firstHandle, received2[0].ReceiptHandle)
}

func TestOldReceiptHandleInvalidAfterReappearance(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "msg")
	require.NoError(t, err)

	// Receive with zero visibility timeout
	received, err := e.ReceiveMessage(ctx, "q", 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, received, 1)
	oldHandle := received[0].ReceiptHandle

	// Let message reappear and receive it again with a normal timeout
	received2, err := e.ReceiveMessage(ctx, "q", 1, 30, 0)
	require.NoError(t, err)
	require.Len(t, received2, 1)

	// Old receipt handle should be invalid (message moved back then re-received)
	err = e.DeleteMessage("q", oldHandle)
	assert.ErrorIs(t, err, sqs.ErrReceiptHandleIsInvalid)

	// New receipt handle should work
	err = e.DeleteMessage("q", received2[0].ReceiptHandle)
	assert.NoError(t, err)
}

func TestMultipleReappearancesIncrementReceiveCount(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "persistent")
	require.NoError(t, err)

	// Receive the same message 5 times without deleting
	for i := 1; i <= 5; i++ {
		received, err := e.ReceiveMessage(ctx, "q", 1, 0, 0)
		require.NoError(t, err)
		require.Len(t, received, 1)
		assert.Equal(t, "persistent", received[0].Body)
		assert.Equal(t, i, received[0].ReceiveCount)
	}
}

// --- Long polling tests ---

func TestLongPolling_ReturnsImmediatelyWhenMessagesAvailable(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "already here")
	require.NoError(t, err)

	// Long polling with WaitTimeSeconds=5, but message already available
	received, err := e.ReceiveMessage(ctx, "q", 1, 30, 5)
	require.NoError(t, err)
	require.Len(t, received, 1)
	assert.Equal(t, "already here", received[0].Body)
}

func TestLongPolling_BlocksUntilMessageArrives(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	var received []*sqs.Message
	var recvErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		received, recvErr = e.ReceiveMessage(ctx, "q", 1, 30, 5)
	}()

	// Give the goroutine time to start blocking
	time.Sleep(50 * time.Millisecond)

	// Send a message to wake the waiter
	_, err = e.SendMessage("q", "arrived later")
	require.NoError(t, err)

	wg.Wait()
	require.NoError(t, recvErr)
	require.Len(t, received, 1)
	assert.Equal(t, "arrived later", received[0].Body)
}

func TestLongPolling_TimesOutWithNoMessages(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	start := time.Now()
	received, err := e.ReceiveMessage(ctx, "q", 1, 30, 1)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, received)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "should wait close to WaitTimeSeconds")
}

func TestLongPolling_CancelledByContext(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)

	var received []*sqs.Message
	var recvErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		received, recvErr = e.ReceiveMessage(cancelCtx, "q", 1, 30, 20)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	wg.Wait()
	require.NoError(t, recvErr)
	assert.Empty(t, received)
}

func TestLongPolling_WakesOnVisibilityExpiry(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	// Send a message and receive it with a short visibility timeout
	_, err = e.SendMessage("q", "will-expire")
	require.NoError(t, err)

	received, err := e.ReceiveMessage(ctx, "q", 1, 1, 0) // 1 second visibility
	require.NoError(t, err)
	require.Len(t, received, 1)

	// Queue is now empty (message is inflight). Start a long poll that should
	// pick up the message once its visibility timeout expires (~1s).
	var received2 []*sqs.Message
	var recvErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		received2, recvErr = e.ReceiveMessage(ctx, "q", 1, 30, 5)
	}()

	wg.Wait()
	require.NoError(t, recvErr)
	require.Len(t, received2, 1)
	assert.Equal(t, "will-expire", received2[0].Body)
	assert.Equal(t, 2, received2[0].ReceiveCount)
}

func TestLongPolling_MultipleWaitersAllWake(t *testing.T) {
	ctx := context.Background()
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	const numWaiters = 3
	results := make([][]*sqs.Message, numWaiters)
	var wg sync.WaitGroup
	wg.Add(numWaiters)

	errs := make([]error, numWaiters)
	for i := range numWaiters {
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = e.ReceiveMessage(ctx, "q", 1, 30, 5)
		}(i)
	}

	time.Sleep(50 * time.Millisecond)

	// Send enough messages for all waiters
	for i := range numWaiters {
		_, err = e.SendMessage("q", fmt.Sprintf("msg-%d", i))
		require.NoError(t, err)
	}

	wg.Wait()

	for i, e := range errs {
		require.NoError(t, e, "waiter %d returned error", i)
	}

	// All messages should be consumed across the waiters
	totalReceived := 0
	for _, r := range results {
		totalReceived += len(r)
	}
	assert.Equal(t, numWaiters, totalReceived)
}
