package sqs_test

import (
	"testing"

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
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	msg, err := e.SendMessage("q", "hello world")
	require.NoError(t, err)
	assert.NotEmpty(t, msg.MessageID)
	assert.Equal(t, "hello world", msg.Body)
	assert.NotEmpty(t, msg.MD5OfBody)

	received, err := e.ReceiveMessage("q", 1, 30)
	require.NoError(t, err)
	require.Len(t, received, 1)
	assert.Equal(t, msg.MessageID, received[0].MessageID)
	assert.Equal(t, "hello world", received[0].Body)
	assert.Equal(t, 1, received[0].ReceiveCount)
	assert.NotEmpty(t, received[0].ReceiptHandle)
}

func TestReceiveMessageEmptyQueue(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	received, err := e.ReceiveMessage("q", 1, 30)
	require.NoError(t, err)
	assert.Empty(t, received)
}

func TestReceiveMessageNonExistentQueue(t *testing.T) {
	e := newEngine()

	_, err := e.ReceiveMessage("nope", 1, 30)
	assert.ErrorIs(t, err, sqs.ErrQueueDoesNotExist)
}

func TestDeleteMessage(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "msg1")
	require.NoError(t, err)

	received, err := e.ReceiveMessage("q", 1, 30)
	require.NoError(t, err)
	require.Len(t, received, 1)

	err = e.DeleteMessage("q", received[0].ReceiptHandle)
	require.NoError(t, err)

	// After delete, message should not reappear even with visibility=0
	received2, err := e.ReceiveMessage("q", 1, 0)
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
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	for i := range 5 {
		_, err = e.SendMessage("q", "msg"+string(rune('0'+i)))
		require.NoError(t, err)
	}

	received, err := e.ReceiveMessage("q", 3, 30)
	require.NoError(t, err)
	assert.Len(t, received, 3)

	// Remaining 2 still available
	received2, err := e.ReceiveMessage("q", 10, 30)
	require.NoError(t, err)
	assert.Len(t, received2, 2)
}

func TestMessageInvisibleAfterReceive(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "msg")
	require.NoError(t, err)

	// Receive with a long visibility timeout
	received, err := e.ReceiveMessage("q", 1, 300)
	require.NoError(t, err)
	assert.Len(t, received, 1)

	// Should not be receivable again immediately
	received2, err := e.ReceiveMessage("q", 1, 30)
	require.NoError(t, err)
	assert.Empty(t, received2)
}

func TestMessageReappearsAfterVisibilityTimeout(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	sent, err := e.SendMessage("q", "retry-me")
	require.NoError(t, err)

	// Receive with zero visibility timeout — message becomes available on next call
	received, err := e.ReceiveMessage("q", 1, 0)
	require.NoError(t, err)
	require.Len(t, received, 1)
	assert.Equal(t, "retry-me", received[0].Body)
	assert.Equal(t, sent.MessageID, received[0].MessageID)
	assert.Equal(t, 1, received[0].ReceiveCount)
	firstHandle := received[0].ReceiptHandle

	// Next ReceiveMessage should find the message again (visibility expired)
	received2, err := e.ReceiveMessage("q", 1, 0)
	require.NoError(t, err)
	require.Len(t, received2, 1)

	// Same message (same ID and body), but new receipt handle and incremented count
	assert.Equal(t, sent.MessageID, received2[0].MessageID)
	assert.Equal(t, "retry-me", received2[0].Body)
	assert.Equal(t, 2, received2[0].ReceiveCount)
	assert.NotEqual(t, firstHandle, received2[0].ReceiptHandle)
}

func TestOldReceiptHandleInvalidAfterReappearance(t *testing.T) {
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "msg")
	require.NoError(t, err)

	// Receive with zero visibility timeout
	received, err := e.ReceiveMessage("q", 1, 0)
	require.NoError(t, err)
	require.Len(t, received, 1)
	oldHandle := received[0].ReceiptHandle

	// Let message reappear and receive it again with a normal timeout
	received2, err := e.ReceiveMessage("q", 1, 30)
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
	e := newEngine()

	_, err := e.CreateQueue("q")
	require.NoError(t, err)

	_, err = e.SendMessage("q", "persistent")
	require.NoError(t, err)

	// Receive the same message 5 times without deleting
	for i := 1; i <= 5; i++ {
		received, err := e.ReceiveMessage("q", 1, 0)
		require.NoError(t, err)
		require.Len(t, received, 1)
		assert.Equal(t, "persistent", received[0].Body)
		assert.Equal(t, i, received[0].ReceiveCount)
	}
}
