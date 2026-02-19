package httpapi_test

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/devstack/internal/httpapi"
	"github.com/yarlson/devstack/internal/sqs"
)

func newIntegrationSetup(t *testing.T) (*awssqs.Client, *httptest.Server, *sqs.Engine) {
	t.Helper()

	// Use a placeholder host; replaced below once the test server starts.
	engine := sqs.NewEngine("eu-central-1", "000000000000", "placeholder")
	handler := sqs.NewHandler(engine)
	ts := httptest.NewServer(httpapi.NewServer(handler))

	// Update the engine host so generated queue URLs point at the test server.
	engine.SetHost(ts.Listener.Addr().String())

	client := awssqs.New(awssqs.Options{
		Region:       "eu-central-1",
		BaseEndpoint: aws.String(ts.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	return client, ts, engine
}

func newIntegrationClient(t *testing.T) (*awssqs.Client, *httptest.Server) {
	client, ts, _ := newIntegrationSetup(t)
	return client, ts
}

func TestIntegration_CreateQueue(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	out, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("integration-test-queue"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.QueueUrl)
	assert.Contains(t, *out.QueueUrl, "integration-test-queue")
}

func TestIntegration_GetQueueUrl(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	_, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("url-test-queue"),
	})
	require.NoError(t, err)

	out, err := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{
		QueueName: aws.String("url-test-queue"),
	})
	require.NoError(t, err)
	assert.Contains(t, *out.QueueUrl, "url-test-queue")
}

func TestIntegration_SendReceiveDelete(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	// Create queue
	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("srd-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send message
	sendOut, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("Hello from SDK"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *sendOut.MessageId)
	assert.NotEmpty(t, *sendOut.MD5OfMessageBody)

	// Receive message
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	msg := recvOut.Messages[0]
	assert.Equal(t, "Hello from SDK", *msg.Body)
	assert.Equal(t, *sendOut.MessageId, *msg.MessageId)
	assert.NotEmpty(t, *msg.ReceiptHandle)

	// Delete message
	_, err = client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	require.NoError(t, err)

	// Verify queue is empty
	recvOut2, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut2.Messages)
}

func TestIntegration_GetQueueUrl_NotFound(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	_, err := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{
		QueueName: aws.String("no-such-queue"),
	})
	assert.Error(t, err)
}

func TestIntegration_MultipleMessages(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("multi-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send 5 messages
	for i := range 5 {
		_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl:    queueURL,
			MessageBody: aws.String("msg-" + string(rune('A'+i))),
		})
		require.NoError(t, err)
	}

	// Receive 3 messages
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 3,
	})
	require.NoError(t, err)
	assert.Len(t, recvOut.Messages, 3)

	// Receive remaining 2
	recvOut2, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Len(t, recvOut2.Messages, 2)
}

func TestIntegration_MessageReappearsAfterVisibilityTimeout(t *testing.T) {
	client, ts, engine := newIntegrationSetup(t)
	defer ts.Close()
	ctx := context.Background()

	// Use a controllable clock so we don't need time.Sleep.
	now := time.Now()
	engine.SetClock(func() time.Time { return now })

	// Create queue
	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("visibility-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send a message
	sendOut, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("reappearing message"),
	})
	require.NoError(t, err)
	originalMessageID := *sendOut.MessageId

	// Receive with 1-second visibility timeout.
	// Note: SDK omits VisibilityTimeout=0 from the request body,
	// so we use 1s and advance the clock past it to test reappearance.
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "reappearing message", *recvOut.Messages[0].Body)
	assert.Equal(t, originalMessageID, *recvOut.Messages[0].MessageId)
	firstHandle := *recvOut.Messages[0].ReceiptHandle

	// Message should be invisible right now (clock hasn't advanced)
	recvEmpty, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recvEmpty.Messages)

	// Advance clock past the 1-second visibility timeout
	now = now.Add(2 * time.Second)
	engine.SetClock(func() time.Time { return now })

	// Message should reappear after timeout
	recvOut2, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut2.Messages, 1)

	// Same message (same ID and body), new receipt handle
	assert.Equal(t, originalMessageID, *recvOut2.Messages[0].MessageId)
	assert.Equal(t, "reappearing message", *recvOut2.Messages[0].Body)
	assert.NotEqual(t, firstHandle, *recvOut2.Messages[0].ReceiptHandle)

	// Old receipt handle should be invalid
	_, err = client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: aws.String(firstHandle),
	})
	assert.Error(t, err)

	// New receipt handle should work for delete
	_, err = client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: recvOut2.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)

	// Message should now be gone
	recvOut3, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut3.Messages)
}

func TestIntegration_LongPolling_BlocksUntilMessageArrives(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("longpoll-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	var recvOut *awssqs.ReceiveMessageOutput
	var recvErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		recvOut, recvErr = client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            queueURL,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     5,
		})
	}()

	// Give the long poll time to register
	time.Sleep(100 * time.Millisecond)

	// Send a message — should wake the long poller
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("long-polled message"),
	})
	require.NoError(t, err)

	wg.Wait()
	require.NoError(t, recvErr)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "long-polled message", *recvOut.Messages[0].Body)
}

func TestIntegration_LongPolling_ReturnsImmediatelyWhenMessagesExist(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("longpoll-immediate-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send a message first
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("already present"),
	})
	require.NoError(t, err)

	// Long poll should return immediately since a message is available
	start := time.Now()
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     10,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "already present", *recvOut.Messages[0].Body)
	assert.Less(t, elapsed, 2*time.Second, "should return immediately, not wait full WaitTimeSeconds")
}

func TestIntegration_LongPolling_TimesOut(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("longpoll-timeout-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	start := time.Now()
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, recvOut.Messages)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "should wait close to WaitTimeSeconds")
}
