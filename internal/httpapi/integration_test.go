package httpapi_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yarlson/devstack/internal/httpapi"
	"github.com/yarlson/devstack/internal/sqs"
)

func newIntegrationClient(t *testing.T) (*awssqs.Client, *httptest.Server) {
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
