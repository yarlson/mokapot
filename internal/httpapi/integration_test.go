package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/httpapi"
	"github.com/yarlson/mokapot/internal/sns"
	"github.com/yarlson/mokapot/internal/sqs"
)

func newIntegrationSetup(t *testing.T) (*awssqs.Client, *httptest.Server, *sqs.Engine) {
	t.Helper()

	// Use a placeholder host; replaced below once the test server starts.
	sqsEngine := sqs.NewEngine("eu-central-1", "000000000000", "placeholder")
	sqsHandler := sqs.NewHandler(sqsEngine)

	enqueue := func(queueName, body string) error {
		_, err := sqsEngine.SendMessage(queueName, body, 0)
		return err
	}
	snsEngine := sns.NewEngine("eu-central-1", "000000000000", enqueue)
	snsHandler := sns.NewHandler(snsEngine)

	ts := httptest.NewServer(httpapi.NewServer(sqsHandler, snsHandler))

	// Update the engine host so generated queue URLs point at the test server.
	sqsEngine.SetHost(ts.Listener.Addr().String())

	client := awssqs.New(awssqs.Options{
		Region:       "eu-central-1",
		BaseEndpoint: aws.String(ts.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	return client, ts, sqsEngine
}

func newIntegrationClient(t *testing.T) (*awssqs.Client, *httptest.Server) {
	t.Helper()
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

// --- Delayed message tests ---

func TestIntegration_DelayedMessage_NotReceivedBeforeDelay(t *testing.T) {
	client, ts, engine := newIntegrationSetup(t)
	defer ts.Close()
	ctx := context.Background()

	now := time.Now()
	engine.SetClock(func() time.Time { return now })

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("delay-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send with 10-second delay
	sendOut, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:     queueURL,
		MessageBody:  aws.String("delayed message"),
		DelaySeconds: 10,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *sendOut.MessageId)

	// Should not be receivable immediately
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut.Messages)

	// Advance clock past the delay
	now = now.Add(11 * time.Second)
	engine.SetClock(func() time.Time { return now })

	// Now should be receivable
	recvOut, err = client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "delayed message", *recvOut.Messages[0].Body)
	assert.Equal(t, *sendOut.MessageId, *recvOut.Messages[0].MessageId)
}

func TestIntegration_DelayedMessage_ZeroDelayIsImmediate(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("zero-delay-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send with explicit zero delay
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:     queueURL,
		MessageBody:  aws.String("immediate"),
		DelaySeconds: 0,
	})
	require.NoError(t, err)

	// Should be receivable immediately
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "immediate", *recvOut.Messages[0].Body)
}

func TestIntegration_DelayedMessage_LongPollWakesOnDelayExpiry(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("delay-longpoll-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send with 1 second delay
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:     queueURL,
		MessageBody:  aws.String("delay-then-poll"),
		DelaySeconds: 1,
	})
	require.NoError(t, err)

	// Long poll with 5s wait — should return after ~1s when delay expires
	start := time.Now()
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "delay-then-poll", *recvOut.Messages[0].Body)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "should wait for delay to expire")
	assert.Less(t, elapsed, 4*time.Second, "should not wait full poll duration")
}

// --- Get/SetQueueAttributes tests ---

func TestIntegration_GetQueueAttributes(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("attrs-queue"),
	})
	require.NoError(t, err)

	out, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       createOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	require.NoError(t, err)
	assert.Equal(t, "30", out.Attributes["VisibilityTimeout"])
	assert.Equal(t, "0", out.Attributes["DelaySeconds"])
	assert.Contains(t, out.Attributes["QueueArn"], "attrs-queue")
}

func TestIntegration_SetQueueAttributes(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("set-attrs-queue"),
	})
	require.NoError(t, err)

	_, err = client.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl: createOut.QueueUrl,
		Attributes: map[string]string{
			"VisibilityTimeout": "60",
		},
	})
	require.NoError(t, err)

	out, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       createOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{"VisibilityTimeout"},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", out.Attributes["VisibilityTimeout"])
}

// --- Dead-letter queue integration tests ---

func TestIntegration_DLQ_MessageMovedAfterMaxReceiveCount(t *testing.T) {
	client, ts, engine := newIntegrationSetup(t)
	defer ts.Close()
	ctx := context.Background()

	now := time.Now()
	engine.SetClock(func() time.Time { return now })

	// Create source queue and DLQ
	dlqOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("dlq-integration"),
	})
	require.NoError(t, err)

	// Get DLQ ARN
	dlqAttrs, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	dlqARN := dlqAttrs.Attributes["QueueArn"]

	sourceOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("source-integration"),
	})
	require.NoError(t, err)

	// Set RedrivePolicy on source queue
	rpJSON, err := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     3,
	})
	require.NoError(t, err)
	_, err = client.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl: sourceOut.QueueUrl,
		Attributes: map[string]string{
			"RedrivePolicy": string(rpJSON),
		},
	})
	require.NoError(t, err)

	// Send a message to the source queue
	sendOut, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    sourceOut.QueueUrl,
		MessageBody: aws.String("poison message"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *sendOut.MessageId)

	// Receive 3 times without deleting
	for i := 0; i < 3; i++ {
		recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            sourceOut.QueueUrl,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   1,
		})
		require.NoError(t, err)
		require.Len(t, recvOut.Messages, 1)
		assert.Equal(t, "poison message", *recvOut.Messages[0].Body)

		// Advance clock past visibility timeout
		now = now.Add(2 * time.Second)
		engine.SetClock(func() time.Time { return now })
	}

	// Next receive should find source empty (message moved to DLQ)
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            sourceOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut.Messages)

	// DLQ should have the message
	dlqRecv, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            dlqOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, dlqRecv.Messages, 1)
	assert.Equal(t, "poison message", *dlqRecv.Messages[0].Body)
}

func TestIntegration_DLQ_RedrivePolicy_InvalidDLQ(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("source-no-dlq"),
	})
	require.NoError(t, err)

	rpJSON, err := json.Marshal(map[string]any{
		"deadLetterTargetArn": "arn:aws:sqs:eu-central-1:000000000000:nonexistent",
		"maxReceiveCount":     3,
	})
	require.NoError(t, err)

	_, err = client.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl: createOut.QueueUrl,
		Attributes: map[string]string{
			"RedrivePolicy": string(rpJSON),
		},
	})
	assert.Error(t, err, "should fail when DLQ does not exist")
}

func TestIntegration_DLQ_GetRedrivePolicy(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	// Create DLQ first
	dlqOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("dlq-get-policy"),
	})
	require.NoError(t, err)

	dlqAttrs, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	dlqARN := dlqAttrs.Attributes["QueueArn"]

	// Create source and set RedrivePolicy
	sourceOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("source-get-policy"),
	})
	require.NoError(t, err)

	rpJSON, err := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     5,
	})
	require.NoError(t, err)
	_, err = client.SetQueueAttributes(ctx, &awssqs.SetQueueAttributesInput{
		QueueUrl: sourceOut.QueueUrl,
		Attributes: map[string]string{
			"RedrivePolicy": string(rpJSON),
		},
	})
	require.NoError(t, err)

	// GetQueueAttributes should include RedrivePolicy
	attrs, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       sourceOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameRedrivePolicy},
	})
	require.NoError(t, err)

	rpStr, ok := attrs.Attributes["RedrivePolicy"]
	assert.True(t, ok, "RedrivePolicy should be present")

	var parsed map[string]any
	err = json.Unmarshal([]byte(rpStr), &parsed)
	require.NoError(t, err)
	assert.Equal(t, dlqARN, parsed["deadLetterTargetArn"])
}

// --- Batch operation integration tests ---

func TestIntegration_SendMessageBatch(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("batch-send-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send a batch of 5 messages
	entries := make([]sqstypes.SendMessageBatchRequestEntry, 5)
	for i := range 5 {
		id := fmt.Sprintf("msg-%d", i)
		body := fmt.Sprintf("body-%d", i)
		entries[i] = sqstypes.SendMessageBatchRequestEntry{
			Id:          aws.String(id),
			MessageBody: aws.String(body),
		}
	}

	batchOut, err := client.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{
		QueueUrl: queueURL,
		Entries:  entries,
	})
	require.NoError(t, err)
	assert.Len(t, batchOut.Successful, 5)
	assert.Empty(t, batchOut.Failed)

	for i, s := range batchOut.Successful {
		assert.Equal(t, fmt.Sprintf("msg-%d", i), *s.Id)
		assert.NotEmpty(t, *s.MessageId)
		assert.NotEmpty(t, *s.MD5OfMessageBody)
	}

	// Receive all 5 messages
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Len(t, recvOut.Messages, 5)
}

func TestIntegration_SendMessageBatch_WithDelay(t *testing.T) {
	client, ts, engine := newIntegrationSetup(t)
	defer ts.Close()
	ctx := context.Background()

	now := time.Now()
	engine.SetClock(func() time.Time { return now })

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("batch-send-delay-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	batchOut, err := client.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{
		QueueUrl: queueURL,
		Entries: []sqstypes.SendMessageBatchRequestEntry{
			{
				Id:          aws.String("immediate"),
				MessageBody: aws.String("no-delay"),
			},
			{
				Id:           aws.String("delayed"),
				MessageBody:  aws.String("with-delay"),
				DelaySeconds: 10,
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, batchOut.Successful, 2)

	// Only immediate message should be receivable
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "no-delay", *recvOut.Messages[0].Body)

	// Advance past the delay
	now = now.Add(11 * time.Second)
	engine.SetClock(func() time.Time { return now })

	recvOut, err = client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "with-delay", *recvOut.Messages[0].Body)
}

func TestIntegration_DeleteMessageBatch(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("batch-delete-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send 3 messages
	for i := range 3 {
		_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl:    queueURL,
			MessageBody: aws.String(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Receive all 3
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 3)

	// Delete all 3 in a batch
	delEntries := make([]sqstypes.DeleteMessageBatchRequestEntry, len(recvOut.Messages))
	for i, msg := range recvOut.Messages {
		delEntries[i] = sqstypes.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("del-%d", i)),
			ReceiptHandle: msg.ReceiptHandle,
		}
	}

	delOut, err := client.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{
		QueueUrl: queueURL,
		Entries:  delEntries,
	})
	require.NoError(t, err)
	assert.Len(t, delOut.Successful, 3)
	assert.Empty(t, delOut.Failed)

	// Queue should be empty
	recvOut, err = client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut.Messages)
}

func TestIntegration_DeleteMessageBatch_PartialFailure(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("batch-del-partial-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send one message
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("real-msg"),
	})
	require.NoError(t, err)

	// Receive it
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	// Delete batch: one valid handle, one bogus
	delOut, err := client.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{
		QueueUrl: queueURL,
		Entries: []sqstypes.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String("good"),
				ReceiptHandle: recvOut.Messages[0].ReceiptHandle,
			},
			{
				Id:            aws.String("bad"),
				ReceiptHandle: aws.String("bogus-handle"),
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, delOut.Successful, 1)
	assert.Equal(t, "good", *delOut.Successful[0].Id)
	assert.Len(t, delOut.Failed, 1)
	assert.Equal(t, "bad", *delOut.Failed[0].Id)
	assert.Equal(t, "ReceiptHandleIsInvalid", *delOut.Failed[0].Code)
	assert.True(t, delOut.Failed[0].SenderFault)
}

func TestIntegration_SendMessageBatch_10Messages(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("batch-10-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send exactly 10 messages (the maximum)
	entries := make([]sqstypes.SendMessageBatchRequestEntry, 10)
	for i := range 10 {
		entries[i] = sqstypes.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("msg-%d", i)),
			MessageBody: aws.String(fmt.Sprintf("body-%d", i)),
		}
	}

	batchOut, err := client.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{
		QueueUrl: queueURL,
		Entries:  entries,
	})
	require.NoError(t, err)
	assert.Len(t, batchOut.Successful, 10)
	assert.Empty(t, batchOut.Failed)

	// Receive all 10 (may take multiple receives due to max 10 per call)
	allReceived := make([]sqstypes.Message, 0, 10)
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	allReceived = append(allReceived, recvOut.Messages...)
	assert.Len(t, allReceived, 10)
}

func TestIntegration_PurgeQueue(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("purge-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send several messages
	for i := range 5 {
		_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
			QueueUrl:    queueURL,
			MessageBody: aws.String(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Purge the queue
	_, err = client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{
		QueueUrl: queueURL,
	})
	require.NoError(t, err)

	// Queue should be empty
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut.Messages)
}

func TestIntegration_PurgeQueue_ClearsInflight(t *testing.T) {
	client, ts, engine := newIntegrationSetup(t)
	defer ts.Close()
	ctx := context.Background()

	now := time.Now()
	engine.SetClock(func() time.Time { return now })

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("purge-inflight-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send a message
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("inflight-msg"),
	})
	require.NoError(t, err)

	// Receive it (now inflight)
	recvOut, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	// Purge the queue — should clear inflight too
	_, err = client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{
		QueueUrl: queueURL,
	})
	require.NoError(t, err)

	// Advance past visibility timeout so inflight would reappear if not purged
	now = now.Add(60 * time.Second)
	engine.SetClock(func() time.Time { return now })

	recvOut, err = client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut.Messages)
}

func TestIntegration_PurgeQueue_NonExistentQueue(t *testing.T) {
	client, ts := newIntegrationClient(t)
	defer ts.Close()
	ctx := context.Background()

	_, err := client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{
		QueueUrl: aws.String("http://localhost/000000000000/no-such-queue"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistentQueue")
}

func TestIntegration_PurgeQueue_CooldownEnforced(t *testing.T) {
	client, ts, engine := newIntegrationSetup(t)
	defer ts.Close()
	ctx := context.Background()

	now := time.Now()
	engine.SetClock(func() time.Time { return now })

	createOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("purge-cooldown-queue"),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// First purge succeeds
	_, err = client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{
		QueueUrl: queueURL,
	})
	require.NoError(t, err)

	// Immediate second purge should fail
	_, err = client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{
		QueueUrl: queueURL,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PurgeQueueInProgress")

	// Advance past the 60-second cooldown
	now = now.Add(61 * time.Second)
	engine.SetClock(func() time.Time { return now })

	// Should succeed again
	_, err = client.PurgeQueue(ctx, &awssqs.PurgeQueueInput{
		QueueUrl: queueURL,
	})
	require.NoError(t, err)
}

// --- SNS integration tests ---

type snsIntegrationSetup struct {
	sqsClient *awssqs.Client
	snsClient *awssns.Client
	server    *httptest.Server
	sqsEngine *sqs.Engine
}

func newSNSIntegrationSetup(t *testing.T) *snsIntegrationSetup {
	t.Helper()

	sqsEngine := sqs.NewEngine("eu-central-1", "000000000000", "placeholder")
	sqsHandler := sqs.NewHandler(sqsEngine)

	enqueue := func(queueName, body string) error {
		_, err := sqsEngine.SendMessage(queueName, body, 0)
		return err
	}
	snsEngine := sns.NewEngine("eu-central-1", "000000000000", enqueue)
	snsHandler := sns.NewHandler(snsEngine)

	ts := httptest.NewServer(httpapi.NewServer(sqsHandler, snsHandler))
	sqsEngine.SetHost(ts.Listener.Addr().String())

	sqsClient := awssqs.New(awssqs.Options{
		Region:       "eu-central-1",
		BaseEndpoint: aws.String(ts.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	snsClient := awssns.New(awssns.Options{
		Region:       "eu-central-1",
		BaseEndpoint: aws.String(ts.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	return &snsIntegrationSetup{
		sqsClient: sqsClient,
		snsClient: snsClient,
		server:    ts,
		sqsEngine: sqsEngine,
	}
}

func TestIntegration_SNS_CreateTopic(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	out, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("test-topic"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.TopicArn)
	assert.Contains(t, *out.TopicArn, "test-topic")
	assert.Contains(t, *out.TopicArn, "arn:aws:sns:eu-central-1:000000000000:test-topic")
}

func TestIntegration_SNS_CreateTopicIdempotent(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	out1, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("idempotent-topic"),
	})
	require.NoError(t, err)

	out2, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("idempotent-topic"),
	})
	require.NoError(t, err)

	assert.Equal(t, *out1.TopicArn, *out2.TopicArn)
}

func TestIntegration_SNS_Subscribe(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	// Create topic
	topicOut, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("sub-topic"),
	})
	require.NoError(t, err)

	// Create SQS queue
	queueOut, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("sub-queue"),
	})
	require.NoError(t, err)

	// Get queue ARN
	attrOut, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       queueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Subscribe
	subOut, err := s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)
	require.NotNil(t, subOut.SubscriptionArn)
	assert.Contains(t, *subOut.SubscriptionArn, *topicOut.TopicArn)
}

func TestIntegration_SNS_PublishFanout(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	// Create topic
	topicOut, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("fanout-topic"),
	})
	require.NoError(t, err)

	// Create two SQS queues
	queue1Out, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("fanout-queue-1"),
	})
	require.NoError(t, err)

	queue2Out, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("fanout-queue-2"),
	})
	require.NoError(t, err)

	// Get queue ARNs
	attr1, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       queue1Out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queue1ARN := attr1.Attributes["QueueArn"]

	attr2, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       queue2Out.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queue2ARN := attr2.Attributes["QueueArn"]

	// Subscribe both queues
	_, err = s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queue1ARN),
	})
	require.NoError(t, err)

	_, err = s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queue2ARN),
	})
	require.NoError(t, err)

	// Publish a message
	pubOut, err := s.snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: topicOut.TopicArn,
		Message:  aws.String("fanout message"),
		Subject:  aws.String("Test Subject"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *pubOut.MessageId)

	// Both queues should receive the message
	recv1, err := s.sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queue1Out.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv1.Messages, 1)

	recv2, err := s.sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queue2Out.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recv2.Messages, 1)

	// Verify both messages contain SNS envelope
	for _, msgs := range [][]*string{
		{recv1.Messages[0].Body},
		{recv2.Messages[0].Body},
	} {
		var envelope map[string]string
		err = json.Unmarshal([]byte(*msgs[0]), &envelope)
		require.NoError(t, err)
		assert.Equal(t, "Notification", envelope["Type"])
		assert.Equal(t, *pubOut.MessageId, envelope["MessageId"])
		assert.Equal(t, *topicOut.TopicArn, envelope["TopicArn"])
		assert.Equal(t, "fanout message", envelope["Message"])
		assert.Equal(t, "Test Subject", envelope["Subject"])
		assert.NotEmpty(t, envelope["Timestamp"])
	}
}

func TestIntegration_SNS_PublishSingleQueue(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	// Create topic
	topicOut, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("single-topic"),
	})
	require.NoError(t, err)

	// Create SQS queue
	queueOut, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("single-queue"),
	})
	require.NoError(t, err)

	// Get queue ARN
	attrOut, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       queueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Subscribe
	_, err = s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)

	// Publish
	pubOut, err := s.snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: topicOut.TopicArn,
		Message:  aws.String("hello SNS"),
	})
	require.NoError(t, err)

	// Receive from SQS queue
	recvOut, err := s.sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	// Verify envelope
	var envelope map[string]string
	err = json.Unmarshal([]byte(*recvOut.Messages[0].Body), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, *pubOut.MessageId, envelope["MessageId"])
	assert.Equal(t, "hello SNS", envelope["Message"])

	// Delete the message to confirm full lifecycle
	_, err = s.sqsClient.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      queueOut.QueueUrl,
		ReceiptHandle: recvOut.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)

	// Queue should be empty
	recvOut2, err := s.sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut2.Messages)
}

func TestIntegration_SNS_PublishNoSubscribers(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	// Create topic with no subscribers
	topicOut, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("empty-topic"),
	})
	require.NoError(t, err)

	// Publish should succeed even with no subscribers
	pubOut, err := s.snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: topicOut.TopicArn,
		Message:  aws.String("to nobody"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *pubOut.MessageId)
}

func TestIntegration_SNS_PublishTopicNotFound(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	_, err := s.snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: aws.String("arn:aws:sns:eu-central-1:000000000000:nonexistent"),
		Message:  aws.String("hello"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NotFound")
}

func TestIntegration_SNS_SubscribeTopicNotFound(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	_, err := s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: aws.String("arn:aws:sns:eu-central-1:000000000000:nonexistent"),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:eu-central-1:000000000000:q"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NotFound")
}

// --- SNS Raw Message Delivery integration tests ---

func TestIntegration_SNS_RawMessageDelivery(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	// Create topic
	topicOut, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("raw-topic"),
	})
	require.NoError(t, err)

	// Create SQS queue
	queueOut, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("raw-queue"),
	})
	require.NoError(t, err)

	// Get queue ARN
	attrOut, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       queueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Subscribe with raw delivery
	subOut, err := s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)

	// Enable RawMessageDelivery
	_, err = s.snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: subOut.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	// Publish a message
	_, err = s.snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: topicOut.TopicArn,
		Message:  aws.String("raw body"),
	})
	require.NoError(t, err)

	// Receive from SQS
	recvOut, err := s.sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            queueOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	// Body should be the raw message, not wrapped in SNS envelope
	assert.Equal(t, "raw body", *recvOut.Messages[0].Body)
}

func TestIntegration_SNS_EnvelopeVsRawDelivery(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	// Create topic
	topicOut, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("mixed-delivery-topic"),
	})
	require.NoError(t, err)

	// Create two SQS queues: one for raw, one for envelope
	rawQueueOut, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("raw-sub-queue"),
	})
	require.NoError(t, err)

	envelopeQueueOut, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("envelope-sub-queue"),
	})
	require.NoError(t, err)

	// Get queue ARNs
	rawAttr, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       rawQueueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	rawQueueARN := rawAttr.Attributes["QueueArn"]

	envAttr, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       envelopeQueueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	envQueueARN := envAttr.Attributes["QueueArn"]

	// Subscribe both queues
	rawSubOut, err := s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(rawQueueARN),
	})
	require.NoError(t, err)

	_, err = s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(envQueueARN),
	})
	require.NoError(t, err)

	// Enable RawMessageDelivery on raw subscription only
	_, err = s.snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: rawSubOut.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	// Publish a message
	pubOut, err := s.snsClient.Publish(ctx, &awssns.PublishInput{
		TopicArn: topicOut.TopicArn,
		Message:  aws.String("test payload"),
	})
	require.NoError(t, err)

	// Raw queue should receive plain body
	rawRecv, err := s.sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            rawQueueOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, rawRecv.Messages, 1)
	assert.Equal(t, "test payload", *rawRecv.Messages[0].Body)

	// Envelope queue should receive SNS JSON envelope
	envRecv, err := s.sqsClient.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            envelopeQueueOut.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, envRecv.Messages, 1)

	var envelope map[string]string
	err = json.Unmarshal([]byte(*envRecv.Messages[0].Body), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, *pubOut.MessageId, envelope["MessageId"])
	assert.Equal(t, "test payload", envelope["Message"])
}

func TestIntegration_SNS_GetSubscriptionAttributes(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	// Create topic
	topicOut, err := s.snsClient.CreateTopic(ctx, &awssns.CreateTopicInput{
		Name: aws.String("get-sub-attr-topic"),
	})
	require.NoError(t, err)

	// Create SQS queue
	queueOut, err := s.sqsClient.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String("get-sub-attr-queue"),
	})
	require.NoError(t, err)

	attrOut, err := s.sqsClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       queueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Subscribe
	subOut, err := s.snsClient.Subscribe(ctx, &awssns.SubscribeInput{
		TopicArn: topicOut.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)

	// Get subscription attributes — default
	gsaOut, err := s.snsClient.GetSubscriptionAttributes(ctx, &awssns.GetSubscriptionAttributesInput{
		SubscriptionArn: subOut.SubscriptionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "false", gsaOut.Attributes["RawMessageDelivery"])
	assert.Equal(t, "sqs", gsaOut.Attributes["Protocol"])
	assert.Equal(t, queueARN, gsaOut.Attributes["Endpoint"])
	assert.Equal(t, *topicOut.TopicArn, gsaOut.Attributes["TopicArn"])

	// Set RawMessageDelivery to true
	_, err = s.snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: subOut.SubscriptionArn,
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	// Get again — should reflect the change
	gsaOut, err = s.snsClient.GetSubscriptionAttributes(ctx, &awssns.GetSubscriptionAttributesInput{
		SubscriptionArn: subOut.SubscriptionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "true", gsaOut.Attributes["RawMessageDelivery"])
}

func TestIntegration_SNS_SetSubscriptionAttributes_NotFound(t *testing.T) {
	s := newSNSIntegrationSetup(t)
	defer s.server.Close()
	ctx := context.Background()

	_, err := s.snsClient.SetSubscriptionAttributes(ctx, &awssns.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String("arn:aws:sns:eu-central-1:000000000000:topic:nonexistent"),
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NotFound")
}
