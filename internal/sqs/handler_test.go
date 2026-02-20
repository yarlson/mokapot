package sqs_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/sqs"
)

func newTestHandler() *sqs.Handler {
	engine := sqs.NewEngine("eu-central-1", "000000000000", "localhost:4566")
	return sqs.NewHandler(engine)
}

func postQuery(handler *sqs.Handler, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	queueName := ""
	// Extract queue name from path like /000000000000/my-queue
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 2 {
		queueName = parts[1]
	}
	handler.HandleRequest(rec, req, queueName)
	return rec
}

// --- Golden response shape: CreateQueue ---

func TestXML_CreateQueue_Shape(t *testing.T) {
	h := newTestHandler()
	rec := postQuery(h, "/", "Action=CreateQueue&QueueName=golden-queue")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/xml; charset=utf-8", rec.Header().Get("Content-Type"))

	body := rec.Body.String()
	assert.Contains(t, body, "<CreateQueueResponse>")
	assert.Contains(t, body, "<CreateQueueResult>")
	assert.Contains(t, body, "<QueueUrl>")
	assert.Contains(t, body, "000000000000/golden-queue")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: GetQueueUrl ---

func TestXML_GetQueueUrl_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=get-url-queue")

	rec := postQuery(h, "/", "Action=GetQueueUrl&QueueName=get-url-queue")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<GetQueueUrlResponse>")
	assert.Contains(t, body, "<GetQueueUrlResult>")
	assert.Contains(t, body, "<QueueUrl>")
	assert.Contains(t, body, "get-url-queue")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: SendMessage ---

func TestXML_SendMessage_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=send-queue")

	rec := postQuery(h, "/000000000000/send-queue", "Action=SendMessage&MessageBody=test+body")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<SendMessageResponse>")
	assert.Contains(t, body, "<SendMessageResult>")
	assert.Contains(t, body, "<MessageId>")
	assert.Contains(t, body, "<MD5OfMessageBody>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: ReceiveMessage ---

func TestXML_ReceiveMessage_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=recv-queue")
	postQuery(h, "/000000000000/recv-queue", "Action=SendMessage&MessageBody=golden+msg")

	rec := postQuery(h, "/000000000000/recv-queue", "Action=ReceiveMessage&MaxNumberOfMessages=1")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ReceiveMessageResponse>")
	assert.Contains(t, body, "<ReceiveMessageResult>")
	assert.Contains(t, body, "<Message>")
	assert.Contains(t, body, "<MessageId>")
	assert.Contains(t, body, "<ReceiptHandle>")
	assert.Contains(t, body, "<MD5OfBody>")
	assert.Contains(t, body, "<Body>golden msg</Body>")
	assert.Contains(t, body, "<Attribute>")
	assert.Contains(t, body, "<Name>ApproximateReceiveCount</Name>")
	assert.Contains(t, body, "<Value>1</Value>")
	assert.Contains(t, body, "<Name>SentTimestamp</Name>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_ReceiveMessage_Empty_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=empty-queue")

	rec := postQuery(h, "/000000000000/empty-queue", "Action=ReceiveMessage")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ReceiveMessageResponse>")
	assert.Contains(t, body, "<ReceiveMessageResult>")
	assert.NotContains(t, body, "<Message>")
	assert.Contains(t, body, "<ResponseMetadata>")
}

// --- Golden response shape: DeleteMessage ---

func TestXML_DeleteMessage_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=del-queue")
	postQuery(h, "/000000000000/del-queue", "Action=SendMessage&MessageBody=to+delete")

	// Receive to get a handle
	recvRec := postQuery(h, "/000000000000/del-queue", "Action=ReceiveMessage&MaxNumberOfMessages=1")
	require.Equal(t, http.StatusOK, recvRec.Code)

	// Extract receipt handle from XML
	recvBody := recvRec.Body.String()
	start := strings.Index(recvBody, "<ReceiptHandle>") + len("<ReceiptHandle>")
	end := strings.Index(recvBody, "</ReceiptHandle>")
	require.Greater(t, end, start)
	handle := recvBody[start:end]

	rec := postQuery(h, "/000000000000/del-queue", "Action=DeleteMessage&ReceiptHandle="+handle)

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<DeleteMessageResponse>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: Error responses ---

func TestXML_Error_QueueDoesNotExist_Shape(t *testing.T) {
	h := newTestHandler()
	rec := postQuery(h, "/", "Action=GetQueueUrl&QueueName=nonexistent")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Error>")
	assert.Contains(t, body, "<Type>Sender</Type>")
	assert.Contains(t, body, "<Code>AWS.SimpleQueueService.NonExistentQueue</Code>")
	assert.Contains(t, body, "<Message>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_Error_InvalidAction_Shape(t *testing.T) {
	h := newTestHandler()
	rec := postQuery(h, "/", "Action=BogusAction")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>InvalidAction</Code>")
	assert.Contains(t, body, "<Message>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_Error_MissingQueueName_Shape(t *testing.T) {
	h := newTestHandler()
	rec := postQuery(h, "/", "Action=CreateQueue")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>InvalidParameterValue</Code>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_Error_ReceiptHandleIsInvalid_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=rh-queue")

	rec := postQuery(h, "/000000000000/rh-queue", "Action=DeleteMessage&ReceiptHandle=bogus-handle")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>ReceiptHandleIsInvalid</Code>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: SendMessage with DelaySeconds ---

func TestXML_SendMessage_WithDelaySeconds_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=delay-shape-queue")

	rec := postQuery(h, "/000000000000/delay-shape-queue", "Action=SendMessage&MessageBody=delayed+body&DelaySeconds=10")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<SendMessageResponse>")
	assert.Contains(t, body, "<SendMessageResult>")
	assert.Contains(t, body, "<MessageId>")
	assert.Contains(t, body, "<MD5OfMessageBody>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_SendMessage_InvalidDelaySeconds_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=bad-delay-queue")

	rec := postQuery(h, "/000000000000/bad-delay-queue", "Action=SendMessage&MessageBody=body&DelaySeconds=1000")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>InvalidParameterValue</Code>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: GetQueueAttributes ---

func TestXML_GetQueueAttributes_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=attr-queue")

	rec := postQuery(h, "/000000000000/attr-queue", "Action=GetQueueAttributes&AttributeName.1=All")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<GetQueueAttributesResponse>")
	assert.Contains(t, body, "<GetQueueAttributesResult>")
	assert.Contains(t, body, "<Attribute>")
	assert.Contains(t, body, "<Name>")
	assert.Contains(t, body, "<Value>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_GetQueueAttributes_Specific_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=attr-specific-queue")

	rec := postQuery(h, "/000000000000/attr-specific-queue", "Action=GetQueueAttributes&AttributeName.1=VisibilityTimeout")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<GetQueueAttributesResponse>")
	assert.Contains(t, body, "<Name>VisibilityTimeout</Name>")
	assert.Contains(t, body, "<Value>30</Value>")
}

// --- Golden response shape: SetQueueAttributes ---

func TestXML_SetQueueAttributes_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=set-attr-queue")

	rec := postQuery(h, "/000000000000/set-attr-queue", "Action=SetQueueAttributes&Attribute.1.Name=VisibilityTimeout&Attribute.1.Value=60")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<SetQueueAttributesResponse>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_SetQueueAttributes_MissingAttrs_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=set-attr-err-queue")

	rec := postQuery(h, "/000000000000/set-attr-err-queue", "Action=SetQueueAttributes")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>MissingParameter</Code>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: SendMessageBatch ---

func TestXML_SendMessageBatch_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=batch-send-queue")

	rec := postQuery(h, "/000000000000/batch-send-queue",
		"Action=SendMessageBatch"+
			"&SendMessageBatchRequestEntry.1.Id=msg1"+
			"&SendMessageBatchRequestEntry.1.MessageBody=hello+one"+
			"&SendMessageBatchRequestEntry.2.Id=msg2"+
			"&SendMessageBatchRequestEntry.2.MessageBody=hello+two")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<SendMessageBatchResponse>")
	assert.Contains(t, body, "<SendMessageBatchResult>")
	assert.Contains(t, body, "<SendMessageBatchResultEntry>")
	assert.Contains(t, body, "<Id>msg1</Id>")
	assert.Contains(t, body, "<Id>msg2</Id>")
	assert.Contains(t, body, "<MessageId>")
	assert.Contains(t, body, "<MD5OfMessageBody>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_SendMessageBatch_Empty_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=batch-send-empty-queue")

	rec := postQuery(h, "/000000000000/batch-send-empty-queue", "Action=SendMessageBatch")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>AWS.SimpleQueueService.EmptyBatchRequest</Code>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: DeleteMessageBatch ---

func TestXML_DeleteMessageBatch_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=batch-del-queue")
	postQuery(h, "/000000000000/batch-del-queue", "Action=SendMessage&MessageBody=msg1")
	postQuery(h, "/000000000000/batch-del-queue", "Action=SendMessage&MessageBody=msg2")

	// Receive both messages
	recvRec := postQuery(h, "/000000000000/batch-del-queue", "Action=ReceiveMessage&MaxNumberOfMessages=2")
	require.Equal(t, http.StatusOK, recvRec.Code)
	recvBody := recvRec.Body.String()

	// Extract receipt handles
	handles := extractAllReceiptHandles(recvBody)
	require.Len(t, handles, 2)

	rec := postQuery(h, "/000000000000/batch-del-queue",
		"Action=DeleteMessageBatch"+
			"&DeleteMessageBatchRequestEntry.1.Id=del1"+
			"&DeleteMessageBatchRequestEntry.1.ReceiptHandle="+handles[0]+
			"&DeleteMessageBatchRequestEntry.2.Id=del2"+
			"&DeleteMessageBatchRequestEntry.2.ReceiptHandle="+handles[1])

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<DeleteMessageBatchResponse>")
	assert.Contains(t, body, "<DeleteMessageBatchResult>")
	assert.Contains(t, body, "<DeleteMessageBatchResultEntry>")
	assert.Contains(t, body, "<Id>del1</Id>")
	assert.Contains(t, body, "<Id>del2</Id>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_DeleteMessageBatch_PartialFailure_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=batch-del-partial-queue")
	postQuery(h, "/000000000000/batch-del-partial-queue", "Action=SendMessage&MessageBody=msg1")

	recvRec := postQuery(h, "/000000000000/batch-del-partial-queue", "Action=ReceiveMessage&MaxNumberOfMessages=1")
	require.Equal(t, http.StatusOK, recvRec.Code)
	handles := extractAllReceiptHandles(recvRec.Body.String())
	require.Len(t, handles, 1)

	rec := postQuery(h, "/000000000000/batch-del-partial-queue",
		"Action=DeleteMessageBatch"+
			"&DeleteMessageBatchRequestEntry.1.Id=good"+
			"&DeleteMessageBatchRequestEntry.1.ReceiptHandle="+handles[0]+
			"&DeleteMessageBatchRequestEntry.2.Id=bad"+
			"&DeleteMessageBatchRequestEntry.2.ReceiptHandle=bogus-handle")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<DeleteMessageBatchResponse>")
	assert.Contains(t, body, "<DeleteMessageBatchResultEntry>")
	assert.Contains(t, body, "<Id>good</Id>")
	assert.Contains(t, body, "<BatchResultErrorEntry>")
	assert.Contains(t, body, "<Id>bad</Id>")
	assert.Contains(t, body, "<Code>ReceiptHandleIsInvalid</Code>")
	assert.Contains(t, body, "<SenderFault>true</SenderFault>")
}

// --- Golden response shape: ChangeMessageVisibility ---

func TestXML_ChangeMessageVisibility_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=cmv-queue")
	postQuery(h, "/000000000000/cmv-queue", "Action=SendMessage&MessageBody=cmv+body")

	// Receive to get a handle
	recvRec := postQuery(h, "/000000000000/cmv-queue", "Action=ReceiveMessage&MaxNumberOfMessages=1")
	require.Equal(t, http.StatusOK, recvRec.Code)

	handles := extractAllReceiptHandles(recvRec.Body.String())
	require.Len(t, handles, 1)

	rec := postQuery(h, "/000000000000/cmv-queue", "Action=ChangeMessageVisibility&ReceiptHandle="+handles[0]+"&VisibilityTimeout=60")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ChangeMessageVisibilityResponse>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_ChangeMessageVisibility_InvalidHandle_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=cmv-err-queue")

	rec := postQuery(h, "/000000000000/cmv-err-queue", "Action=ChangeMessageVisibility&ReceiptHandle=bogus&VisibilityTimeout=30")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>ReceiptHandleIsInvalid</Code>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_ChangeMessageVisibility_MissingParams_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=cmv-missing-queue")

	rec := postQuery(h, "/000000000000/cmv-missing-queue", "Action=ChangeMessageVisibility&ReceiptHandle=some-handle")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>MissingParameter</Code>")
}

// --- Golden response shape: ChangeMessageVisibilityBatch ---

func TestXML_ChangeMessageVisibilityBatch_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=cmvb-queue")
	postQuery(h, "/000000000000/cmvb-queue", "Action=SendMessage&MessageBody=msg1")
	postQuery(h, "/000000000000/cmvb-queue", "Action=SendMessage&MessageBody=msg2")

	recvRec := postQuery(h, "/000000000000/cmvb-queue", "Action=ReceiveMessage&MaxNumberOfMessages=2")
	require.Equal(t, http.StatusOK, recvRec.Code)
	handles := extractAllReceiptHandles(recvRec.Body.String())
	require.Len(t, handles, 2)

	rec := postQuery(h, "/000000000000/cmvb-queue",
		"Action=ChangeMessageVisibilityBatch"+
			"&ChangeMessageVisibilityBatchRequestEntry.1.Id=cv1"+
			"&ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle="+handles[0]+
			"&ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=60"+
			"&ChangeMessageVisibilityBatchRequestEntry.2.Id=cv2"+
			"&ChangeMessageVisibilityBatchRequestEntry.2.ReceiptHandle="+handles[1]+
			"&ChangeMessageVisibilityBatchRequestEntry.2.VisibilityTimeout=120")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ChangeMessageVisibilityBatchResponse>")
	assert.Contains(t, body, "<ChangeMessageVisibilityBatchResult>")
	assert.Contains(t, body, "<ChangeMessageVisibilityBatchResultEntry>")
	assert.Contains(t, body, "<Id>cv1</Id>")
	assert.Contains(t, body, "<Id>cv2</Id>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_ChangeMessageVisibilityBatch_PartialFailure_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=cmvb-partial-queue")
	postQuery(h, "/000000000000/cmvb-partial-queue", "Action=SendMessage&MessageBody=msg1")

	recvRec := postQuery(h, "/000000000000/cmvb-partial-queue", "Action=ReceiveMessage&MaxNumberOfMessages=1")
	require.Equal(t, http.StatusOK, recvRec.Code)
	handles := extractAllReceiptHandles(recvRec.Body.String())
	require.Len(t, handles, 1)

	rec := postQuery(h, "/000000000000/cmvb-partial-queue",
		"Action=ChangeMessageVisibilityBatch"+
			"&ChangeMessageVisibilityBatchRequestEntry.1.Id=good"+
			"&ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle="+handles[0]+
			"&ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=60"+
			"&ChangeMessageVisibilityBatchRequestEntry.2.Id=bad"+
			"&ChangeMessageVisibilityBatchRequestEntry.2.ReceiptHandle=bogus-handle"+
			"&ChangeMessageVisibilityBatchRequestEntry.2.VisibilityTimeout=60")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ChangeMessageVisibilityBatchResponse>")
	assert.Contains(t, body, "<ChangeMessageVisibilityBatchResultEntry>")
	assert.Contains(t, body, "<Id>good</Id>")
	assert.Contains(t, body, "<BatchResultErrorEntry>")
	assert.Contains(t, body, "<Id>bad</Id>")
	assert.Contains(t, body, "<Code>ReceiptHandleIsInvalid</Code>")
	assert.Contains(t, body, "<SenderFault>true</SenderFault>")
}

func extractAllReceiptHandles(xmlBody string) []string {
	var handles []string
	remaining := xmlBody
	for {
		start := strings.Index(remaining, "<ReceiptHandle>")
		if start < 0 {
			break
		}
		start += len("<ReceiptHandle>")
		end := strings.Index(remaining[start:], "</ReceiptHandle>")
		if end < 0 {
			break
		}
		handles = append(handles, remaining[start:start+end])
		remaining = remaining[start+end:]
	}
	return handles
}

// --- Golden response shape: ListQueues ---

func TestXML_ListQueues_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=list-queue-a")
	postQuery(h, "/", "Action=CreateQueue&QueueName=list-queue-b")

	rec := postQuery(h, "/", "Action=ListQueues")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ListQueuesResponse>")
	assert.Contains(t, body, "<ListQueuesResult>")
	assert.Contains(t, body, "<QueueUrl>")
	assert.Contains(t, body, "list-queue-a")
	assert.Contains(t, body, "list-queue-b")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_ListQueues_Empty_Shape(t *testing.T) {
	h := newTestHandler()

	rec := postQuery(h, "/", "Action=ListQueues")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ListQueuesResponse>")
	assert.Contains(t, body, "<ListQueuesResult>")
	assert.NotContains(t, body, "<QueueUrl>")
	assert.Contains(t, body, "<ResponseMetadata>")
}

func TestXML_ListQueues_WithPrefix_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=prefix-match")
	postQuery(h, "/", "Action=CreateQueue&QueueName=prefix-other")
	postQuery(h, "/", "Action=CreateQueue&QueueName=nomatch")

	rec := postQuery(h, "/", "Action=ListQueues&QueueNamePrefix=prefix-")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "prefix-match")
	assert.Contains(t, body, "prefix-other")
	assert.NotContains(t, body, "nomatch")
}

// --- Golden response shape: DeleteQueue ---

func TestXML_DeleteQueue_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=del-q")

	rec := postQuery(h, "/000000000000/del-q", "Action=DeleteQueue")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<DeleteQueueResponse>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")

	// Verify the queue is actually gone.
	rec2 := postQuery(h, "/", "Action=GetQueueUrl&QueueName=del-q")
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "NonExistentQueue")
}

func TestXML_DeleteQueue_NonExistent_Shape(t *testing.T) {
	h := newTestHandler()

	rec := postQuery(h, "/000000000000/nonexistent-q", "Action=DeleteQueue")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>AWS.SimpleQueueService.NonExistentQueue</Code>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_DeleteQueue_RemovedFromListQueues(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=keep-q")
	postQuery(h, "/", "Action=CreateQueue&QueueName=remove-q")

	postQuery(h, "/000000000000/remove-q", "Action=DeleteQueue")

	rec := postQuery(h, "/", "Action=ListQueues")
	body := rec.Body.String()
	assert.Contains(t, body, "keep-q")
	assert.NotContains(t, body, "remove-q")
}

// --- Golden response shape: SendMessage with MessageAttributes ---

func TestXML_SendMessage_WithAttributes_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=attr-send-queue")

	rec := postQuery(h, "/000000000000/attr-send-queue",
		"Action=SendMessage&MessageBody=test"+
			"&MessageAttribute.1.Name=Color"+
			"&MessageAttribute.1.Value.DataType=String"+
			"&MessageAttribute.1.Value.StringValue=blue")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<SendMessageResponse>")
	assert.Contains(t, body, "<MD5OfMessageBody>")
	assert.Contains(t, body, "<MD5OfMessageAttributes>")
	assert.Contains(t, body, "<MessageId>")
}

// --- Golden response shape: ReceiveMessage with MessageAttributes ---

func TestXML_ReceiveMessage_WithAttributes_Shape(t *testing.T) {
	h := newTestHandler()
	postQuery(h, "/", "Action=CreateQueue&QueueName=attr-recv-queue")
	postQuery(h, "/000000000000/attr-recv-queue",
		"Action=SendMessage&MessageBody=test"+
			"&MessageAttribute.1.Name=Color"+
			"&MessageAttribute.1.Value.DataType=String"+
			"&MessageAttribute.1.Value.StringValue=red")

	rec := postQuery(h, "/000000000000/attr-recv-queue",
		"Action=ReceiveMessage&MaxNumberOfMessages=1"+
			"&MessageAttributeName.1=All")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ReceiveMessageResponse>")
	assert.Contains(t, body, "<MD5OfMessageAttributes>")
	assert.Contains(t, body, "<MessageAttribute>")
	assert.Contains(t, body, "<Name>Color</Name>")
	assert.Contains(t, body, "<DataType>String</DataType>")
	assert.Contains(t, body, "<StringValue>red</StringValue>")
}

// --- JSON protocol tests ---

func postJSON(handler *sqs.Handler, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", target)
	rec := httptest.NewRecorder()
	handler.HandleRequest(rec, req, "")
	return rec
}

func postJSONQueue(handler *sqs.Handler, target, queueName, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/000000000000/"+queueName, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", target)
	rec := httptest.NewRecorder()
	handler.HandleRequest(rec, req, queueName)
	return rec
}

// mustCreateQueueJSON creates a queue via the JSON protocol and fails the test if it doesn't succeed.
func mustCreateQueueJSON(t *testing.T, handler *sqs.Handler, body string) {
	t.Helper()
	rec := postJSON(handler, "AmazonSQS.CreateQueue", body)
	require.Equal(t, http.StatusOK, rec.Code)
}

// mustSendMessageJSON sends a message via the JSON protocol and fails the test if it doesn't succeed.
func mustSendMessageJSON(t *testing.T, handler *sqs.Handler, queueName, body string) {
	t.Helper()
	rec := postJSONQueue(handler, "AmazonSQS.SendMessage", queueName, body)
	require.Equal(t, http.StatusOK, rec.Code)
}

// jsonBody safely marshals a map to a JSON string for use as a request body,
// avoiding string concatenation that could produce malformed JSON.
func jsonBody(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestJSON_CreateQueue(t *testing.T) {
	h := newTestHandler()
	rec := postJSON(h, "AmazonSQS.CreateQueue", `{"QueueName":"json-queue"}`)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))

	body := rec.Body.String()
	assert.Contains(t, body, "QueueUrl")
	assert.Contains(t, body, "json-queue")
}

func TestJSON_CreateQueue_MissingName(t *testing.T) {
	h := newTestHandler()
	rec := postJSON(h, "AmazonSQS.CreateQueue", `{}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "__type")
	assert.Contains(t, body, "InvalidParameterValue")
}

func TestJSON_GetQueueUrl(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-url-queue"}`)

	rec := postJSON(h, "AmazonSQS.GetQueueUrl", `{"QueueName":"json-url-queue"}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "QueueUrl")
	assert.Contains(t, body, "json-url-queue")
}

func TestJSON_GetQueueUrl_NotFound(t *testing.T) {
	h := newTestHandler()
	rec := postJSON(h, "AmazonSQS.GetQueueUrl", `{"QueueName":"nonexistent"}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "NonExistentQueue")
}

func TestJSON_SendMessage(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-send-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SendMessage", "json-send-queue",
		`{"QueueUrl":"http://localhost:4566/000000000000/json-send-queue","MessageBody":"hello json"}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "MessageId")
	assert.Contains(t, body, "MD5OfMessageBody")
}

func TestJSON_SendMessage_WithAttributes(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-send-attr-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SendMessage", "json-send-attr-queue",
		`{"MessageBody":"test","MessageAttributes":{"Color":{"DataType":"String","StringValue":"blue"}}}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "MessageId")
	assert.Contains(t, body, "MD5OfMessageBody")
	assert.Contains(t, body, "MD5OfMessageAttributes")
}

func TestJSON_ReceiveMessage(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-recv-queue"}`)
	mustSendMessageJSON(t, h, "json-recv-queue",
		`{"MessageBody":"json msg"}`)

	rec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-recv-queue",
		`{"MaxNumberOfMessages":1}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Messages")
	assert.Contains(t, body, "MessageId")
	assert.Contains(t, body, "ReceiptHandle")
	assert.Contains(t, body, "MD5OfBody")
	assert.Contains(t, body, "Body")
	assert.Contains(t, body, "json msg")
	assert.Contains(t, body, "ApproximateReceiveCount")
	assert.Contains(t, body, "SentTimestamp")
}

func TestJSON_ReceiveMessage_Empty(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-recv-empty-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-recv-empty-queue", `{}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Messages")
}

func TestJSON_ReceiveMessage_WithAttributes(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-recv-attr-queue"}`)
	mustSendMessageJSON(t, h, "json-recv-attr-queue",
		`{"MessageBody":"test","MessageAttributes":{"Color":{"DataType":"String","StringValue":"red"}}}`)

	rec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-recv-attr-queue",
		`{"MaxNumberOfMessages":1,"MessageAttributeNames":["All"]}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "MessageAttributes")
	assert.Contains(t, body, "Color")
	assert.Contains(t, body, "String")
	assert.Contains(t, body, "red")
}

func TestJSON_DeleteMessage(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-del-msg-queue"}`)
	mustSendMessageJSON(t, h, "json-del-msg-queue",
		`{"MessageBody":"to delete"}`)

	recvRec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-del-msg-queue",
		`{"MaxNumberOfMessages":1}`)
	require.Equal(t, http.StatusOK, recvRec.Code)

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	err := json.Unmarshal(recvRec.Body.Bytes(), &recvResp)
	require.NoError(t, err)
	require.Len(t, recvResp.Messages, 1)

	rec := postJSONQueue(h, "AmazonSQS.DeleteMessage", "json-del-msg-queue",
		jsonBody(t, map[string]string{"ReceiptHandle": recvResp.Messages[0].ReceiptHandle}))

	require.Equal(t, http.StatusOK, rec.Code)
}

func TestJSON_DeleteMessage_InvalidHandle(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-del-bad-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.DeleteMessage", "json-del-bad-queue",
		`{"ReceiptHandle":"bogus-handle"}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ReceiptHandleIsInvalid")
}

func TestJSON_SendMessageBatch(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-batch-send-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SendMessageBatch", "json-batch-send-queue",
		`{"Entries":[{"Id":"m1","MessageBody":"hello one"},{"Id":"m2","MessageBody":"hello two"}]}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Successful")
	assert.Contains(t, body, "m1")
	assert.Contains(t, body, "m2")
	assert.Contains(t, body, "MessageId")
	assert.Contains(t, body, "MD5OfMessageBody")
}

func TestJSON_SendMessageBatch_Empty(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-batch-send-empty-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SendMessageBatch", "json-batch-send-empty-queue",
		`{"Entries":[]}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "EmptyBatchRequest")
}

func TestJSON_DeleteMessageBatch(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-batch-del-queue"}`)
	mustSendMessageJSON(t, h, "json-batch-del-queue", `{"MessageBody":"msg1"}`)
	mustSendMessageJSON(t, h, "json-batch-del-queue", `{"MessageBody":"msg2"}`)

	recvRec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-batch-del-queue",
		`{"MaxNumberOfMessages":2}`)
	require.Equal(t, http.StatusOK, recvRec.Code)

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	err := json.Unmarshal(recvRec.Body.Bytes(), &recvResp)
	require.NoError(t, err)
	require.Len(t, recvResp.Messages, 2)

	rec := postJSONQueue(h, "AmazonSQS.DeleteMessageBatch", "json-batch-del-queue",
		jsonBody(t, map[string]any{"Entries": []map[string]string{
			{"Id": "d1", "ReceiptHandle": recvResp.Messages[0].ReceiptHandle},
			{"Id": "d2", "ReceiptHandle": recvResp.Messages[1].ReceiptHandle},
		}}))

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Successful")
	assert.Contains(t, body, "d1")
	assert.Contains(t, body, "d2")
}

func TestJSON_DeleteMessageBatch_PartialFailure(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-batch-del-partial-queue"}`)
	mustSendMessageJSON(t, h, "json-batch-del-partial-queue", `{"MessageBody":"msg1"}`)

	recvRec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-batch-del-partial-queue",
		`{"MaxNumberOfMessages":1}`)
	require.Equal(t, http.StatusOK, recvRec.Code)

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	err := json.Unmarshal(recvRec.Body.Bytes(), &recvResp)
	require.NoError(t, err)
	require.Len(t, recvResp.Messages, 1)

	rec := postJSONQueue(h, "AmazonSQS.DeleteMessageBatch", "json-batch-del-partial-queue",
		jsonBody(t, map[string]any{"Entries": []map[string]string{
			{"Id": "good", "ReceiptHandle": recvResp.Messages[0].ReceiptHandle},
			{"Id": "bad", "ReceiptHandle": "bogus-handle"},
		}}))

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Successful")
	assert.Contains(t, body, `"Id":"good"`)
	assert.Contains(t, body, "Failed")
	assert.Contains(t, body, `"Id":"bad"`)
	assert.Contains(t, body, "ReceiptHandleIsInvalid")
}

func TestJSON_PurgeQueue(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-purge-queue"}`)
	mustSendMessageJSON(t, h, "json-purge-queue", `{"MessageBody":"to purge"}`)

	rec := postJSONQueue(h, "AmazonSQS.PurgeQueue", "json-purge-queue", `{}`)

	require.Equal(t, http.StatusOK, rec.Code)
}

func TestJSON_PurgeQueue_NonExistent(t *testing.T) {
	h := newTestHandler()

	rec := postJSONQueue(h, "AmazonSQS.PurgeQueue", "nonexistent", `{}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "NonExistentQueue")
}

func TestJSON_ChangeMessageVisibility(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-cmv-queue"}`)
	mustSendMessageJSON(t, h, "json-cmv-queue", `{"MessageBody":"cmv body"}`)

	recvRec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-cmv-queue",
		`{"MaxNumberOfMessages":1}`)
	require.Equal(t, http.StatusOK, recvRec.Code)

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	err := json.Unmarshal(recvRec.Body.Bytes(), &recvResp)
	require.NoError(t, err)
	require.Len(t, recvResp.Messages, 1)

	rec := postJSONQueue(h, "AmazonSQS.ChangeMessageVisibility", "json-cmv-queue",
		jsonBody(t, map[string]any{"ReceiptHandle": recvResp.Messages[0].ReceiptHandle, "VisibilityTimeout": 60}))

	require.Equal(t, http.StatusOK, rec.Code)
}

func TestJSON_ChangeMessageVisibility_InvalidHandle(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-cmv-err-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.ChangeMessageVisibility", "json-cmv-err-queue",
		`{"ReceiptHandle":"bogus","VisibilityTimeout":30}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ReceiptHandleIsInvalid")
}

func TestJSON_ChangeMessageVisibilityBatch(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-cmvb-queue"}`)
	mustSendMessageJSON(t, h, "json-cmvb-queue", `{"MessageBody":"msg1"}`)
	mustSendMessageJSON(t, h, "json-cmvb-queue", `{"MessageBody":"msg2"}`)

	recvRec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-cmvb-queue",
		`{"MaxNumberOfMessages":2}`)
	require.Equal(t, http.StatusOK, recvRec.Code)

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	err := json.Unmarshal(recvRec.Body.Bytes(), &recvResp)
	require.NoError(t, err)
	require.Len(t, recvResp.Messages, 2)

	rec := postJSONQueue(h, "AmazonSQS.ChangeMessageVisibilityBatch", "json-cmvb-queue",
		jsonBody(t, map[string]any{"Entries": []map[string]any{
			{"Id": "cv1", "ReceiptHandle": recvResp.Messages[0].ReceiptHandle, "VisibilityTimeout": 60},
			{"Id": "cv2", "ReceiptHandle": recvResp.Messages[1].ReceiptHandle, "VisibilityTimeout": 120},
		}}))

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Successful")
	assert.Contains(t, body, "cv1")
	assert.Contains(t, body, "cv2")
}

func TestJSON_ChangeMessageVisibilityBatch_PartialFailure(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-cmvb-partial-queue"}`)
	mustSendMessageJSON(t, h, "json-cmvb-partial-queue", `{"MessageBody":"msg1"}`)

	recvRec := postJSONQueue(h, "AmazonSQS.ReceiveMessage", "json-cmvb-partial-queue",
		`{"MaxNumberOfMessages":1}`)
	require.Equal(t, http.StatusOK, recvRec.Code)

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	err := json.Unmarshal(recvRec.Body.Bytes(), &recvResp)
	require.NoError(t, err)
	require.Len(t, recvResp.Messages, 1)

	rec := postJSONQueue(h, "AmazonSQS.ChangeMessageVisibilityBatch", "json-cmvb-partial-queue",
		jsonBody(t, map[string]any{"Entries": []map[string]any{
			{"Id": "good", "ReceiptHandle": recvResp.Messages[0].ReceiptHandle, "VisibilityTimeout": 60},
			{"Id": "bad", "ReceiptHandle": "bogus", "VisibilityTimeout": 60},
		}}))

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Successful")
	assert.Contains(t, body, `"Id":"good"`)
	assert.Contains(t, body, "Failed")
	assert.Contains(t, body, `"Id":"bad"`)
	assert.Contains(t, body, "ReceiptHandleIsInvalid")
}

func TestJSON_GetQueueAttributes(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-gqa-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.GetQueueAttributes", "json-gqa-queue",
		`{"AttributeNames":["All"]}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Attributes")
	assert.Contains(t, body, "VisibilityTimeout")
}

func TestJSON_GetQueueAttributes_Specific(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-gqa-spec-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.GetQueueAttributes", "json-gqa-spec-queue",
		`{"AttributeNames":["VisibilityTimeout"]}`)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Attributes map[string]string `json:"Attributes"`
	}
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "30", resp.Attributes["VisibilityTimeout"])
}

func TestJSON_SetQueueAttributes(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-sqa-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SetQueueAttributes", "json-sqa-queue",
		`{"Attributes":{"VisibilityTimeout":"60"}}`)

	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the attribute was set.
	getRec := postJSONQueue(h, "AmazonSQS.GetQueueAttributes", "json-sqa-queue",
		`{"AttributeNames":["VisibilityTimeout"]}`)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp struct {
		Attributes map[string]string `json:"Attributes"`
	}
	err := json.Unmarshal(getRec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "60", resp.Attributes["VisibilityTimeout"])
}

func TestJSON_SetQueueAttributes_MissingAttrs(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-sqa-err-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SetQueueAttributes", "json-sqa-err-queue", `{}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "MissingParameter")
}

func TestJSON_ListQueues(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-list-a"}`)
	mustCreateQueueJSON(t, h, `{"QueueName":"json-list-b"}`)

	rec := postJSON(h, "AmazonSQS.ListQueues", `{}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "QueueUrls")
	assert.Contains(t, body, "json-list-a")
	assert.Contains(t, body, "json-list-b")
}

func TestJSON_ListQueues_WithPrefix(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-pfx-match"}`)
	mustCreateQueueJSON(t, h, `{"QueueName":"json-pfx-other"}`)
	mustCreateQueueJSON(t, h, `{"QueueName":"json-no-match"}`)

	rec := postJSON(h, "AmazonSQS.ListQueues", `{"QueueNamePrefix":"json-pfx-"}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "json-pfx-match")
	assert.Contains(t, body, "json-pfx-other")
	assert.NotContains(t, body, "json-no-match")
}

func TestJSON_DeleteQueue(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-del-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.DeleteQueue", "json-del-queue", `{}`)

	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the queue is gone.
	getRec := postJSON(h, "AmazonSQS.GetQueueUrl", `{"QueueName":"json-del-queue"}`)
	assert.Equal(t, http.StatusBadRequest, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "NonExistentQueue")
}

func TestJSON_DeleteQueue_NonExistent(t *testing.T) {
	h := newTestHandler()

	rec := postJSONQueue(h, "AmazonSQS.DeleteQueue", "nonexistent-q", `{}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "NonExistentQueue")
}

func TestJSON_Error_InvalidAction(t *testing.T) {
	h := newTestHandler()
	rec := postJSON(h, "AmazonSQS.BogusAction", `{}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "InvalidAction")
	assert.Contains(t, body, "__type")
}

func TestJSON_Error_HeaderFormat(t *testing.T) {
	h := newTestHandler()
	rec := postJSON(h, "AmazonSQS.GetQueueUrl", `{"QueueName":"nonexistent"}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Header().Get("x-amzn-query-error"), "Sender")
}

func TestJSON_SendMessage_WithDelaySeconds(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-delay-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SendMessage", "json-delay-queue",
		`{"MessageBody":"delayed body","DelaySeconds":10}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "MessageId")
	assert.Contains(t, body, "MD5OfMessageBody")
}

func TestJSON_SendMessage_InvalidDelaySeconds(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-bad-delay-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.SendMessage", "json-bad-delay-queue",
		`{"MessageBody":"body","DelaySeconds":1000}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "InvalidParameterValue")
}

func TestJSON_ChangeMessageVisibility_MissingParams(t *testing.T) {
	h := newTestHandler()
	mustCreateQueueJSON(t, h, `{"QueueName":"json-cmv-missing-queue"}`)

	rec := postJSONQueue(h, "AmazonSQS.ChangeMessageVisibility", "json-cmv-missing-queue",
		`{"ReceiptHandle":"some-handle"}`)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "MissingParameter")
}
