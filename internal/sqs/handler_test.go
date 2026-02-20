package sqs_test

import (
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
