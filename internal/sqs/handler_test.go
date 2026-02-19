package sqs_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/devstack/internal/sqs"
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
