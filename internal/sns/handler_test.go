package sns_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/sns"
)

func newTestHandler() *sns.Handler {
	rec := &enqueueRecorder{}
	engine := sns.NewEngine("eu-central-1", "000000000000", rec.enqueue)
	return sns.NewHandler(engine)
}

func postSNSQuery(handler *sns.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.HandleRequest(rec, req)
	return rec
}

// --- Golden response shape: CreateTopic ---

func TestXML_CreateTopic_Shape(t *testing.T) {
	h := newTestHandler()
	rec := postSNSQuery(h, "Action=CreateTopic&Name=golden-topic")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/xml; charset=utf-8", rec.Header().Get("Content-Type"))

	body := rec.Body.String()
	assert.Contains(t, body, "<CreateTopicResponse>")
	assert.Contains(t, body, "<CreateTopicResult>")
	assert.Contains(t, body, "<TopicArn>")
	assert.Contains(t, body, "golden-topic")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_CreateTopic_MissingName(t *testing.T) {
	h := newTestHandler()
	rec := postSNSQuery(h, "Action=CreateTopic")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>InvalidParameter</Code>")
	assert.Contains(t, body, "<RequestId>")
}

// --- Golden response shape: Subscribe ---

func TestXML_Subscribe_Shape(t *testing.T) {
	h := newTestHandler()

	// Create topic first
	postSNSQuery(h, "Action=CreateTopic&Name=sub-topic")

	rec := postSNSQuery(h, "Action=Subscribe"+
		"&TopicArn=arn:aws:sns:eu-central-1:000000000000:sub-topic"+
		"&Protocol=sqs"+
		"&Endpoint=arn:aws:sqs:eu-central-1:000000000000:my-queue")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<SubscribeResponse>")
	assert.Contains(t, body, "<SubscribeResult>")
	assert.Contains(t, body, "<SubscriptionArn>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_Subscribe_TopicNotFound(t *testing.T) {
	h := newTestHandler()

	rec := postSNSQuery(h, "Action=Subscribe"+
		"&TopicArn=arn:aws:sns:eu-central-1:000000000000:nonexistent"+
		"&Protocol=sqs"+
		"&Endpoint=arn:aws:sqs:eu-central-1:000000000000:q")

	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>NotFound</Code>")
}

// --- Golden response shape: Publish ---

func TestXML_Publish_Shape(t *testing.T) {
	h := newTestHandler()
	postSNSQuery(h, "Action=CreateTopic&Name=pub-topic")

	rec := postSNSQuery(h, "Action=Publish"+
		"&TopicArn=arn:aws:sns:eu-central-1:000000000000:pub-topic"+
		"&Message=hello+world")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<PublishResponse>")
	assert.Contains(t, body, "<PublishResult>")
	assert.Contains(t, body, "<MessageId>")
	assert.Contains(t, body, "<ResponseMetadata>")
	assert.Contains(t, body, "<RequestId>")
}

func TestXML_Publish_TopicNotFound(t *testing.T) {
	h := newTestHandler()

	rec := postSNSQuery(h, "Action=Publish"+
		"&TopicArn=arn:aws:sns:eu-central-1:000000000000:nonexistent"+
		"&Message=hello")

	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>NotFound</Code>")
}

func TestXML_Publish_MissingTopicArn(t *testing.T) {
	h := newTestHandler()

	rec := postSNSQuery(h, "Action=Publish&Message=hello")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>InvalidParameter</Code>")
}

// --- Golden response shape: Error ---

func TestXML_Error_InvalidAction_Shape(t *testing.T) {
	h := newTestHandler()
	rec := postSNSQuery(h, "Action=BogusAction")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>InvalidAction</Code>")
	assert.Contains(t, body, "<RequestId>")
}

// --- IsSNSAction tests ---

func TestIsSNSAction(t *testing.T) {
	snsActions := []string{
		"CreateTopic", "DeleteTopic", "ListTopics",
		"Subscribe", "Unsubscribe", "Publish",
		"GetTopicAttributes", "SetTopicAttributes",
		"ListSubscriptionsByTopic",
		"GetSubscriptionAttributes", "SetSubscriptionAttributes",
	}
	for _, action := range snsActions {
		assert.True(t, sns.IsSNSAction(action), "expected %s to be SNS action", action)
	}

	sqsActions := []string{
		"CreateQueue", "SendMessage", "ReceiveMessage",
		"DeleteMessage", "PurgeQueue", "GetQueueUrl",
	}
	for _, action := range sqsActions {
		assert.False(t, sns.IsSNSAction(action), "expected %s to NOT be SNS action", action)
	}
}

// --- JSON protocol tests ---

func postSNSJSON(handler *sns.Handler, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", target)
	rec := httptest.NewRecorder()
	handler.HandleRequest(rec, req)
	return rec
}

func TestJSON_CreateTopic(t *testing.T) {
	h := newTestHandler()
	rec := postSNSJSON(h, "SNS.CreateTopic", `{"Name":"json-topic"}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "TopicArn")
	assert.Contains(t, body, "json-topic")
}

func TestJSON_Subscribe(t *testing.T) {
	h := newTestHandler()
	postSNSJSON(h, "SNS.CreateTopic", `{"Name":"json-sub-topic"}`)

	rec := postSNSJSON(h, "SNS.Subscribe", `{
		"TopicArn":"arn:aws:sns:eu-central-1:000000000000:json-sub-topic",
		"Protocol":"sqs",
		"Endpoint":"arn:aws:sqs:eu-central-1:000000000000:q"
	}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "SubscriptionArn")
}

func TestJSON_Publish(t *testing.T) {
	h := newTestHandler()
	postSNSJSON(h, "SNS.CreateTopic", `{"Name":"json-pub-topic"}`)

	rec := postSNSJSON(h, "SNS.Publish", `{
		"TopicArn":"arn:aws:sns:eu-central-1:000000000000:json-pub-topic",
		"Message":"hello json"
	}`)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "MessageId")
}
