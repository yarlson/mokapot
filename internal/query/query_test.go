package query_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/devstack/internal/query"
)

func TestParseRequest(t *testing.T) {
	body := "Action=CreateQueue&QueueName=test-queue"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	params, err := query.ParseRequest(req)
	require.NoError(t, err)

	assert.Equal(t, "CreateQueue", params.Action())
	assert.Equal(t, "test-queue", params.Get("QueueName"))
}

func TestWriteXML(t *testing.T) {
	type testResp struct {
		XMLName xml.Name `xml:"TestResponse"`
		Value   string   `xml:"Value"`
	}

	rec := httptest.NewRecorder()
	query.WriteXML(rec, http.StatusOK, testResp{Value: "hello"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/xml; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "<TestResponse>")
	assert.Contains(t, rec.Body.String(), "<Value>hello</Value>")
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	query.WriteError(rec, http.StatusBadRequest, "TestCode", "Test message")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ErrorResponse>")
	assert.Contains(t, body, "<Code>TestCode</Code>")
	assert.Contains(t, body, "<Message>Test message</Message>")
	assert.Contains(t, body, "<Type>Sender</Type>")
	assert.Contains(t, body, "<RequestId>")
}

func TestNewRequestID(t *testing.T) {
	id1 := query.NewRequestID()
	id2 := query.NewRequestID()
	assert.NotEmpty(t, id1)
	assert.NotEqual(t, id1, id2)
}
