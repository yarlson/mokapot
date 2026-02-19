package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yarlson/mokapot/internal/httpapi"
	"github.com/yarlson/mokapot/internal/sns"
	"github.com/yarlson/mokapot/internal/sqs"
)

func newTestServer() http.Handler {
	sqsEngine := sqs.NewEngine("us-east-1", "000000000000", "localhost:4566")
	enqueue := func(queueName, body string) error {
		_, err := sqsEngine.SendMessage(queueName, body, 0)
		return err
	}
	snsEngine := sns.NewEngine("us-east-1", "000000000000", enqueue)
	return httpapi.NewServer(sqs.NewHandler(sqsEngine), sns.NewHandler(snsEngine))
}

func TestHealthEndpoint(t *testing.T) {
	srv := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/_health", http.NoBody)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var body map[string]string
	err := json.NewDecoder(rec.Body).Decode(&body)
	require.NoError(t, err)
	assert.Equal(t, "ok", body["status"])
}

func TestGetOnPostRouteReturns405(t *testing.T) {
	srv := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
