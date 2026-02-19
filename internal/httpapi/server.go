package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/yarlson/mokapot/internal/sns"
	"github.com/yarlson/mokapot/internal/sqs"
)

// NewServer creates an HTTP handler with health, SQS routing, SNS routing, and readiness endpoints.
func NewServer(sqsHandler *sqs.Handler, snsHandler *sns.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_health", handleHealth)
	mux.HandleFunc("POST /", handleRoot(sqsHandler, snsHandler))
	// accountId is captured but intentionally not validated — this is a local
	// emulator and any account ID is accepted, matching LocalStack behavior.
	mux.HandleFunc("POST /{accountId}/{queueName}", handleQueueScoped(sqsHandler))
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		slog.Error("failed to write health response", "error", err)
	}
}

func handleRoot(sqsHandler *sqs.Handler, snsHandler *sns.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slog.Debug("request received", "path", r.URL.Path, "content-type", r.Header.Get("Content-Type"))

		if isSNSRequest(r) {
			snsHandler.HandleRequest(w, r)
		} else {
			sqsHandler.HandleRequest(w, r, "")
		}
	}
}

func handleQueueScoped(sqsHandler *sqs.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queueName")
		slog.Debug("request received", "path", r.URL.Path, "queue", queueName, "content-type", r.Header.Get("Content-Type"))
		sqsHandler.HandleRequest(w, r, queueName)
	}
}

// isSNSRequest determines whether a request should be routed to the SNS handler.
func isSNSRequest(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/x-amz-json") {
		target := r.Header.Get("X-Amz-Target")
		return strings.HasPrefix(target, "SNS.") ||
			strings.HasPrefix(target, "AmazonSimpleNotificationService.")
	}

	// Query protocol — parse form to inspect the Action parameter.
	// ParseForm is idempotent; the handler's subsequent call will be a no-op.
	if err := r.ParseForm(); err != nil {
		return false
	}
	return sns.IsSNSAction(r.Form.Get("Action"))
}
