package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/yarlson/devstack/internal/sqs"
)

// NewServer creates an HTTP handler with health, SQS routing, and readiness endpoints.
func NewServer(sqsHandler *sqs.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_health", handleHealth)
	mux.HandleFunc("POST /", handleRoot(sqsHandler))
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

func handleRoot(sqsHandler *sqs.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slog.Debug("request received", "path", r.URL.Path, "content-type", r.Header.Get("Content-Type"))
		sqsHandler.HandleRequest(w, r, "")
	}
}

func handleQueueScoped(sqsHandler *sqs.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queueName")
		slog.Debug("request received", "path", r.URL.Path, "queue", queueName, "content-type", r.Header.Get("Content-Type"))
		sqsHandler.HandleRequest(w, r, queueName)
	}
}
