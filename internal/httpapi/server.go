package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// NewServer creates an HTTP handler with health and readiness endpoints.
func NewServer() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_health", handleHealth)
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
		slog.Error("failed to write health response", "error", err)
	}
}
