package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yarlson/devstack/internal/httpapi"
	"github.com/yarlson/devstack/internal/sqs"
)

func main() {
	port := envOr("PORT", "4566")
	region := envOr("REGION", "eu-central-1")
	accountID := envOr("ACCOUNT_ID", "000000000000")
	hostname := envOr("SQS_HOST", "localhost")
	logLevel := os.Getenv("LOG_LEVEL")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(logLevel),
	}))
	slog.SetDefault(logger)

	host := fmt.Sprintf("%s:%s", hostname, port)
	engine := sqs.NewEngine(region, accountID, host)
	sqsHandler := sqs.NewHandler(engine)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           httpapi.NewServer(sqsHandler),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("starting messagingd", "port", port, "region", region, "accountId", accountID)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		slog.Error("server error", "error", err)
		os.Exit(1)
	case <-quit:
		slog.Info("shutting down")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
