package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/yarlson/mokapot/internal/httpapi"
	"github.com/yarlson/mokapot/internal/sns"
	"github.com/yarlson/mokapot/internal/sqs"
	"github.com/yarlson/mokapot/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	port := envOr("PORT", "4566")
	region := envOr("REGION", "eu-central-1")
	accountID := envOr("ACCOUNT_ID", "000000000000")
	hostname := envOr("SQS_HOST", "localhost")
	logLevel := os.Getenv("LOG_LEVEL")
	persistence := envOr("PERSISTENCE", "memory")
	dataDir := os.Getenv("DATA_DIR")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(logLevel),
	}))
	slog.SetDefault(logger)

	host := fmt.Sprintf("%s:%s", hostname, port)
	sqsEngine := sqs.NewEngine(region, accountID, host)
	sqsHandler := sqs.NewHandler(sqsEngine)

	enqueue := func(queueName, body string) error {
		_, err := sqsEngine.SendMessage(queueName, body, 0, nil)
		return err
	}
	snsEngine := sns.NewEngine(region, accountID, enqueue)
	snsHandler := sns.NewHandler(snsEngine)

	// Open persistent store if configured.
	var boltStore *store.BoltStore
	if persistence == "bbolt" {
		if dataDir == "" {
			return fmt.Errorf("DATA_DIR is required when PERSISTENCE=bbolt")
		}
		var err error
		boltStore, err = store.Open(filepath.Join(dataDir, "state.db"))
		if err != nil {
			return fmt.Errorf("open persistence store: %w", err)
		}
		defer boltStore.Close()

		if err := restoreState(boltStore, sqsEngine, snsEngine); err != nil {
			return fmt.Errorf("restore state: %w", err)
		}
		slog.Info("state restored from persistent store", "dataDir", dataDir)
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           httpapi.NewServer(sqsHandler, snsHandler),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Periodic save goroutine.
	var stopSave chan struct{}
	var saveWg sync.WaitGroup
	if boltStore != nil {
		stopSave = make(chan struct{})
		saveWg.Add(1)
		go func() {
			defer saveWg.Done()
			periodicSave(boltStore, sqsEngine, snsEngine, 30*time.Second, stopSave)
		}()
	}

	// Periodic retention cleanup goroutine.
	stopCleanup := make(chan struct{})
	var cleanupWg sync.WaitGroup
	cleanupWg.Add(1)
	go func() {
		defer cleanupWg.Done()
		periodicRetentionCleanup(sqsEngine, 5*time.Minute, stopCleanup)
	}()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("starting mokapot", "port", port, "region", region, "accountId", accountID, "persistence", persistence)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	case <-quit:
		slog.Info("shutting down")
	}

	close(stopCleanup)
	cleanupWg.Wait()

	if stopSave != nil {
		close(stopSave)
		saveWg.Wait()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	// Final save on shutdown.
	if boltStore != nil {
		if err := saveState(boltStore, sqsEngine, snsEngine); err != nil {
			slog.Error("failed to save state on shutdown", "error", err)
		} else {
			slog.Info("state saved to persistent store")
		}
	}

	return nil
}

func restoreState(s *store.BoltStore, sqsEngine *sqs.Engine, snsEngine *sns.Engine) error {
	sqsData, err := s.LoadSQSState()
	if err != nil {
		return fmt.Errorf("load SQS state: %w", err)
	}
	if sqsData != nil {
		if err := sqsEngine.Restore(sqsData); err != nil {
			return fmt.Errorf("restore SQS state: %w", err)
		}
	}

	snsData, err := s.LoadSNSState()
	if err != nil {
		return fmt.Errorf("load SNS state: %w", err)
	}
	if snsData != nil {
		if err := snsEngine.Restore(snsData); err != nil {
			return fmt.Errorf("restore SNS state: %w", err)
		}
	}

	return nil
}

// saveState snapshots SQS and SNS engines atomically and persists them.
// Both engine write locks are held during the snapshot to prevent
// cross-engine inconsistency (e.g. an SNS publish delivering to SQS
// between the two snapshots).
// Lock order: SNS then SQS — matches the Publish→SendMessage call flow.
func saveState(s *store.BoltStore, sqsEngine *sqs.Engine, snsEngine *sns.Engine) error {
	snsEngine.Lock()
	sqsEngine.Lock()
	sqsData, sqsErr := sqsEngine.SnapshotLocked()
	snsData, snsErr := snsEngine.SnapshotLocked()
	sqsEngine.Unlock()
	snsEngine.Unlock()

	if sqsErr != nil {
		return fmt.Errorf("snapshot SQS: %w", sqsErr)
	}
	if snsErr != nil {
		return fmt.Errorf("snapshot SNS: %w", snsErr)
	}

	if err := s.SaveSQSState(sqsData); err != nil {
		return fmt.Errorf("save SQS state: %w", err)
	}
	if err := s.SaveSNSState(snsData); err != nil {
		return fmt.Errorf("save SNS state: %w", err)
	}

	return nil
}

func periodicSave(s *store.BoltStore, sqsEngine *sqs.Engine, snsEngine *sns.Engine, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := saveState(s, sqsEngine, snsEngine); err != nil {
				slog.Warn("periodic state save failed", "error", err)
			}
		}
	}
}

func periodicRetentionCleanup(sqsEngine *sqs.Engine, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if removed := sqsEngine.CleanupExpiredMessages(); removed > 0 {
				slog.Info("retention cleanup removed expired messages", "count", removed)
			}
		}
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
