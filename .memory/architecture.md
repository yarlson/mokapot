# Architecture

## Request Flow

```
AWS SDK (Node.js / PHP / Go)
  │
  ▼
HTTP POST to http://messaging:4566/
  or http://messaging:4566/{accountId}/{queueName}
  │
  ▼
mokapot (cmd/mokapot/main.go)
  │ — configures slog JSON logger
  │ — opens BoltStore if PERSISTENCE=bbolt (restores state on startup)
  │ — starts http.Server with timeouts
  │ — periodic state save (30s) + final save on shutdown
  │ — graceful shutdown on SIGINT/SIGTERM
  │
  ▼
httpapi.NewServer(sqsHandler, snsHandler) (internal/httpapi/server.go)
  │ — http.ServeMux routing
  │ — GET /_health
  │ — POST / → handleRoot → isSNSRequest() → snsHandler or sqsHandler
  │ — POST /{accountId}/{queueName} → handleQueueScoped → sqsHandler.HandleRequest
  │
  ▼
Query Parser (internal/query/query.go)
  │ — ParseRequest: decodes form-encoded body → url.Values
  │ — Params: .Action(), .Get(key) accessors
  │ — WriteXML / WriteError: AWS-style XML responses
  │
  ├─▶ SQS Handler (internal/sqs/handler.go)
  │     │ — detects JSON vs Query protocol from Content-Type
  │     │ — dispatches by Action to engine methods
  │     │ — returns XML or JSON responses
  │     ▼
  │   SQS Engine (internal/sqs/engine.go)
  │     │ — in-memory queue store (map[string]*Queue + queuesByARN index)
  │     │ — per-queue: available []*Message, inflight map, waiter list
  │     │ — visibility timeout with lazy reappearance on ReceiveMessage
  │     │ — long polling: waiter channels notified on SendMessage; timer-based wakeup for visibility expiry and delayed message availability
  │     │ — delayed messages: per-message or queue-level DelaySeconds; filtered in receiveFromQueue
  │     │ — dead-letter queue: RedrivePolicy on source queue; messages exceeding maxReceiveCount moved to DLQ during receive
  │     │ — message attributes: typed key-value metadata (String/Number/Binary); MD5 digest per AWS canonical encoding; attribute filtering on ReceiveMessage; attributes survive DLQ moves and snapshot/restore
  │     │ — batch operations: SendMessageBatch/DeleteMessageBatch with partial failure, per-entry error isolation
  │     │ — PurgeQueue: clears all available and inflight messages; 60-second cooldown (PurgeQueueInProgress)
  │     │ — ChangeMessageVisibility: update visibility timeout of inflight message; 0 releases immediately and wakes long pollers
  │     │ — ChangeMessageVisibilityBatch: batch variant (up to 10) with partial failure
  │     │ — ListQueues with optional prefix filter, DeleteQueue
  │     │ — Get/SetQueueAttributes: mutable attribute whitelist, numeric range validation, RedrivePolicy DLQ existence check
  │     │ — receipt handle regeneration + ReceiveCount tracking
  │     │ — injectable clock (now func() time.Time)
  │     │ — context-aware ReceiveMessage (cancellable long polls)
  │     │ — thread-safe via sync.RWMutex + per-queue sync.Mutex
  │
  ├─▶ BoltStore (internal/store/store.go)
  │     │ — bbolt-backed persistence (DATA_DIR/state.db)
  │     │ — SaveSQSState / LoadSQSState: serialize SQS engine snapshot to "sqs" bucket
  │     │ — SaveSNSState / LoadSNSState: serialize SNS engine snapshot to "sns" bucket
  │     │ — JSON encoding of snapshot structs
  │
  └─▶ SNS Handler (internal/sns/handler.go)
        │ — detects JSON vs Query protocol from Content-Type
        │ — dispatches by Action (CreateTopic, Subscribe, Publish, Set/GetSubscriptionAttributes, ListTopics, DeleteTopic, ListSubscriptionsByTopic, Unsubscribe, Get/SetTopicAttributes)
        │ — parses MessageAttributes from both JSON and Query protocols
        │ — returns XML or JSON responses
        ▼
      SNS Engine (internal/sns/engine.go)
        │ — in-memory topic store (map[string]*Topic)
        │ — per-topic: Subscriptions slice, per-topic mutex
        │ — global subscriptionsByARN index for direct subscription lookup
        │ — CreateTopic (idempotent), Subscribe (sqs only), Publish
        │ — ListTopics, DeleteTopic (cascading subscription cleanup)
        │ — ListSubscriptionsByTopic, Unsubscribe
        │ — Get/SetSubscriptionAttributes with per-subscription mutex
        │ — Get/SetTopicAttributes
        │ — RawMessageDelivery: per-subscription toggle; Publish checks attribute and delivers raw body or SNS envelope
        │ — FilterPolicy: parsed and cached on SetSubscriptionAttributes; evaluated during Publish to skip non-matching subscribers
        │ — Publish builds SNS envelope (includes MessageAttributes), fans out via EnqueueFunc callback
        │ — injectable clock (now func() time.Time)
        │ — thread-safe via sync.RWMutex + per-topic sync.Mutex + per-subscription sync.RWMutex
        ▼
      Filter Policy Engine (internal/sns/filter.go)
        │ — parses FilterPolicy JSON into typed condition tree
        │ — supports: exact string, exact numeric, prefix, exists, anything-but, numeric range
        │ — conditions within a key OR'd; keys AND'd
        ▼
      SQS queue (via EnqueueFunc — no direct sqs import)
```

## Implementation Slices

14 vertical slices defined in `slices/SLICES.md`. Dependency graph:

```
1  Service boots ✓
│
2  Send + receive + delete ✓  ← foundation
├── 3  Visibility timeout ✓ (reappearance + receipt handle invalidation; no ChangeMessageVisibility action yet)
│   ├── 4  Long polling ✓
│   ├── 6  Dead-letter queue ✓
│   └── 12 Change visibility ✓
├── 5  Delayed messages ✓
├── 7  Batch operations ✓ (SendMessageBatch, DeleteMessageBatch)
├── 8  Purge queue ✓
├── 9  SNS fanout (envelope) ✓
│   ├── 10 Raw delivery ✓
│   └── 11 Filter policies ✓
├── 13 Persistence (bbolt) ✓
└── 14 Housekeeping (CRUD lists) ✓
```

## Key Design Decisions

- **Single endpoint** (LocalStack-style) at port 4566; no per-service ports
- **In-memory default** with optional bbolt persistence
- **No IAM enforcement** — policies stored/returned but not evaluated
- **Only `sqs` protocol** for SNS subscriptions
- **No FIFO queues** by default (scope undefined, deferred)
- **No per-message goroutine timers** — global scheduler with deadline heap
- **Injectable clock** — `Engine.now` field (`func() time.Time`) defaults to `time.Now`; overridable via `SetClock` for deterministic tests without `time.Sleep`
- **Dual protocol in handler** — Content-Type `application/x-amz-json-1.0` triggers JSON path (Go/JS SDK v3); form-encoded triggers Query/XML path (PHP/older SDKs)

## Docker & Release

- **Dockerfile**: distroless base (`gcr.io/distroless/static-debian12:nonroot`); expects pre-built binary from GoReleaser at `$TARGETPLATFORM/mokapot`; `/data` directory for persistence; non-root user
- **docker-compose**: uses published `yarlson/mokapot:latest` image; `PERSISTENCE=bbolt`, `DATA_DIR=/data`, named volume `messaging-data`
- **GoReleaser v2** (`.goreleaser.yaml`): builds linux/darwin × amd64/arm64; publishes tar.gz archives, Docker images (`yarlson/mokapot`), and Homebrew cask (`yarlson/homebrew-tap`)
- **GitHub Actions** (`.github/workflows/release.yml`): triggers on `v*` tags; runs GoReleaser; requires secrets `GH_PAT`, `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`

## Defaults

- Region: `eu-central-1`
- Account ID: `000000000000`
- Port: `4566`
- Persistence: `memory`
- Log level: `info`
