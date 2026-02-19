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
messagingd (cmd/messagingd/main.go)
  │ — configures slog JSON logger
  │ — starts http.Server with timeouts
  │ — graceful shutdown on SIGINT/SIGTERM
  │
  ▼
httpapi.NewServer() (internal/httpapi/server.go)
  │ — http.ServeMux routing
  │ — currently: GET /_health only
  │ — planned: POST / and POST /{accountId}/{queueName}
  │
  ▼
Query Parser (planned: internal/query/)
  │ — decodes form-encoded Action param
  │ — dispatches to SQS or SNS handler
  │
  ├─▶ SQS Handlers (planned: internal/sqs/)
  │     │ — queue engine: available/inflight/delayed stores
  │     │ — visibility timeout, long polling, delay, DLQ
  │     ▼
  │   Store Interface (planned: internal/store/)
  │     ├─ memory (default)
  │     └─ bbolt (optional persistence)
  │
  └─▶ SNS Handlers (planned: internal/sns/)
        │ — topic/subscription management
        │ — publish → filter → deliver to SQS queues
        ▼
      SQS queue (internal enqueue)
```

## Implementation Slices

14 vertical slices defined in `slices/SLICES.md`. Dependency graph:

```
1  Service boots (DONE)
│
2  Send + receive + delete  ← foundation
├── 3  Visibility timeout
│   ├── 4  Long polling
│   ├── 6  Dead-letter queue
│   └── 12 Change visibility
├── 5  Delayed messages
├── 7  Batch operations
├── 8  Purge queue
├── 9  SNS fanout (envelope)
│   ├── 10 Raw delivery
│   └── 11 Filter policies
├── 13 Persistence (bbolt)
└── 14 Housekeeping (CRUD lists)
```

## Key Design Decisions

- **Single endpoint** (LocalStack-style) at port 4566; no per-service ports
- **In-memory default** with optional bbolt persistence
- **No IAM enforcement** — policies stored/returned but not evaluated
- **Only `sqs` protocol** for SNS subscriptions
- **No FIFO queues** by default (scope undefined, deferred)
- **No per-message goroutine timers** — global scheduler with deadline heap

## Docker

- Multi-stage build: `golang:1.25-alpine` → `alpine:3.21`
- Non-root user (`appuser`)
- Exposes port 4566
- Configurable via environment variables in docker-compose

## Defaults

- Region: `eu-central-1`
- Account ID: `000000000000`
- Port: `4566`
- Persistence: `memory`
- Log level: `info`
