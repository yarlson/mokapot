# devstack — Local AWS SQS/SNS Emulator

## What

A lightweight, Go-based local development emulator for AWS SQS and SNS. Replaces LocalStack for SNS/SQS only. Runs as a single container in docker-compose. Compatible with AWS SDK for JavaScript (Node.js) and AWS SDK for PHP via endpoint override + dummy credentials.

## Architecture

- **Single Go binary** (`messagingd`) serving HTTP on port 4566
- **Dual protocol**: AWS Query (form-encoded + XML) and AWS JSON 1.0 (`application/x-amz-json-1.0` + JSON)
- **SigV4 passthrough**: accepts signed requests without validating signatures
- **Routing**: `POST /` for global actions (CreateQueue, ListQueues), `POST /{accountId}/{queueName}` for queue-scoped actions
- **SNS delivery protocol**: `sqs` only (no http/email/sms/lambda)

## Core Flow

1. SDKs send AWS Query-style HTTP requests to `http://messaging:4566`
2. Server parses action from form data, routes to SQS or SNS handler
3. Handlers operate on in-memory (or bbolt-persisted) data structures
4. XML responses returned in standard AWS shape

## Capabilities

### SQS
- Queue CRUD (CreateQueue, DeleteQueue, ListQueues, GetQueueUrl)
- Queue attributes (Get/SetQueueAttributes)
- Message lifecycle: SendMessage, ReceiveMessage, DeleteMessage
- Batch operations: SendMessageBatch, DeleteMessageBatch, ChangeMessageVisibilityBatch
- Visibility timeout with automatic reappearance
- Long polling (WaitTimeSeconds)
- Delayed messages (DelaySeconds)
- Dead-letter queue with RedrivePolicy
- PurgeQueue
- ChangeMessageVisibility

### SNS
- Topic CRUD (CreateTopic, DeleteTopic, ListTopics, Get/SetTopicAttributes)
- Subscriptions (Subscribe, Unsubscribe, ListSubscriptionsByTopic, Get/SetSubscriptionAttributes)
- Publish with fanout to SQS queues
- RawMessageDelivery toggle
- FilterPolicy evaluation (exact match, allowlist, exists, anything-but, numeric comparisons)

## System State

SQS core message lifecycle is operational. SNS is not yet implemented.

**Operational:**
- `messagingd` binary boots, listens on configurable PORT (default 4566)
- `GET /_health` returns `{"status":"ok"}`
- Graceful shutdown on SIGINT/SIGTERM
- Structured JSON logging with configurable level
- Dockerfile (multi-stage, alpine) and docker-compose.yml operational
- SQS queue CRUD: CreateQueue (idempotent), GetQueueUrl, SendMessage, ReceiveMessage, DeleteMessage
- Dual protocol support: AWS Query/XML and AWS JSON 1.0 (Go/JS SDK v3)
- In-memory queue engine with per-queue mutex, visibility timeout, receipt handle tracking
- Visibility timeout with automatic reappearance: expired inflight messages return to available pool with new receipt handles and incremented ReceiveCount
- Long polling: ReceiveMessage blocks up to WaitTimeSeconds when no messages are available; wakes on message arrival, visibility timeout expiry, or context cancellation; supports per-queue default via ReceiveMessageWaitTimeSeconds attribute
- Injectable clock (`Engine.SetClock`) for deterministic time control in tests
- Integration tests using real AWS SDK Go v2 client against test server

**Not yet implemented:** ListQueues response, batch operations, delayed messages, DLQ, PurgeQueue, ChangeMessageVisibility, SNS, persistence

## Tech Stack

- **Language:** Go 1.25
- **Module:** `github.com/yarlson/devstack`
- **Testing:** `testify`, `aws-sdk-go-v2` (integration tests)
- **Linting:** `golangci-lint`
- **Logging:** `log/slog` with JSON handler
- **Container:** Alpine-based multi-stage Docker build
- **Orchestration:** docker-compose
- **IDs:** `github.com/google/uuid`
