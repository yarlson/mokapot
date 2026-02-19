# mokapot — Local AWS SQS/SNS Emulator

## What

A lightweight, Go-based local development emulator for AWS SQS and SNS. Replaces LocalStack for SNS/SQS only. Runs as a single container in docker-compose. Compatible with AWS SDK for JavaScript (Node.js) and AWS SDK for PHP via endpoint override + dummy credentials.

## Architecture

- **Single Go binary** (`mokapot`) serving HTTP on port 4566
- **Dual protocol**: AWS Query (form-encoded + XML) and AWS JSON 1.0 (`application/x-amz-json-1.0` + JSON)
- **SigV4 passthrough**: accepts signed requests without validating signatures
- **Routing**: `POST /` dispatches to SQS or SNS based on Content-Type and Action/X-Amz-Target headers; `POST /{accountId}/{queueName}` for queue-scoped SQS actions
- **SNS delivery protocol**: `sqs` only (no http/email/sms/lambda)
- **SNS→SQS coupling**: SNS engine receives an `EnqueueFunc` callback; no direct import of the `sqs` package

## Core Flow

1. SDKs send AWS Query-style HTTP requests to `http://messaging:4566`
2. Server parses action from form data, routes to SQS or SNS handler
3. Handlers operate on in-memory (or bbolt-persisted) data structures
4. XML responses returned in standard AWS shape

## Capabilities

### SQS

- Queue creation: CreateQueue (idempotent), GetQueueUrl
- Queue attributes: Get/SetQueueAttributes
- Message lifecycle: SendMessage, ReceiveMessage, DeleteMessage
- Batch operations: SendMessageBatch, DeleteMessageBatch (with partial failure, duplicate ID detection, batch size validation)
- Visibility timeout with automatic reappearance
- Long polling (WaitTimeSeconds)
- Delayed messages (DelaySeconds)
- Dead-letter queue with RedrivePolicy
- PurgeQueue (60-second cooldown)

### SNS

- CreateTopic (idempotent — returns existing topic if name matches)
- Subscribe (sqs protocol only — endpoint is an SQS queue ARN)
- Publish with fanout to all SQS subscriptions; messages wrapped in standard SNS JSON envelope (Type, MessageId, TopicArn, Message, Subject, Timestamp, MessageAttributes, Signature stubs)
- MessageAttributes: Publish accepts message attributes (both JSON and Query protocols); attributes included in SNS envelope and used for filter policy evaluation
- Get/SetSubscriptionAttributes: per-subscription attribute store with RawMessageDelivery validation (must be "true"/"false") and FilterPolicy validation (must be valid JSON with supported operators)
- RawMessageDelivery: when enabled on a subscription, Publish delivers the plain message body instead of the SNS JSON envelope; per-subscription toggle, mixed raw/envelope fanout supported
- FilterPolicy: per-subscription message filtering; supports exact string match, exact numeric match, prefix, exists/not-exists, anything-but (string/number/array), and numeric range operators (=, >, >=, <, <=); conditions within a key are OR'd, keys are AND'd; parsed and cached on SetSubscriptionAttributes; empty/nil policy passes all messages
- Dual protocol: AWS Query/XML and AWS JSON 1.0 (same as SQS)

### SNS (not yet implemented)

- DeleteTopic, ListTopics, Get/SetTopicAttributes
- Unsubscribe, ListSubscriptionsByTopic

## System State

SQS core message lifecycle is operational. SNS fanout (CreateTopic, Subscribe, Publish, Get/SetSubscriptionAttributes, RawMessageDelivery, FilterPolicy) is operational.

**Operational:**

- `mokapot` binary boots, listens on configurable PORT (default 4566)
- `GET /_health` returns `{"status":"ok"}`
- Graceful shutdown on SIGINT/SIGTERM
- Structured JSON logging with configurable level
- Dockerfile (multi-stage, alpine) and docker-compose.yml operational
- SQS queue CRUD: CreateQueue (idempotent), GetQueueUrl, SendMessage, ReceiveMessage, DeleteMessage
- Dual protocol support: AWS Query/XML and AWS JSON 1.0 (Go/JS SDK v3)
- In-memory queue engine with per-queue mutex, visibility timeout, receipt handle tracking
- Visibility timeout with automatic reappearance: expired inflight messages return to available pool with new receipt handles and incremented ReceiveCount
- Long polling: ReceiveMessage blocks up to WaitTimeSeconds when no messages are available; wakes on message arrival, visibility timeout expiry, delayed message availability, or context cancellation; supports per-queue default via ReceiveMessageWaitTimeSeconds attribute
- Delayed messages: per-message DelaySeconds (0–900) overrides queue-level DelaySeconds attribute; delayed messages are invisible until AvailableAt; long-poll waiters wake when delayed messages become receivable
- Get/SetQueueAttributes with validation (mutable attribute whitelist, numeric range checks)
- Dead-letter queue: RedrivePolicy (deadLetterTargetArn + maxReceiveCount); messages exceeding maxReceiveCount are moved to DLQ during ReceiveMessage; DLQ existence validated on SetQueueAttributes
- Batch operations: SendMessageBatch and DeleteMessageBatch with per-entry error handling, partial failure support, duplicate ID detection, batch size validation (max 10), per-entry DelaySeconds override
- PurgeQueue: clears all messages (available, inflight, delayed) from a queue; 60-second cooldown enforced (PurgeQueueInProgress); queue remains usable after purge
- Injectable clock (`Engine.SetClock`) for deterministic time control in tests
- Integration tests using real AWS SDK Go v2 client against test server

**Not yet implemented:** DeleteQueue, ListQueues, ChangeMessageVisibility, ChangeMessageVisibilityBatch, SNS topic/subscription CRUD beyond CreateTopic+Subscribe+Publish+Get/SetSubscriptionAttributes, persistence

## Tech Stack

- **Language:** Go 1.25
- **Module:** `github.com/yarlson/mokapot`
- **Testing:** `testify`, `aws-sdk-go-v2` (integration tests)
- **Linting:** `golangci-lint`
- **Logging:** `log/slog` with JSON handler
- **Container:** Alpine-based multi-stage Docker build
- **Orchestration:** docker-compose
- **IDs:** `github.com/google/uuid`
