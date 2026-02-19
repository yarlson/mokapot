# devstack — Local AWS SQS/SNS Emulator

## What

A lightweight, Go-based local development emulator for AWS SQS and SNS. Replaces LocalStack for SNS/SQS only. Runs as a single container in docker-compose. Compatible with AWS SDK for JavaScript (Node.js) and AWS SDK for PHP via endpoint override + dummy credentials.

## Architecture

- **Single Go binary** (`messagingd`) serving HTTP on port 4566
- **AWS Query API protocol**: `application/x-www-form-urlencoded` input, XML responses
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

The project is in early development. Slice 1 (service boots in docker-compose) is implemented:
- `messagingd` binary boots, listens on configurable PORT (default 4566)
- `GET /_health` returns `{"status":"ok"}`
- Graceful shutdown on SIGINT/SIGTERM
- Structured JSON logging with configurable level
- Dockerfile (multi-stage, alpine) and docker-compose.yml operational

No SQS/SNS business logic implemented yet.

## Tech Stack

- **Language:** Go 1.25
- **Module:** `github.com/yarlson/devstack`
- **Testing:** `testify`
- **Linting:** `golangci-lint`
- **Logging:** `log/slog` with JSON handler
- **Container:** Alpine-based multi-stage Docker build
- **Orchestration:** docker-compose
