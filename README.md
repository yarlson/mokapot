# mokapot

Local AWS mock. One binary, zero credentials.

Point any AWS SDK at `localhost:4566` and stop waiting for cloud roundtrips. mokapot speaks both AWS protocols (Query/XML and JSON 1.0) so Go, Node, PHP, and Python SDKs work out of the box.

**Currently implemented: SQS.** SNS is next.

## Quick start

```bash
# Go
go run ./cmd/mokapot

# Docker
docker build -t mokapot .
docker run -p 4566:4566 mokapot

# Docker Compose
docker-compose up
```

Server listens on `:4566`. Health check at `GET /_health`.

## What works

### SQS

Full message lifecycle with both protocols:

| Category | Operations                                                               |
| -------- | ------------------------------------------------------------------------ |
| Queues   | `CreateQueue`, `GetQueueUrl`, `GetQueueAttributes`, `SetQueueAttributes` |
| Messages | `SendMessage`, `ReceiveMessage`, `DeleteMessage`                         |
| Batches  | `SendMessageBatch`, `DeleteMessageBatch` (partial failure, max 10)       |

Plus the hard parts:

- **Long polling** - `WaitTimeSeconds` up to 20s, wakes on new messages, visibility expiry, or delay expiry
- **Visibility timeout** - inflight tracking, auto-requeue with new receipt handles, `ReceiveCount` increment
- **Delayed messages** - per-message `DelaySeconds` (0-900) overrides queue default
- **Dead-letter queues** - `RedrivePolicy` with `maxReceiveCount`, auto-migration to DLQ

### SNS

Not yet implemented.

## Configuration

All env vars, all optional:

| Variable     | Default        | Purpose                             |
| ------------ | -------------- | ----------------------------------- |
| `PORT`       | `4566`         | Server port                         |
| `REGION`     | `eu-central-1` | Region in ARNs and queue URLs       |
| `ACCOUNT_ID` | `000000000000` | Account ID in ARNs and queue URLs   |
| `SQS_HOST`   | `localhost`    | Hostname in queue URLs              |
| `LOG_LEVEL`  | `info`         | `debug` / `info` / `warn` / `error` |

## SDK wiring

Point your SDK's endpoint at mokapot. Credentials can be anything:

```go
// Go
client := sqs.New(sqs.Options{
    Region:       "eu-central-1",
    BaseEndpoint: aws.String("http://localhost:4566"),
    Credentials:  credentials.NewStaticCredentialsProvider("x", "x", ""),
})
```

```javascript
// Node.js
const client = new SQSClient({
  region: "eu-central-1",
  endpoint: "http://localhost:4566",
  credentials: { accessKeyId: "x", secretAccessKey: "x" },
});
```

## Development

```bash
go test ./...        # 99 tests - unit + integration (real AWS SDK client)
golangci-lint run    # lint
```

### Project layout

```
cmd/mokapot/        Server entrypoint, config, signal handling
internal/httpapi/   HTTP routing, protocol detection (JSON vs XML)
internal/sqs/       Queue engine, message lifecycle, handlers
internal/query/     AWS Query API (form-encoded) parser
```

### Design notes

- **stdlib only** - no HTTP framework, just `net/http`
- **In-memory** - no persistence, data gone on restart
- **Thread-safe** - engine-level RWMutex + per-queue mutex
- **Injectable clock** - `Engine.SetClock()` for deterministic time tests, no `time.Sleep` in test suite
- **Dual protocol from one handler** - Content-Type sniffing routes to JSON or XML codepath
