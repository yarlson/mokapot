# mokapot

Local AWS mock. One binary, zero credentials.

Point any AWS SDK at `localhost:4566` and stop waiting for cloud roundtrips. mokapot speaks both AWS protocols (Query/XML and JSON 1.0) so Go, Node, PHP, and Python SDKs work out of the box.

**Currently implemented: SQS + SNS.**

## Quick start

```bash
# Go
go run ./cmd/mokapot

# Docker
docker run -p 4566:4566 yarlson/mokapot:latest

# Docker Compose
docker-compose up
```

Server listens on `:4566`. Health check at `GET /_health`.

## Install

```bash
brew tap yarlson/homebrew-tap
brew install --cask mokapot
```

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

Topic + subscription + publish flow with both protocols:

| Category      | Operations                                                                                                                    |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Topics        | `CreateTopic`, `ListTopics`, `DeleteTopic`, `GetTopicAttributes`, `SetTopicAttributes`                                        |
| Subscriptions | `Subscribe` (`sqs` only), `Unsubscribe`, `ListSubscriptionsByTopic`, `GetSubscriptionAttributes`, `SetSubscriptionAttributes` |
| Publishing    | `Publish` with SNS envelope fanout to subscribed SQS queues                                                                   |

Plus the hard parts:

- **Raw message delivery** - per-subscription toggle via `RawMessageDelivery`
- **Filter policies** - exact match, prefix, exists, anything-but, and numeric operators
- **Message attributes** - accepted on publish, propagated in envelope, and used for filter matching

## Configuration

All env vars are optional unless noted:

| Variable      | Default        | Purpose                                              |
| ------------- | -------------- | ---------------------------------------------------- |
| `PORT`        | `4566`         | Server port                                          |
| `REGION`      | `eu-central-1` | Region in ARNs and queue URLs                        |
| `ACCOUNT_ID`  | `000000000000` | Account ID in ARNs and queue URLs                    |
| `SQS_HOST`    | `localhost`    | Hostname in queue URLs                               |
| `LOG_LEVEL`   | `info`         | `debug` / `info` / `warn` / `error`                  |
| `PERSISTENCE` | `memory`       | State backend: `memory` or `bbolt`                   |
| `DATA_DIR`    | _(empty)_      | Required when `PERSISTENCE=bbolt`; stores `state.db` |

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
go test ./...        # unit + integration (real AWS SDK client)
golangci-lint run    # lint
```

### Releases

Tags matching `v*` run GoReleaser v2 via `.github/workflows/release.yml`.

- Homebrew tap publishing target: `yarlson/homebrew-tap`
- Docker image target: `yarlson/mokapot` (linux/amd64 + linux/arm64)
- Required GitHub secret for cross-repo tap pushes: `GH_PAT` (repo-scoped PAT)
- Required Docker Hub secrets: `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`

### Project layout

```
cmd/mokapot/        Server entrypoint, config, signal handling
internal/httpapi/   HTTP routing, protocol detection (JSON vs XML)
internal/sqs/       Queue engine, message lifecycle, handlers
internal/sns/       Topic engine, subscriptions, publish handlers
internal/store/     Optional bbolt persistence (save/restore snapshots)
internal/query/     AWS Query API (form-encoded) parser
```

### Design notes

- **stdlib only** - no HTTP framework, just `net/http`
- **In-memory by default** - optional persistence with `PERSISTENCE=bbolt` + `DATA_DIR`
- **Thread-safe** - engine-level RWMutex + per-queue mutex
- **Injectable clock** - `Engine.SetClock()` for deterministic time tests, no `time.Sleep` in test suite
- **Dual protocol from one handler** - Content-Type sniffing routes to JSON or XML codepath

## License

[MIT](LICENSE)

---

Built with ❤️ for developers who value simplicity and speed.
