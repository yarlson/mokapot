# mokapot

Local AWS mock. One binary, zero credentials.

Point any AWS SDK at `localhost:4566` and stop waiting for cloud roundtrips. mokapot speaks both AWS protocols (Query/XML and JSON 1.0) so Go, Node, PHP, and Python SDKs work out of the box.

**Currently implemented: SQS + SNS.**

## Quick start

```bash
# Go
go run ./cmd/mokapot

# Docker (ephemeral, in-memory state)
docker run --rm -p 4566:4566 yarlson/mokapot:latest

# Docker Compose (persistent bbolt state via named volume)
docker compose up
```

Server listens on `:4566`. Health check at `GET /_health`.

## Install

### Homebrew (macOS)

```bash
brew tap yarlson/homebrew-tap
brew install --cask mokapot
```

### GitHub Releases (Linux/macOS)

Download the archive for your OS/arch from:
<https://github.com/yarlson/mokapot/releases>

```bash
VERSION=vX.Y.Z
OS=darwin   # darwin or linux
ARCH=arm64  # arm64 or amd64

curl -fL -o mokapot.tar.gz \
  "https://github.com/yarlson/mokapot/releases/download/${VERSION}/mokapot_${VERSION#v}_${OS}_${ARCH}.tar.gz"
tar -xzf mokapot.tar.gz
chmod +x mokapot
sudo mv mokapot /usr/local/bin/mokapot
```

## Docker usage

### Docker

Run with in-memory state (default):

```bash
docker run --rm -p 4566:4566 yarlson/mokapot:latest
```

Run with persisted bbolt state:

```bash
docker run --rm -p 4566:4566 \
  -e PERSISTENCE=bbolt \
  -e DATA_DIR=/data \
  -v mokapot-data:/data \
  yarlson/mokapot:latest
```

### Docker Compose

The bundled `docker-compose.yml` runs `yarlson/mokapot:latest` on port `4566` with:

- `PERSISTENCE=bbolt`
- `DATA_DIR=/data`
- `messaging-data` named volume

Start and verify:

```bash
docker compose up -d
curl -s http://localhost:4566/_health
docker compose logs -f messaging
```

Stop:

```bash
docker compose down
```

Remove persisted data volume:

```bash
docker compose down -v
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

`docker-compose.yml` overrides persistence defaults to `PERSISTENCE=bbolt` and `DATA_DIR=/data`.

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

```php
// PHP
$client = new Aws\Sqs\SqsClient([
    'region' => 'eu-central-1',
    'endpoint' => 'http://localhost:4566',
    'credentials' => ['key' => 'test', 'secret' => 'test'],
    'version' => 'latest',
]);
```

## Development

```bash
go test ./...        # unit + integration (real AWS SDK client)
golangci-lint run    # lint
```

### Releases

GitHub Actions currently has one release pipeline: `.github/workflows/release.yml`.
It runs on tags matching `v*` and executes GoReleaser v2 (`release --clean`).

- Homebrew tap publishing target: `yarlson/homebrew-tap`
- Docker image target: `yarlson/mokapot` (linux/amd64 + linux/arm64)
- GitHub release artifacts: `mokapot_<version>_<os>_<arch>.tar.gz` + `checksums.txt`
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
