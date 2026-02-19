# devstack

A lightweight, in-memory AWS SQS emulator for local development and testing. Built with Go's standard library, devstack provides a fully-functional SQS mock server that supports both AWS Query API (XML) and AWS JSON 1.0 protocols.

- **Local SQS development** without AWS credentials or network access
- **Full SQS API support** including queues, messages, visibility timeout, long polling, delays, and dead-letter queues
- **AWS SDK compatible** - works with aws-sdk-go-v2 out of the box
- **Docker-ready** with multi-stage builds and health checks
- **99 tests** covering unit and integration scenarios

---

## Prerequisites

| Requirement | Version                                |
| ----------- | -------------------------------------- |
| Go          | 1.25.0+                                |
| Docker      | Optional, for containerized deployment |

---

## Run (Local)

### Using Go directly

```bash
# Install dependencies
go mod download

# Run the server
go run ./cmd/messagingd/main.go
```

### Using Docker Compose

```bash
docker-compose up
```

### Building a binary

```bash
CGO_ENABLED=0 go build -o messagingd ./cmd/messagingd
./messagingd
```

The server listens on port **4566** by default.

---

## Configuration

All configuration is via environment variables. All variables are optional with sensible defaults.

| Variable     | Description                          | Default        |
| ------------ | ------------------------------------ | -------------- |
| `PORT`       | HTTP server port                     | `4566`         |
| `REGION`     | AWS region for queue ARNs            | `eu-central-1` |
| `ACCOUNT_ID` | AWS account ID for queue ARNs        | `000000000000` |
| `SQS_HOST`   | Hostname for queue URLs              | `localhost`    |
| `LOG_LEVEL`  | Log level (debug, info, warn, error) | `info`         |

### Example

```bash
PORT=5000 REGION=us-east-1 LOG_LEVEL=debug go run ./cmd/messagingd/main.go
```

---

## Ports & Health

| Port | Service           | Description                |
| ---- | ----------------- | -------------------------- |
| 4566 | Messaging Service | SQS emulator HTTP endpoint |

### Health Check

```bash
curl http://localhost:4566/_health
```

**Response:**

```json
{ "status": "ok" }
```

---

## Dependencies

### Direct Dependencies

| Package                                    | Purpose                         |
| ------------------------------------------ | ------------------------------- |
| `github.com/aws/aws-sdk-go-v2`             | AWS SDK core                    |
| `github.com/aws/aws-sdk-go-v2/credentials` | AWS credentials handling        |
| `github.com/aws/aws-sdk-go-v2/service/sqs` | SQS service client              |
| `github.com/google/uuid`                   | UUID generation for message IDs |
| `github.com/stretchr/testify`              | Testing assertions              |

---

## Deploy

### Docker

Build and run the container:

```bash
docker build -t devstack .
docker run -p 4566:4566 devstack
```

### Docker Compose

```bash
docker-compose up
```

### Graceful Shutdown

The server handles `SIGINT` and `SIGTERM` signals with a graceful shutdown timeout, ensuring in-flight requests complete before termination.

---

## Troubleshooting

No known issues documented. Check the following for debugging:

- Set `LOG_LEVEL=debug` for verbose logging
- Verify the health endpoint: `GET /_health`
- Ensure port 4566 is not in use by another process

---

## Development

### Running Tests

```bash
go test ./...
```

The test suite includes:

- Unit tests for the SQS engine, handlers, and query parser
- Integration tests using the AWS SDK v2 for Go

### Project Structure

| Directory           | Purpose                                            |
| ------------------- | -------------------------------------------------- |
| `cmd/messagingd/`   | Server entry point, signal handling, configuration |
| `internal/httpapi/` | HTTP routing and protocol detection                |
| `internal/sqs/`     | SQS engine, message lifecycle, queue management    |
| `internal/query/`   | AWS Query API parser                               |

### Supported SQS Operations

- `CreateQueue` - Create a new queue
- `GetQueueUrl` - Retrieve queue URL by name
- `GetQueueAttributes` - Fetch queue configuration
- `SetQueueAttributes` - Configure visibility timeout, delays, retention, redrive policy
- `SendMessage` - Send a single message with optional delay
- `SendMessageBatch` - Send up to 10 messages
- `ReceiveMessage` - Retrieve messages with visibility timeout
- `DeleteMessage` - Remove a message from the queue
- `DeleteMessageBatch` - Delete up to 10 messages
- Long polling support
- Dead-letter queue (DLQ) with redrive policy

---

## Contributing

Not documented. Check repository for contribution guidelines.

---

## License

Not documented. Check repository for license information.
