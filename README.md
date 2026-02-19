# mokapot

A lightweight, in-memory AWS service mock for local development and testing. Point your AWS SDK at mokapot instead of real AWS and develop offline with zero credentials.

Currently supports **SQS** and **SNS**. More services planned.

- **Drop-in AWS replacement** - works with any AWS SDK via endpoint override
- **Dual protocol** - AWS Query API (XML) and AWS JSON 1.0
- **Docker-ready** with multi-stage builds and health checks

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
go run ./cmd/mokapot/main.go
```

### Using Docker Compose

```bash
docker-compose up
```

### Building a binary

```bash
CGO_ENABLED=0 go build -o mokapot ./cmd/mokapot
./mokapot
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
PORT=5000 REGION=us-east-1 LOG_LEVEL=debug go run ./cmd/mokapot/main.go
```

---

## Ports & Health

| Port | Service | Description       |
| ---- | ------- | ----------------- |
| 4566 | mokapot | AWS mock endpoint |

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
docker build -t mokapot .
docker run -p 4566:4566 mokapot
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
| `cmd/mokapot/`      | Server entry point, signal handling, configuration |
| `internal/httpapi/` | HTTP routing and protocol detection                |
| `internal/sqs/`     | SQS engine, message lifecycle, queue management    |
| `internal/query/`   | AWS Query API parser                               |

### Supported Services

#### SQS

- `CreateQueue`, `GetQueueUrl`, `GetQueueAttributes`, `SetQueueAttributes`
- `SendMessage`, `SendMessageBatch`
- `ReceiveMessage` with long polling and visibility timeout
- `DeleteMessage`, `DeleteMessageBatch`
- Dead-letter queues with redrive policy

#### SNS

Not yet implemented.

---

## Contributing

Not documented. Check repository for contribution guidelines.

---

## License

Not documented. Check repository for license information.
