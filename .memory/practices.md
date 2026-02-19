# Practices and Conventions

## Development Workflow

- **Test-Driven Development** is mandatory: write failing test first, implement, refactor, run checks
- After every code change run: `golangci-lint run` (0 issues) and `go test ./...` (all pass)
- No commits without passing checks

## Go Conventions

- Idiomatic Go: no unnecessary abstractions, no Java-style patterns
- Composition over complex hierarchies
- Prefer standard library patterns
- Let golangci-lint guide style decisions
- Interfaces close to consumers, not in separate `interfaces.go` files
- DRY, KISS, YAGNI

## Dependencies

- Use `go get package@latest` (never edit go.mod directly)
- Run `go mod tidy` after adding dependencies
- Don't pin versions during installation

## Testing

- Primary test client: **AWS SDK for Go v2** (`github.com/aws/aws-sdk-go-v2`) for integration tests — same protocol real apps use, catches SigV4/XML compatibility issues
- Unit tests use `testify` (assert + require)
- Golden response shape tests for each action (verify XML structure, required fields, error shapes)
- Node.js and PHP SDK tests for final validation, not primary feedback loop
- **Deterministic time in tests**: use `Engine.SetClock` to inject a controllable clock; advance time by mutating the closure — never use `time.Sleep` in tests

## Project Structure

```
cmd/messagingd/    — entrypoint; configures engine, handler, HTTP server
internal/httpapi/  — HTTP server, routing (health, root POST, queue-scoped POST)
internal/query/    — form-encoded request parser, XML response writer, error helpers
internal/sqs/      — SQS handler (protocol dispatch) + engine (in-memory queue store)
internal/sns/      — handlers + topic/subscription + delivery (planned)
internal/store/    — interfaces + memory store + bbolt store (planned)
internal/runtime/  — schedulers, waiter management (planned)
internal/types/    — shared structs, canonical encodings (planned)
```

## Configuration

All via environment variables (optional, with defaults):
- `PORT` (4566), `REGION` (eu-central-1), `ACCOUNT_ID` (000000000000), `SQS_HOST` (localhost)
- `LOG_LEVEL` (info), `DATA_DIR` (empty = in-memory), `PERSISTENCE` (memory)

## Concurrency Model

- Per-queue mutex guarding: available slice, inflight map, waiter list, delayed heap (planned)
- Visibility reappearance is **lazy**: expired inflight messages are requeued at the start of each `ReceiveMessage` call (no background goroutine)
- **Long polling**: waiter struct holds a buffered channel; `SendMessage` calls `notifyWaiters()` (under queue lock) to wake all blocked receivers; polling loop also wakes on nearest inflight expiry to pick up reappearing messages
- No per-message goroutine timers (avoid goroutine explosion)
- No global scheduler goroutine for long polling — each long-polling `ReceiveMessage` manages its own timer and waiter lifecycle
- `ReceiveMessage` accepts `context.Context` — context cancellation terminates long polls gracefully

## Error Handling

- AWS Query-style XML error responses with Code, Message, RequestId
- HTTP 400 for client errors; correct AWS error codes (QueueDoesNotExist, ReceiptHandleIsInvalid, etc.)

## Visual Validation

- Use `scr` skill for CLI/TUI output validation (formatting, colors, tables, progress indicators)
