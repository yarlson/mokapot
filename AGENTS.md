# Project Context

This is a Go-based CLI tool.

## Structure

`module github.com/yarlson/mokapot`

- `cmd/` - CLI commands (Cobra-based)
- `internal/` - Core business logic
- `main.go` - Entry point

## Dependency Management

When adding dependencies:

- Use `go get package@latest` (not direct edits to `go.mod`)
- Don't pin versions during installation
- Run `go mod tidy` after adding dependencies

## Development Workflow (Required)

**Test-Driven Development:**
Write tests BEFORE implementation. No exceptions.

1. Write failing test
2. Implement minimal code to pass
3. Refactor if needed
4. Run all checks

## Quality Checks (Required)

After every code change, you must run:

- `golangci-lint run` - linting (must show 0 issues)
- `go test ./...` - all tests (must pass)

This is non-negotiable. No commits without passing checks.

## Visual Validation

For CLI/TUI output, use the `scr` skill to validate visual presentation:

- Terminal output formatting and layout
- Color schemes and visual aesthetics
- Progress indicators and spinners
- Table rendering and alignment
- Error message display

Generate screenshots to verify the user-facing experience looks correct.

## Testing the Messaging Emulator

The most reliable way to verify each slice works end-to-end is to use the **official AWS SDK for Go v2** (`github.com/aws/aws-sdk-go-v2`) as the test client. It speaks the exact same protocol real applications use (SigV4 headers, query-encoded requests, XML responses) and catches compatibility issues that hand-crafted HTTP requests miss.

- Write integration tests using the Go SDK pointed at the local endpoint with dummy credentials.
- Each slice should have at least one Go SDK integration test that proves the user outcome works.
- This is preferred over Node.js/PHP SDK tests during development because it stays in-language, runs fast, and has zero cross-runtime friction.
- Node.js and PHP SDK tests remain valuable for final validation but are not the primary feedback loop.

## Language-Specific Conventions

- For Go conventions, see [docs/GO.md](docs/GO.md)
