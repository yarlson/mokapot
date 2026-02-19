## PRD: Go-based Local Dev Emulator for AWS SQS + SNS (Node.js + PHP SDK compatible)

### Document status

- **Owner:** You
- **Primary consumer:** Agentic AI implementer (Go)
- **Target use:** **Local development** only, run via **docker-compose**
- **Compatibility goal:** Works with **AWS SDK for JavaScript (Node.js)** and **AWS SDK for PHP** using normal client APIs + endpoint override.

---

## 1. Problem statement

We need a lightweight, deterministic, locally runnable substitute for AWS **SQS + SNS** that:

- runs as a single Go service in docker-compose
- supports the subset of AWS behavior needed by real applications using Node.js and PHP SDKs
- provides correct-enough semantics (visibility timeout, long polling, DLQ redrive, SNS→SQS fanout, raw delivery, filter policies)
- does **not** require Terraform or production-grade security/IAM correctness

This replaces LocalStack **for SNS/SQS only** in local stacks.

---

## 2. Goals

### 2.1 Functional goals

1. **SQS Standard queues** with core API actions and semantics:
   - message lifecycle: send → receive → invisibility → delete / reappear
   - long polling (`WaitTimeSeconds`)
   - delay seconds
   - retention cleanup
   - batch actions

2. **DLQ / redrive policy**:
   - maxReceiveCount enforcement
   - move-to-DLQ behavior

3. **SNS Topics + SQS subscriptions**:
   - publish fanout to SQS
   - `RawMessageDelivery`
   - `FilterPolicy` (see scope)

4. **SDK compatibility**:
   - Node.js AWS SDK and PHP AWS SDK can operate with only endpoint override + dummy credentials.

5. **docker-compose friendly**:
   - single container
   - stable ports, stable account/region, optional persistence volume

### 2.2 Non-functional goals

- Fast startup (<1s typical)
- Deterministic behavior (especially in tests)
- Concurrency-safe under typical dev workloads
- Clear logs and a basic health endpoint

---

## 3. Non-goals (explicitly out of scope)

1. Production readiness (HA, multi-node, strong durability guarantees)
2. IAM policy evaluation and authZ enforcement
   - **We store/return policy strings if needed**, but do not enforce.

3. CloudWatch metrics, tracing integration, AWS service discovery
4. SNS protocols beyond `sqs` (http/https/email/sms/lambda/etc.)
   - These may return a clean “NotSupported” error.

5. Full “AWS-perfect” error text parity
   - Must return correct _error codes_ and broadly correct shapes; exact wording can differ.

6. Exactly matching AWS “approximate” counters or throttling behavior

---

## 4. Assumptions

- Consumers use **endpoint override** in Node.js and PHP SDKs.
- Consumers use dummy credentials like `test/test`.
- Region can be fixed (default `eu-central-1`) unless overridden.
- Account id can be fixed (default `000000000000`) unless overridden.
- No Terraform, no CloudFormation, no IAM dependencies.

---

## 5. User stories

### SQS

- As a developer, I can create a queue and send messages to it.
- As a developer, my service can long-poll the queue and process messages.
- As a developer, if processing fails and message is not deleted, it becomes visible again after visibility timeout.
- As a developer, I can configure a DLQ and messages are moved after maxReceiveCount.
- As a developer, I can purge the queue during tests.

### SNS

- As a developer, I can create a topic and subscribe an SQS queue.
- As a developer, publishing to a topic delivers messages to all subscribed queues.
- As a developer, I can enable raw delivery to receive plain message bodies.
- As a developer, filter policies prevent delivery unless attributes match.

---

## 6. API compatibility strategy

### 6.1 Protocol

Implement **AWS Query API style** requests:

- Input: `application/x-www-form-urlencoded` (typical)
- Also accept querystring (GET) for robustness, even if not used.

You do not need to implement JSON RPC protocols for SNS/SQS.

### 6.2 Auth (SigV4)

- Accept requests that include SigV4 headers/params:
  - `Authorization`, `X-Amz-Date`, `X-Amz-Content-Sha256`, `X-Amz-Security-Token`

- **Do not validate signatures** (local dev) but **must not reject signed requests**.
- Optional: allow a “strict mode” flag for signature validation later, default off.

### 6.3 Endpoint and routing

Single endpoint recommended (LocalStack-style):

- `http://messaging:4566/`

SQS queue URLs must look like:

- `http://{host}:{port}/{accountId}/{queueName}`

Important: AWS SDKs often send queue-specific actions **to the queue URL path**, not to `/`.
So the server must accept both:

- `POST /` for actions like `CreateQueue`, `ListQueues`
- `POST /{accountId}/{queueName}` for queue-scoped actions like `SendMessage`, `ReceiveMessage`, etc.

SNS uses `TopicArn` parameters and can be handled on `/` consistently.

---

## 7. Supported actions (MVP + completeness for local dev)

### 7.1 SQS actions

**Queue lifecycle**

- `CreateQueue`
- `DeleteQueue`
- `ListQueues`
- `GetQueueUrl`

**Attributes**

- `GetQueueAttributes`
- `SetQueueAttributes`

**Messaging**

- `SendMessage`
- `SendMessageBatch`
- `ReceiveMessage`
- `DeleteMessage`
- `DeleteMessageBatch`
- `ChangeMessageVisibility`
- `ChangeMessageVisibilityBatch`
- `PurgeQueue`

**Optional (explicit toggle; default off unless required)**

- FIFO queues: `FifoQueue`, `ContentBasedDeduplication`, group ordering, dedup window

### 7.2 SNS actions

- `CreateTopic`
- `DeleteTopic`
- `ListTopics`
- `GetTopicAttributes`
- `SetTopicAttributes`
- `Subscribe`
- `Unsubscribe`
- `ListSubscriptionsByTopic`
- `GetSubscriptionAttributes`
- `SetSubscriptionAttributes`
- `Publish`

**Supported protocol**

- `sqs` only

---

## 8. Data model

### 8.1 Identifiers

- **Region:** configurable, default `eu-central-1`
- **AccountId:** configurable, default `000000000000`

### 8.2 SQS entities

**Queue**

- `Name` (string)
- `URL` (string): `http://host:port/{accountId}/{name}`
- `ARN` (string): `arn:aws:sqs:{region}:{accountId}:{name}`
- `Attributes` (map string→string):
  - Enforced:
    - `VisibilityTimeout` (seconds, default 30)
    - `ReceiveMessageWaitTimeSeconds` (seconds, default 0)
    - `DelaySeconds` (seconds, default 0)
    - `MessageRetentionPeriod` (seconds, default 345600)
    - `RedrivePolicy` (json string, optional)

  - Stored/returned (not enforced unless easy):
    - `Policy`, `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`, etc.

- Internal runtime state:
  - `available` messages store
  - `inflight` messages store
  - `delayed` messages store
  - `waiters` (long polling)
  - `dedup` cache (FIFO optional)

**Message**

- `MessageId` (uuid-like string)
- `Body` (string)
- `MessageAttributes` (typed map):
  - String / Number / Binary

- System attributes (computed):
  - `SentTimestamp` (ms epoch)
  - `ApproximateFirstReceiveTimestamp` (ms epoch)
  - `ApproximateReceiveCount` (string integer)

- `MD5OfBody`
- `MD5OfMessageAttributes`
- Runtime fields:
  - `availableAt` (time) for delay
  - `invisibleUntil` (time) for visibility
  - `lastReceiptHandle` (string, updated each receive)
  - `firstReceivedAt` (time optional)

### 8.3 SNS entities

**Topic**

- `Name`
- `ARN`: `arn:aws:sns:{region}:{accountId}:{name}`
- `Attributes` map string→string (store/return)
- `Subscriptions` list of Subscription IDs

**Subscription**

- `SubscriptionArn` (string)
- `TopicArn` (string)
- `Protocol` = `sqs`
- `Endpoint` = queue ARN or queue URL (accept both; normalize internally to queue ARN)
- `Attributes`:
  - `RawMessageDelivery` (true/false)
  - `FilterPolicy` (json string)
  - store/return any others without enforcement

---

## 9. Core semantics and algorithms

### 9.1 SQS message visibility

**ReceiveMessage** behavior:

1. Select up to `MaxNumberOfMessages` (1..10, default 1) that are:
   - available now (`availableAt <= now`)
   - not inflight (`invisibleUntil <= now`)
   - not moved to DLQ

2. For each selected message:
   - increment receive count
   - set `firstReceivedAt` if unset
   - generate a new `ReceiptHandle` (random)
   - set `invisibleUntil = now + visibilityTimeout`
     - visibility timeout comes from request param `VisibilityTimeout` if present else queue attribute

   - move message to inflight map keyed by receipt handle

3. Return messages including:
   - `MessageId`, `ReceiptHandle`, `Body`, requested attributes, message attributes

**DeleteMessage**:

- Requires a receipt handle that currently maps to an inflight message.
- On delete:
  - remove from inflight
  - remove message from any other storage

- If receipt handle invalid: return AWS-ish error code:
  - `ReceiptHandleIsInvalid`

**Visibility timeout expiry**:

- When `now >= invisibleUntil`, message becomes available again **unless** it is moved to DLQ.

### 9.2 Long polling

If no messages are available:

- If `WaitTimeSeconds` > 0, block until:
  - a message becomes available, or
  - timeout occurs

- Wake immediately when:
  - a message is sent to the queue
  - a delayed message becomes available
  - an inflight message visibility expires into availability

Implementation requirement:

- Must not busy-wait; use waiter channels/condition vars.

### 9.3 DelaySeconds

On send:

- compute `availableAt = now + delay` where delay comes from:
  - request `DelaySeconds` if present else queue attribute

- store in delayed structure (min-heap by availableAt)
- when time reached, move into available and notify waiters

### 9.4 Retention

Periodic cleanup:

- any message older than retention period is deleted (both available and inflight)
- ensure inflight entries removed as well

### 9.5 DLQ / RedrivePolicy

If queue has `RedrivePolicy` set, it contains:

- `deadLetterTargetArn`
- `maxReceiveCount`

On each receive attempt:

- if message’s receive count becomes `> maxReceiveCount`:
  - remove message from current queue
  - enqueue it into DLQ as a new message **or** same message (implementation choice)
  - preserve:
    - body
    - message attributes
    - system attributes if feasible

  - do not return it in the receive response

Notes:

- Prefer preserving `MessageId`? AWS behavior varies; for local dev, preserving body+attrs is most important.
- Log DLQ moves at info level.

### 9.6 Batch APIs

**SendMessageBatch**:

- Up to 10 entries
- Response includes:
  - `Successful` entries (MessageId, MD5s)
  - `Failed` entries (Id, SenderFault, Code, Message)

**DeleteMessageBatch / ChangeMessageVisibilityBatch**:

- Same successful/failed structure
- Partial failures must be supported.

### 9.7 Message attribute encoding + MD5

- Implement MD5 calculation for:
  - body
  - message attributes

- Must be stable and match AWS SDK expectations well enough that SDK parsing doesn’t break.
- If exact MD5-of-attributes matching is painful, you may:
  - compute MD5 over a deterministic canonical encoding of attributes
  - include it consistently
  - ensure tests do not depend on AWS-perfect attribute MD5

(But best effort: implement AWS-style canonicalization.)

---

## 10. SNS publish and delivery

### 10.1 Publish

Inputs:

- `TopicArn`
- `Message`
- Optional `Subject`
- Optional `MessageAttributes`
- Optional `MessageStructure=json` (MVP behavior: store/ignore or partial support)

Behavior:

1. Lookup topic by arn.
2. For each subscription:
   - Only support protocol `sqs`
   - Apply filter policy (if present)
   - Build delivery payload:
     - If `RawMessageDelivery=true`:
       - payload body = published `Message`

     - Else:
       - payload body = SNS JSON envelope:
         - include at least: Type, MessageId, TopicArn, Message, Subject (optional), Timestamp, MessageAttributes

3. Enqueue payload into subscribed SQS queue (respecting that queue’s DelaySeconds, retention, etc.)
4. Return `MessageId` for publish response.

### 10.2 Filter policy

- Implement filter policy evaluation against **published MessageAttributes**.
- Scope for local dev emulator:
  - Support common patterns:
    - exact match for strings/numbers
    - allowlist arrays
    - `exists`
    - `anything-but`
    - numeric comparisons (>=, <=, between)

  - Parse `FilterPolicy` JSON; if invalid, return `InvalidParameter` error.

If full AWS parity of filter rules is too costly, implement above subset and document unsupported operators clearly in errors.

---

## 11. Error handling and response shapes

### 11.1 Response format

Return AWS Query-style XML-ish responses with:

- `<ActionNameResponse>`
- `<ResponseMetadata><RequestId>...</RequestId></ResponseMetadata>`

For errors:

- HTTP status 400 (or 403 for auth-ish), body like:
  - `<ErrorResponse><Error><Type>Sender</Type><Code>...</Code><Message>...</Message></Error><RequestId>...</RequestId></ErrorResponse>`

### 11.2 Error codes to implement (minimum)

SQS:

- `QueueDoesNotExist`
- `QueueNameExists`
- `InvalidParameterValue`
- `InvalidParameter`
- `ReceiptHandleIsInvalid`
- `PurgeQueueInProgress` (optional: implement cooldown)
- `AWS.SimpleQueueService.NonExistentQueue` (alias if SDK expects)

SNS:

- `NotFound`
- `InvalidParameter`
- `InvalidParameterValue`
- `AuthorizationError` (not actually enforced; rarely needed)
- `SubscriptionLimitExceeded` (unlikely; can omit)

---

## 12. Configuration

### 12.1 Environment variables (all optional)

- `PORT` (default `4566`)
- `HOSTNAME` (default `messaging` or derived)
- `REGION` (default `eu-central-1`)
- `ACCOUNT_ID` (default `000000000000`)
- `DATA_DIR` (default empty → in-memory only)
- `PERSISTENCE` (`memory` | `bbolt`, default `memory`)
- `LOG_LEVEL` (`debug|info|warn|error`, default `info`)
- `STRICT_SIGV4` (`true|false`, default `false`)
- `ENABLE_FIFO` (`true|false`, default `false`)

### 12.2 CLI flags (mirror env vars)

- `--port`
- `--region`
- `--account-id`
- `--data-dir`
- `--persistence`
- `--log-level`
- `--strict-sigv4`
- `--enable-fifo`

---

## 13. Persistence (optional but recommended)

### 13.1 Modes

- **In-memory (default):** fastest, wiped on restart
- **bbolt:** single file under `DATA_DIR/state.db`

### 13.2 Persistence scope

Persist:

- queues, queue attributes
- topics, subscriptions, attributes
- messages (available, delayed, inflight)
- inflight deadlines and receive counts

On restart:

- restore message states
- recompute timers (deadlines) and resume scheduler

---

## 14. Runtime architecture

### 14.1 Package/module structure (recommended)

- `cmd/messagingd/` – main
- `internal/httpapi/` – HTTP server, routing, middleware
- `internal/query/` – Query decoder, XML encoder, error helpers
- `internal/sqs/` – handlers + queue engine
- `internal/sns/` – handlers + topic/subscription + delivery
- `internal/store/` – interfaces + memory store + bbolt store
- `internal/runtime/` – schedulers (visibility, delay, retention), waiter management
- `internal/types/` – shared structs, canonical encodings
- `internal/testing/` – test utilities, golden tests, sdk harness

### 14.2 Concurrency model

Per-queue runtime:

- mutex guarding:
  - available deque
  - inflight map (receiptHandle → message ref + deadline)
  - delayed heap
  - waiter list

- global scheduler goroutine:
  - wakes at next known deadline across all queues
  - moves expired inflight → available
  - moves delayed → available
  - triggers retention cleanup periodically

- waiter mechanism:
  - `ReceiveMessage` registers a waiter channel with deadline
  - on message availability change, notify waiters

Hard requirement:

- No per-message goroutine timers (avoid goroutine explosion).

---

## 15. Observability

### 15.1 Logging

Structured logs (json preferred) including:

- request id
- action
- queue/topic identifiers
- result status
- key lifecycle events:
  - message moved to inflight
  - visibility expired
  - DLQ move
  - sns publish fanout counts

### 15.2 Health endpoints

- `GET /_health` → `200 OK` with `{ "status": "ok" }`
- `GET /_ready` (optional) → readiness (store opened, scheduler started)

---

## 16. docker-compose integration

### 16.1 Service definition

The service must run cleanly in docker-compose with a named volume for persistence.

Example:

```yaml
services:
  messaging:
    image: yourorg/messagingd:dev
    ports:
      - "4566:4566"
    environment:
      PORT: "4566"
      REGION: "eu-central-1"
      ACCOUNT_ID: "000000000000"
      PERSISTENCE: "bbolt"
      DATA_DIR: "/data"
      LOG_LEVEL: "info"
    volumes:
      - messaging-data:/data

volumes:
  messaging-data:
```

### 16.2 App config expectations (documented in README)

Node.js + PHP apps set:

- endpoint: `http://messaging:4566`
- region: `eu-central-1` (or env)
- credentials: dummy

---

## 17. Testing requirements (must be implemented)

### 17.1 Unit tests (Go)

- Queue engine correctness:
  - visibility transitions
  - receipt handle invalidation
  - long polling wakeup correctness
  - delay delivery timing behavior (use fake clock if possible)
  - DLQ move behavior

- Filter policy evaluation tests

### 17.2 Integration tests (SDK-level)

Provide a test harness that runs inside CI using docker:

- Start `messagingd`
- Run:
  1. Node.js test using AWS SDK (v3 typical)
  2. PHP test using AWS SDK for PHP

**Required integration scenarios**
SQS:

1. CreateQueue → SendMessage → ReceiveMessage → DeleteMessage (message disappears)
2. ReceiveMessage without delete → message reappears after visibility timeout
3. Long polling: ReceiveMessage blocks then returns promptly after SendMessage
4. DelaySeconds: SendMessage with delay not received before delay passes
5. Batch send + batch delete partial failure simulation
6. DLQ:
   - configure RedrivePolicy
   - receive message repeatedly without delete
   - confirm it ends up in DLQ after maxReceiveCount

SNS:

1. CreateTopic → Subscribe queue → Publish → message arrives in queue
2. RawMessageDelivery on/off changes payload body format
3. FilterPolicy blocks/allows delivery based on message attributes

### 17.3 Golden response shape tests

- For each implemented action, create a snapshot test asserting:
  - response root node names
  - required fields exist
  - error responses include Code/Message/RequestId

---

## 18. Acceptance criteria (Definition of Done)

### Functional DoD

- All actions in Sections 7.1 and 7.2 implemented and documented.
- SDK integration tests for Node.js and PHP pass using endpoint override.
- Core semantics verified by tests:
  - visibility timeout
  - long polling
  - DLQ redrive
  - SNS→SQS delivery (raw + envelope)
  - filter policy subset

### Operational DoD

- Runs in docker-compose with documented config.
- `/ _health` returns OK.
- Logs include request IDs and key lifecycle events.
- Persistence mode works (bbolt) and survives container restart.

### Documentation DoD

- README includes:
  - docker-compose snippet
  - Node.js sample client config
  - PHP sample client config
  - supported actions list
  - limitations list (what’s intentionally not supported)

---

## 19. Explicit limitations (must be documented)

- No IAM enforcement (policies stored/returned only)
- Only SNS protocol `sqs`
- Filter policy support: documented subset (or full if implemented)
- FIFO disabled by default; if enabled, only queue FIFO (and define exact supported behavior)

---

## 20. Appendix: Response skeleton examples (minimum required fields)

### 20.1 SQS CreateQueueResponse

```xml
<CreateQueueResponse>
  <CreateQueueResult>
    <QueueUrl>http://messaging:4566/000000000000/my-queue</QueueUrl>
  </CreateQueueResult>
  <ResponseMetadata>
    <RequestId>req-123</RequestId>
  </ResponseMetadata>
</CreateQueueResponse>
```

### 20.2 SQS ReceiveMessageResponse

```xml
<ReceiveMessageResponse>
  <ReceiveMessageResult>
    <Message>
      <MessageId>mid-123</MessageId>
      <ReceiptHandle>rh-abc</ReceiptHandle>
      <MD5OfBody>...</MD5OfBody>
      <Body>hello</Body>
      <Attribute>
        <Name>ApproximateReceiveCount</Name>
        <Value>1</Value>
      </Attribute>
    </Message>
  </ReceiveMessageResult>
  <ResponseMetadata>
    <RequestId>req-456</RequestId>
  </ResponseMetadata>
</ReceiveMessageResponse>
```

### 20.3 SNS PublishResponse

```xml
<PublishResponse>
  <PublishResult>
    <MessageId>pub-123</MessageId>
  </PublishResult>
  <ResponseMetadata>
    <RequestId>req-789</RequestId>
  </ResponseMetadata>
</PublishResponse>
```

### 20.4 ErrorResponse

```xml
<ErrorResponse>
  <Error>
    <Type>Sender</Type>
    <Code>QueueDoesNotExist</Code>
    <Message>The specified queue does not exist.</Message>
  </Error>
  <RequestId>req-000</RequestId>
</ErrorResponse>
```

---

## 21. Implementation checklist (for the agentic AI)

1. Implement query parser + router (root vs queue path).
2. Implement XML response builder + error builder.
3. Implement in-memory store with concurrency-safe structures.
4. Implement queue engine (available/inflight/delayed + waiters).
5. Implement scheduler (visibility expiry + delay + retention).
6. Implement SQS handlers for all actions.
7. Implement SNS topic/subscription model.
8. Implement SNS publish → SQS enqueue with raw/envelope + filter policy.
9. Add optional bbolt persistence.
10. Add health endpoints + logging.
11. Add Go unit tests + Node/PHP integration tests.
12. Package docker image + compose example + README.
