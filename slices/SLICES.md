# Vertical Slices

Each slice is a small, end-to-end, user-visible outcome that can ship independently.

---

## Slice 1 — Service boots in docker-compose

**User outcome:** Developer adds one service to docker-compose, runs `docker-compose up`, hits `/_health`, gets OK.

**What becomes possible:** Developer confirms the emulator is reachable before wiring up any application code.

**Why minimal:** No SQS/SNS logic. Just HTTP listener, health endpoint, docker image, and compose config.

---

## Slice 2 — Send and receive a message

**User outcome:** Developer points AWS SDK at the local endpoint with dummy credentials, creates a queue, sends a message, receives it, and deletes it.

**What becomes possible:** The core message-passing workflow works locally. Applications can develop against SQS without real AWS.

**Why minimal:** Smallest set of operations that completes a full message lifecycle: CreateQueue, GetQueueUrl, SendMessage, ReceiveMessage, DeleteMessage. Also requires the query protocol parser, XML response builder, SigV4 passthrough, and queue-URL-path routing — but those exist only to serve this outcome.

---

## Slice 3 — Undeleted messages reappear

**User outcome:** Developer receives a message but doesn't delete it. After the visibility timeout, the same message is receivable again.

**What becomes possible:** Retry-on-failure semantics. Simulates what happens in production when a consumer crashes mid-processing.

**Why minimal:** Adds one timer (invisibleUntil) to the existing message state. No new API surface.

---

## Slice 4 — Long polling

**User outcome:** Developer calls ReceiveMessage with WaitTimeSeconds on an empty queue. When a message is sent from another process, the receive returns immediately with that message.

**What becomes possible:** Efficient consumers that block instead of busy-polling. Matches how production consumers behave.

**Why minimal:** Adds a waiter/notify mechanism to ReceiveMessage. No new API actions.

---

## Slice 5 — Delayed messages

**User outcome:** Developer sends a message with DelaySeconds. The message is not receivable until the delay elapses.

**What becomes possible:** Deferred processing patterns (e.g., "process this in 30 seconds").

**Why minimal:** Adds a delayed bucket and one timer to the existing send path. No new API actions.

---

## Slice 6 — Dead-letter queue

**User outcome:** Developer configures a main queue with a RedrivePolicy pointing to a DLQ. After receiving a message N times without deleting it, the message appears in the DLQ.

**What becomes possible:** Poison message isolation. Developer can inspect failed messages in a separate queue.

**Why minimal:** Adds receive-count threshold check and cross-queue move. Pulls in Get/SetQueueAttributes for RedrivePolicy configuration.

---

## Slice 7 — Batch send and batch delete

**User outcome:** Developer sends 10 messages in one call and deletes them in one call. Partial failures are reported per-entry.

**What becomes possible:** Bulk operations match production SDK usage patterns. Partial-failure handling can be tested locally.

**Why minimal:** Wraps existing single-message logic with batch request/response envelope. Adds SendMessageBatch, DeleteMessageBatch.

---

## Slice 8 — Purge queue

**User outcome:** Developer calls PurgeQueue between test runs. All messages are gone.

**What becomes possible:** Fast test isolation without destroying and recreating queues.

**Why minimal:** Single action, clears existing message stores.

---

## Slice 9 — SNS publish fans out to SQS queues

**User outcome:** Developer creates a topic, subscribes two SQS queues, publishes a message. Both queues receive the message wrapped in an SNS JSON envelope.

**What becomes possible:** Event fanout patterns work locally. Multiple consumers react to a single publish.

**Why minimal:** Introduces the SNS domain (CreateTopic, Subscribe, Publish) with one delivery mode (envelope). Smallest unit that proves fanout works end-to-end.

---

## Slice 10 — SNS raw message delivery

**User outcome:** Developer enables RawMessageDelivery on a subscription. Published message body arrives in the queue unwrapped — no SNS envelope.

**What becomes possible:** Consumers that expect plain message bodies (a very common production pattern) work correctly.

**Why minimal:** One subscription attribute check toggles the delivery format. No new API actions.

---

## Slice 11 — SNS filter policies

**User outcome:** Developer sets a FilterPolicy on a subscription. Only messages with matching attributes are delivered to that queue.

**What becomes possible:** Per-subscriber message routing. Developer can test attribute-based filtering without real AWS.

**Why minimal:** Adds filter evaluation to the existing delivery path. No new API surface — just a policy evaluator.

---

## Slice 12 — Change message visibility

**User outcome:** Developer extends the visibility timeout of an in-flight message (needs more processing time) or sets it to zero (release it early).

**What becomes possible:** Adaptive processing: consumers request more time for slow work, or nack messages back to the queue.

**Why minimal:** Single action (ChangeMessageVisibility + batch variant). Modifies one field on an existing in-flight message.

---

## Slice 13 — State survives container restart

**User outcome:** Developer enables bbolt persistence, restarts the container, and finds all queues, topics, subscriptions, and messages intact.

**What becomes possible:** Long-running local dev sessions without state loss. Stable local environment across docker-compose restarts.

**Why minimal:** Adds one storage backend behind an existing interface. No new user-facing API.

---

## Slice 14 — Queue and topic housekeeping

**User outcome:** Developer lists all queues, deletes unused ones, inspects topic subscriptions, unsubscribes a queue, and cleans up topics.

**What becomes possible:** Full resource lifecycle management. SDK admin/discovery operations work. CI scripts can enumerate and tear down resources.

**Why minimal:** CRUD over existing entities (ListQueues, DeleteQueue, ListTopics, DeleteTopic, ListSubscriptionsByTopic, Unsubscribe, GetTopicAttributes, SetTopicAttributes, GetSubscriptionAttributes, SetSubscriptionAttributes). No new domain logic — just accessors.

---

## Dependency graph

```
1  Service boots
│
2  Send + receive + delete  ← foundation for everything below
├── 3  Visibility timeout
│   ├── 4  Long polling
│   ├── 6  Dead-letter queue
│   └── 12 Change visibility
├── 5  Delayed messages
├── 7  Batch operations
├── 8  Purge queue
├── 9  SNS fanout (envelope)
│   ├── 10 Raw delivery
│   └── 11 Filter policies
├── 13 Persistence (any slice above)
└── 14 Housekeeping (any slice above)
```

---

## Blocking ambiguities

| #   | Ambiguity                                                                                                                               | Which slices are blocked            |
| --- | --------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| 1   | **FIFO queues** — PRD says "optional, default off" but never defines the trigger or required semantics. Omit entirely, or define scope? | None blocked (omit until clarified) |
| 2   | **`MessageStructure=json`** — "store/ignore or partial support" is not a decidable spec.                                                | Slice 9                             |
| 3   | **DLQ MessageId** — preserve original or generate new? Affects consumer correlation.                                                    | Slice 6                             |
| 4   | **Unresolvable subscription endpoint at publish time** — silent drop or error?                                                          | Slices 9–11                         |
