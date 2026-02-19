# Terminology

**mokapot** — the main Go binary and entrypoint; the local SQS/SNS emulator service

**Queue URL** — `http://{host}:{port}/{accountId}/{queueName}` — the canonical address for an SQS queue

**Queue ARN** — `arn:aws:sqs:{region}:{accountId}:{queueName}` — Amazon Resource Name for a queue

**Topic ARN** — `arn:aws:sns:{region}:{accountId}:{topicName}` — Amazon Resource Name for an SNS topic

**Visibility timeout** — period after ReceiveMessage during which a message is invisible to other consumers; defaults to 30s

**Long polling** — ReceiveMessage blocks up to WaitTimeSeconds when no messages are available, returning immediately when one arrives

**DelaySeconds** — time a sent message is held before becoming available for receive

**RedrivePolicy** — JSON config on a queue specifying `deadLetterTargetArn` and `maxReceiveCount` for DLQ behavior

**DLQ (Dead-letter queue)** — a queue that receives messages exceeding maxReceiveCount on their source queue

**RawMessageDelivery** — SNS subscription attribute; when true, published message body is delivered unwrapped (no SNS JSON envelope)

**FilterPolicy** — JSON on an SNS subscription; only messages with matching attributes are delivered

**SigV4** — AWS Signature Version 4 auth headers; accepted but not validated in local dev mode

**AWS Query API** — the request/response protocol: form-encoded input, XML output with `<ActionNameResponse>` shape

**Slice** — a vertical implementation slice; a small end-to-end user-visible outcome that ships independently

**Receipt handle** — opaque token returned by ReceiveMessage; required for DeleteMessage and ChangeMessageVisibility; regenerated on each receive

**Inflight** — a message currently held by a consumer (after receive, before delete or visibility expiry)

**PurgeQueue** — removes all messages (available, inflight, delayed) from a queue; AWS enforces a 60-second cooldown between purges (PurgeQueueInProgress error)

**bbolt** — embedded key-value store used as optional persistence backend (`DATA_DIR/state.db`)
