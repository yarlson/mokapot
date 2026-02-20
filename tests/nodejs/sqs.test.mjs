// Node.js SDK v3 integration tests for mokapot SQS.
// Covers all 6 PRD-required scenarios (Section 17.2).
//
// Run against a live mokapot instance:
//   ENDPOINT=http://localhost:4566 npm test

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  SQSClient,
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
  SendMessageCommand,
  SendMessageBatchCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  DeleteMessageBatchCommand,
} from "@aws-sdk/client-sqs";

const ENDPOINT = process.env.ENDPOINT || "http://localhost:4566";

function newClient() {
  return new SQSClient({
    region: "eu-central-1",
    endpoint: ENDPOINT,
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
}

// Helper: create a uniquely-named queue and return its URL.
let queueCounter = 0;
async function createQueue(client, prefix = "test") {
  const name = `${prefix}-${Date.now()}-${++queueCounter}`;
  const out = await client.send(new CreateQueueCommand({ QueueName: name }));
  return { name, url: out.QueueUrl };
}

// Helper: receive with short-poll (WaitTimeSeconds = 0).
async function receiveOne(client, queueUrl, opts = {}) {
  const out = await client.send(
    new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 0,
      ...opts,
    })
  );
  return out.Messages || [];
}

// ---------------------------------------------------------------------------
// Scenario 1: CreateQueue → SendMessage → ReceiveMessage → DeleteMessage
// ---------------------------------------------------------------------------
describe("SQS Scenario 1: full message lifecycle", () => {
  const client = newClient();
  let queueUrl;

  before(async () => {
    ({ url: queueUrl } = await createQueue(client, "lifecycle"));
  });

  after(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  });

  it("sends, receives, and deletes a message", async () => {
    // Send
    const sendOut = await client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: "hello from node",
      })
    );
    assert.ok(sendOut.MessageId);
    assert.ok(sendOut.MD5OfMessageBody);

    // Receive
    const msgs = await receiveOne(client, queueUrl);
    assert.equal(msgs.length, 1);
    assert.equal(msgs[0].Body, "hello from node");
    assert.equal(msgs[0].MessageId, sendOut.MessageId);
    assert.ok(msgs[0].ReceiptHandle);

    // Delete
    await client.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: msgs[0].ReceiptHandle,
      })
    );

    // Confirm queue is empty
    const after = await receiveOne(client, queueUrl);
    assert.equal(after.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Scenario 2: message reappears after visibility timeout
// ---------------------------------------------------------------------------
describe("SQS Scenario 2: visibility timeout reappearance", () => {
  const client = newClient();
  let queueUrl;

  before(async () => {
    ({ url: queueUrl } = await createQueue(client, "vis"));
  });

  after(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  });

  it("reappears after visibility timeout expires", async () => {
    await client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: "reappear me",
      })
    );

    // Receive with 1-second visibility timeout
    const first = await receiveOne(client, queueUrl, {
      VisibilityTimeout: 1,
    });
    assert.equal(first.length, 1);
    assert.equal(first[0].Body, "reappear me");

    // Immediately: message should be invisible
    const hidden = await receiveOne(client, queueUrl);
    assert.equal(hidden.length, 0);

    // Wait for visibility timeout to expire
    await new Promise((r) => setTimeout(r, 1500));

    // Message should reappear
    const reappeared = await receiveOne(client, queueUrl);
    assert.equal(reappeared.length, 1);
    assert.equal(reappeared[0].Body, "reappear me");
  });
});

// ---------------------------------------------------------------------------
// Scenario 3: long polling
// ---------------------------------------------------------------------------
describe("SQS Scenario 3: long polling", () => {
  const client = newClient();
  let queueUrl;

  before(async () => {
    ({ url: queueUrl } = await createQueue(client, "longpoll"));
  });

  after(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  });

  it("blocks then returns promptly when a message arrives", async () => {
    const startMs = Date.now();

    // Start long-poll in background (5s timeout)
    const pollPromise = client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      })
    );

    // Send a message after a short delay
    await new Promise((r) => setTimeout(r, 500));
    await client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: "wake up",
      })
    );

    const result = await pollPromise;
    const elapsedMs = Date.now() - startMs;

    assert.ok(result.Messages && result.Messages.length === 1);
    assert.equal(result.Messages[0].Body, "wake up");
    // Should return well before the 5s timeout
    assert.ok(elapsedMs < 4000, `took ${elapsedMs}ms, expected < 4000ms`);
  });
});

// ---------------------------------------------------------------------------
// Scenario 4: DelaySeconds
// ---------------------------------------------------------------------------
describe("SQS Scenario 4: delayed messages", () => {
  const client = newClient();
  let queueUrl;

  before(async () => {
    ({ url: queueUrl } = await createQueue(client, "delay"));
  });

  after(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  });

  it("message is not receivable before delay elapses", async () => {
    await client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: "delayed msg",
        DelaySeconds: 2,
      })
    );

    // Immediately: should not be receivable
    const early = await receiveOne(client, queueUrl);
    assert.equal(early.length, 0);

    // Wait for delay to elapse
    await new Promise((r) => setTimeout(r, 2500));

    const delayed = await receiveOne(client, queueUrl);
    assert.equal(delayed.length, 1);
    assert.equal(delayed[0].Body, "delayed msg");
  });
});

// ---------------------------------------------------------------------------
// Scenario 5: batch send + batch delete
// ---------------------------------------------------------------------------
describe("SQS Scenario 5: batch operations", () => {
  const client = newClient();
  let queueUrl;

  before(async () => {
    ({ url: queueUrl } = await createQueue(client, "batch"));
  });

  after(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  });

  it("sends a batch and deletes a batch", async () => {
    // Batch send 5 messages
    const entries = Array.from({ length: 5 }, (_, i) => ({
      Id: `msg-${i}`,
      MessageBody: `batch body ${i}`,
    }));

    const sendOut = await client.send(
      new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: entries,
      })
    );
    assert.equal(sendOut.Successful.length, 5);
    assert.equal((sendOut.Failed || []).length, 0);

    // Receive all messages (may need multiple calls since max is 10)
    const received = [];
    for (let attempt = 0; attempt < 3 && received.length < 5; attempt++) {
      const out = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
        })
      );
      if (out.Messages) received.push(...out.Messages);
    }
    assert.equal(received.length, 5);

    // Batch delete all received messages
    const deleteEntries = received.map((m, i) => ({
      Id: `del-${i}`,
      ReceiptHandle: m.ReceiptHandle,
    }));

    const delOut = await client.send(
      new DeleteMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: deleteEntries,
      })
    );
    assert.equal(delOut.Successful.length, 5);
    assert.equal((delOut.Failed || []).length, 0);

    // Queue should be empty
    const empty = await receiveOne(client, queueUrl);
    assert.equal(empty.length, 0);
  });

  it("reports partial failure in delete batch", async () => {
    // Send and receive a message to get a valid receipt handle
    await client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: "partial-fail-msg",
      })
    );
    const msgs = await receiveOne(client, queueUrl);
    assert.equal(msgs.length, 1);

    // Delete batch: one valid handle, one bogus
    const delOut = await client.send(
      new DeleteMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: [
          { Id: "good", ReceiptHandle: msgs[0].ReceiptHandle },
          { Id: "bad", ReceiptHandle: "bogus-handle" },
        ],
      })
    );

    // Valid entry succeeds, invalid entry fails
    assert.equal(delOut.Successful.length, 1);
    assert.equal(delOut.Successful[0].Id, "good");
    assert.equal(delOut.Failed.length, 1);
    assert.equal(delOut.Failed[0].Id, "bad");
    assert.equal(delOut.Failed[0].Code, "ReceiptHandleIsInvalid");
    assert.equal(delOut.Failed[0].SenderFault, true);
  });
});

// ---------------------------------------------------------------------------
// Scenario 6: DLQ — message moves after maxReceiveCount
// ---------------------------------------------------------------------------
describe("SQS Scenario 6: dead-letter queue", () => {
  const client = newClient();
  let sourceUrl;
  let dlqUrl;

  before(async () => {
    // Create DLQ first
    ({ url: dlqUrl } = await createQueue(client, "dlq"));

    // Get DLQ ARN
    const dlqAttrs = await client.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlqUrl,
        AttributeNames: ["QueueArn"],
      })
    );
    const dlqArn = dlqAttrs.Attributes.QueueArn;

    // Create source queue
    ({ url: sourceUrl } = await createQueue(client, "dlq-source"));

    // Set RedrivePolicy (maxReceiveCount = 2)
    await client.send(
      new SetQueueAttributesCommand({
        QueueUrl: sourceUrl,
        Attributes: {
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: dlqArn,
            maxReceiveCount: 2,
          }),
        },
      })
    );
  });

  after(async () => {
    await client.send(new DeleteQueueCommand({ QueueUrl: sourceUrl }));
    await client.send(new DeleteQueueCommand({ QueueUrl: dlqUrl }));
  });

  it("moves message to DLQ after maxReceiveCount", async () => {
    // Send a poison message
    await client.send(
      new SendMessageCommand({
        QueueUrl: sourceUrl,
        MessageBody: "poison pill",
      })
    );

    // Receive twice without deleting (visibility timeout = 1s)
    for (let i = 0; i < 2; i++) {
      const msgs = await receiveOne(client, sourceUrl, {
        VisibilityTimeout: 1,
      });
      assert.equal(msgs.length, 1, `receive attempt ${i + 1}`);
      assert.equal(msgs[0].Body, "poison pill");

      // Wait for visibility timeout to expire
      await new Promise((r) => setTimeout(r, 1500));
    }

    // Third receive from source should be empty (message moved to DLQ)
    const empty = await receiveOne(client, sourceUrl);
    assert.equal(empty.length, 0, "source queue should be empty");

    // DLQ should have the message
    const dlqMsgs = await receiveOne(client, dlqUrl);
    assert.equal(dlqMsgs.length, 1, "DLQ should have the message");
    assert.equal(dlqMsgs[0].Body, "poison pill");
  });
});
