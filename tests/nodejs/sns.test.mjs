// Node.js SDK v3 integration tests for mokapot SNS→SQS.
// Covers all 3 PRD-required scenarios (Section 17.2).
//
// Run against a live mokapot instance:
//   ENDPOINT=http://localhost:4566 npm test

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  SNSClient,
  CreateTopicCommand,
  DeleteTopicCommand,
  SubscribeCommand,
  PublishCommand,
  SetSubscriptionAttributesCommand,
} from "@aws-sdk/client-sns";
import {
  SQSClient,
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";

const ENDPOINT = process.env.ENDPOINT || "http://localhost:4566";

function newSNSClient() {
  return new SNSClient({
    region: "eu-central-1",
    endpoint: ENDPOINT,
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
}

function newSQSClient() {
  return new SQSClient({
    region: "eu-central-1",
    endpoint: ENDPOINT,
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
}

let counter = 0;
function uniqueName(prefix) {
  return `${prefix}-${Date.now()}-${++counter}`;
}

// Helper: create a queue and return { name, url, arn }.
async function createQueueWithArn(sqsClient, prefix) {
  const name = uniqueName(prefix);
  const out = await sqsClient.send(
    new CreateQueueCommand({ QueueName: name })
  );
  const attrs = await sqsClient.send(
    new GetQueueAttributesCommand({
      QueueUrl: out.QueueUrl,
      AttributeNames: ["QueueArn"],
    })
  );
  return { name, url: out.QueueUrl, arn: attrs.Attributes.QueueArn };
}

// Helper: receive one message from a queue using long-poll.
// SNS→SQS fanout is asynchronous, so we retry with long-poll to avoid flakes.
async function receiveOne(sqsClient, queueUrl, { waitSec = 3, retries = 3 } = {}) {
  for (let i = 0; i < retries; i++) {
    const out = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: waitSec,
      })
    );
    if (out.Messages && out.Messages.length > 0) return out.Messages;
  }
  return [];
}

// Helper: drain a queue and return all messages found.
// Uses short-poll — intended for asserting a queue is empty after delivery settles.
async function drain(sqsClient, queueUrl, { waitSec = 2 } = {}) {
  const out = await sqsClient.send(
    new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: waitSec,
    })
  );
  return out.Messages || [];
}

// ---------------------------------------------------------------------------
// Scenario 1: CreateTopic → Subscribe queue → Publish → message arrives
// ---------------------------------------------------------------------------
describe("SNS Scenario 1: publish fans out to SQS", () => {
  const sns = newSNSClient();
  const sqs = newSQSClient();
  let topicArn, queue;

  before(async () => {
    const topicOut = await sns.send(
      new CreateTopicCommand({ Name: uniqueName("fanout") })
    );
    topicArn = topicOut.TopicArn;

    queue = await createQueueWithArn(sqs, "fanout-q");

    await sns.send(
      new SubscribeCommand({
        TopicArn: topicArn,
        Protocol: "sqs",
        Endpoint: queue.arn,
      })
    );
  });

  after(async () => {
    await sns.send(new DeleteTopicCommand({ TopicArn: topicArn }));
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queue.url }));
  });

  it("delivers a published message to the subscribed queue", async () => {
    const pubOut = await sns.send(
      new PublishCommand({
        TopicArn: topicArn,
        Message: "hello from sns",
        Subject: "Test",
      })
    );
    assert.ok(pubOut.MessageId);

    const msgs = await receiveOne(sqs, queue.url);
    assert.equal(msgs.length, 1);

    // Body should be an SNS JSON envelope
    const envelope = JSON.parse(msgs[0].Body);
    assert.equal(envelope.Type, "Notification");
    assert.equal(envelope.Message, "hello from sns");
    assert.equal(envelope.Subject, "Test");
    assert.equal(envelope.TopicArn, topicArn);
    assert.equal(envelope.MessageId, pubOut.MessageId);
    assert.ok(envelope.Timestamp);
  });
});

// ---------------------------------------------------------------------------
// Scenario 2: RawMessageDelivery on/off changes payload body format
// ---------------------------------------------------------------------------
describe("SNS Scenario 2: raw vs envelope delivery", () => {
  const sns = newSNSClient();
  const sqs = newSQSClient();
  let topicArn, rawQueue, envelopeQueue, rawSubArn;

  before(async () => {
    const topicOut = await sns.send(
      new CreateTopicCommand({ Name: uniqueName("raw") })
    );
    topicArn = topicOut.TopicArn;

    rawQueue = await createQueueWithArn(sqs, "raw-q");
    envelopeQueue = await createQueueWithArn(sqs, "env-q");

    // Subscribe both
    const rawSub = await sns.send(
      new SubscribeCommand({
        TopicArn: topicArn,
        Protocol: "sqs",
        Endpoint: rawQueue.arn,
      })
    );
    rawSubArn = rawSub.SubscriptionArn;

    await sns.send(
      new SubscribeCommand({
        TopicArn: topicArn,
        Protocol: "sqs",
        Endpoint: envelopeQueue.arn,
      })
    );

    // Enable RawMessageDelivery on the raw subscription only
    await sns.send(
      new SetSubscriptionAttributesCommand({
        SubscriptionArn: rawSubArn,
        AttributeName: "RawMessageDelivery",
        AttributeValue: "true",
      })
    );
  });

  after(async () => {
    await sns.send(new DeleteTopicCommand({ TopicArn: topicArn }));
    await sqs.send(new DeleteQueueCommand({ QueueUrl: rawQueue.url }));
    await sqs.send(new DeleteQueueCommand({ QueueUrl: envelopeQueue.url }));
  });

  it("delivers raw body to raw sub and envelope to envelope sub", async () => {
    await sns.send(
      new PublishCommand({
        TopicArn: topicArn,
        Message: "payload text",
      })
    );

    // Raw queue: plain body
    const rawMsgs = await receiveOne(sqs, rawQueue.url);
    assert.equal(rawMsgs.length, 1);
    assert.equal(rawMsgs[0].Body, "payload text");

    // Envelope queue: SNS JSON envelope
    const envMsgs = await receiveOne(sqs, envelopeQueue.url);
    assert.equal(envMsgs.length, 1);
    const envelope = JSON.parse(envMsgs[0].Body);
    assert.equal(envelope.Type, "Notification");
    assert.equal(envelope.Message, "payload text");
  });
});

// ---------------------------------------------------------------------------
// Scenario 3: FilterPolicy blocks/allows delivery based on attributes
// ---------------------------------------------------------------------------
describe("SNS Scenario 3: filter policy", () => {
  const sns = newSNSClient();
  const sqs = newSQSClient();
  let topicArn, queue, subArn;

  before(async () => {
    const topicOut = await sns.send(
      new CreateTopicCommand({ Name: uniqueName("filter") })
    );
    topicArn = topicOut.TopicArn;

    queue = await createQueueWithArn(sqs, "filter-q");

    const sub = await sns.send(
      new SubscribeCommand({
        TopicArn: topicArn,
        Protocol: "sqs",
        Endpoint: queue.arn,
      })
    );
    subArn = sub.SubscriptionArn;

    // Set filter policy: only event_type = "order_placed"
    await sns.send(
      new SetSubscriptionAttributesCommand({
        SubscriptionArn: subArn,
        AttributeName: "FilterPolicy",
        AttributeValue: JSON.stringify({ event_type: ["order_placed"] }),
      })
    );
  });

  after(async () => {
    await sns.send(new DeleteTopicCommand({ TopicArn: topicArn }));
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queue.url }));
  });

  it("delivers matching messages and blocks non-matching", async () => {
    // Publish a matching message
    await sns.send(
      new PublishCommand({
        TopicArn: topicArn,
        Message: "order placed",
        MessageAttributes: {
          event_type: {
            DataType: "String",
            StringValue: "order_placed",
          },
        },
      })
    );

    // Publish a non-matching message
    await sns.send(
      new PublishCommand({
        TopicArn: topicArn,
        Message: "user signed up",
        MessageAttributes: {
          event_type: {
            DataType: "String",
            StringValue: "user_signup",
          },
        },
      })
    );

    // Only the matching message should arrive
    const msgs = await receiveOne(sqs, queue.url);
    assert.equal(msgs.length, 1);

    const envelope = JSON.parse(msgs[0].Body);
    assert.equal(envelope.Message, "order placed");

    // Drain — nothing else should be there.
    // Use a dedicated drain with a wait to let any in-flight messages settle.
    const extra = await drain(sqs, queue.url);
    assert.equal(extra.length, 0);
  });
});
