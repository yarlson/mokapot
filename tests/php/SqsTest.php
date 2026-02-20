<?php

declare(strict_types=1);

// PHP SDK integration tests for mokapot SQS.
// Covers all 6 PRD-required scenarios (Section 17.2).
//
// Run against a live mokapot instance:
//   ENDPOINT=http://localhost:4566 vendor/bin/phpunit

use Aws\Sqs\SqsClient;
use PHPUnit\Framework\TestCase;

final class SqsTest extends TestCase
{
    private static int $counter = 0;

    private static function endpoint(): string
    {
        return getenv('ENDPOINT') ?: 'http://localhost:4566';
    }

    private static function newClient(): SqsClient
    {
        return new SqsClient([
            'region' => 'eu-central-1',
            'endpoint' => self::endpoint(),
            'credentials' => [
                'key' => 'test',
                'secret' => 'test',
            ],
            'version' => 'latest',
        ]);
    }

    private static function createQueue(SqsClient $client, string $prefix = 'test'): array
    {
        $name = sprintf('%s-%d-%d', $prefix, time(), ++self::$counter);
        $result = $client->createQueue(['QueueName' => $name]);
        return ['name' => $name, 'url' => $result['QueueUrl']];
    }

    private static function receiveOne(SqsClient $client, string $queueUrl, array $opts = []): array
    {
        $result = $client->receiveMessage(array_merge([
            'QueueUrl' => $queueUrl,
            'MaxNumberOfMessages' => 1,
            'WaitTimeSeconds' => 0,
        ], $opts));
        return $result['Messages'] ?? [];
    }

    // -----------------------------------------------------------------------
    // Scenario 1: CreateQueue -> SendMessage -> ReceiveMessage -> DeleteMessage
    // -----------------------------------------------------------------------
    public function testFullMessageLifecycle(): void
    {
        $client = self::newClient();
        $queue = self::createQueue($client, 'lifecycle');

        try {
            // Send
            $sendResult = $client->sendMessage([
                'QueueUrl' => $queue['url'],
                'MessageBody' => 'hello from php',
            ]);
            $this->assertNotEmpty($sendResult['MessageId']);
            $this->assertNotEmpty($sendResult['MD5OfMessageBody']);

            // Receive
            $messages = self::receiveOne($client, $queue['url']);
            $this->assertCount(1, $messages);
            $this->assertSame('hello from php', $messages[0]['Body']);
            $this->assertSame($sendResult['MessageId'], $messages[0]['MessageId']);
            $this->assertNotEmpty($messages[0]['ReceiptHandle']);

            // Delete
            $client->deleteMessage([
                'QueueUrl' => $queue['url'],
                'ReceiptHandle' => $messages[0]['ReceiptHandle'],
            ]);

            // Confirm queue is empty
            $after = self::receiveOne($client, $queue['url']);
            $this->assertCount(0, $after);
        } finally {
            $client->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: Message reappears after visibility timeout
    // -----------------------------------------------------------------------
    public function testVisibilityTimeoutReappearance(): void
    {
        $client = self::newClient();
        $queue = self::createQueue($client, 'vis');

        try {
            $client->sendMessage([
                'QueueUrl' => $queue['url'],
                'MessageBody' => 'reappear me',
            ]);

            // Receive with 1-second visibility timeout
            $first = self::receiveOne($client, $queue['url'], [
                'VisibilityTimeout' => 1,
            ]);
            $this->assertCount(1, $first);
            $this->assertSame('reappear me', $first[0]['Body']);

            // Immediately: message should be invisible
            $hidden = self::receiveOne($client, $queue['url']);
            $this->assertCount(0, $hidden);

            // Wait for visibility timeout to expire
            sleep(2);

            // Message should reappear
            $reappeared = self::receiveOne($client, $queue['url']);
            $this->assertCount(1, $reappeared);
            $this->assertSame('reappear me', $reappeared[0]['Body']);
        } finally {
            $client->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: Long polling
    // -----------------------------------------------------------------------
    public function testLongPolling(): void
    {
        $client = self::newClient();
        $queue = self::createQueue($client, 'longpoll');

        try {
            // Long-poll on empty queue should block for the specified duration
            $start = microtime(true);
            $result = $client->receiveMessage([
                'QueueUrl' => $queue['url'],
                'MaxNumberOfMessages' => 1,
                'WaitTimeSeconds' => 1,
            ]);
            $elapsedMs = (microtime(true) - $start) * 1000;

            $this->assertEmpty($result['Messages'] ?? []);
            $this->assertGreaterThan(800, $elapsedMs, "Should block for ~1s");
            $this->assertLessThan(3000, $elapsedMs, "Should not exceed 3s");

            // Long-poll with a message already queued should return immediately
            $client->sendMessage([
                'QueueUrl' => $queue['url'],
                'MessageBody' => 'wake up',
            ]);

            $start = microtime(true);
            $result = $client->receiveMessage([
                'QueueUrl' => $queue['url'],
                'MaxNumberOfMessages' => 1,
                'WaitTimeSeconds' => 5,
            ]);
            $elapsedMs = (microtime(true) - $start) * 1000;

            $messages = $result['Messages'] ?? [];
            $this->assertCount(1, $messages);
            $this->assertSame('wake up', $messages[0]['Body']);
            $this->assertLessThan(2000, $elapsedMs, "Should return promptly when message exists");
        } finally {
            $client->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 4: DelaySeconds
    // -----------------------------------------------------------------------
    public function testDelayedMessages(): void
    {
        $client = self::newClient();
        $queue = self::createQueue($client, 'delay');

        try {
            $client->sendMessage([
                'QueueUrl' => $queue['url'],
                'MessageBody' => 'delayed msg',
                'DelaySeconds' => 2,
            ]);

            // Immediately: should not be receivable
            $early = self::receiveOne($client, $queue['url']);
            $this->assertCount(0, $early);

            // Wait for delay to elapse
            sleep(3);

            $delayed = self::receiveOne($client, $queue['url']);
            $this->assertCount(1, $delayed);
            $this->assertSame('delayed msg', $delayed[0]['Body']);
        } finally {
            $client->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 5: Batch send + batch delete
    // -----------------------------------------------------------------------
    public function testBatchOperations(): void
    {
        $client = self::newClient();
        $queue = self::createQueue($client, 'batch');

        try {
            // Batch send 5 messages
            $entries = [];
            for ($i = 0; $i < 5; $i++) {
                $entries[] = [
                    'Id' => "msg-{$i}",
                    'MessageBody' => "batch body {$i}",
                ];
            }

            $sendResult = $client->sendMessageBatch([
                'QueueUrl' => $queue['url'],
                'Entries' => $entries,
            ]);
            $this->assertCount(5, $sendResult['Successful']);
            $this->assertCount(0, $sendResult['Failed'] ?? []);

            // Receive all messages
            $received = [];
            for ($attempt = 0; $attempt < 3 && count($received) < 5; $attempt++) {
                $result = $client->receiveMessage([
                    'QueueUrl' => $queue['url'],
                    'MaxNumberOfMessages' => 10,
                    'WaitTimeSeconds' => 0,
                ]);
                if (isset($result['Messages'])) {
                    array_push($received, ...$result['Messages']);
                }
            }
            $this->assertCount(5, $received);

            // Batch delete all received messages
            $deleteEntries = [];
            foreach ($received as $i => $msg) {
                $deleteEntries[] = [
                    'Id' => "del-{$i}",
                    'ReceiptHandle' => $msg['ReceiptHandle'],
                ];
            }

            $delResult = $client->deleteMessageBatch([
                'QueueUrl' => $queue['url'],
                'Entries' => $deleteEntries,
            ]);
            $this->assertCount(5, $delResult['Successful']);
            $this->assertCount(0, $delResult['Failed'] ?? []);

            // Queue should be empty
            $empty = self::receiveOne($client, $queue['url']);
            $this->assertCount(0, $empty);
        } finally {
            $client->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }

    public function testBatchDeletePartialFailure(): void
    {
        $client = self::newClient();
        $queue = self::createQueue($client, 'batch-pf');

        try {
            // Send and receive a message to get a valid receipt handle
            $client->sendMessage([
                'QueueUrl' => $queue['url'],
                'MessageBody' => 'partial-fail-msg',
            ]);
            $messages = self::receiveOne($client, $queue['url']);
            $this->assertCount(1, $messages);

            // Delete batch: one valid handle, one bogus
            $delResult = $client->deleteMessageBatch([
                'QueueUrl' => $queue['url'],
                'Entries' => [
                    ['Id' => 'good', 'ReceiptHandle' => $messages[0]['ReceiptHandle']],
                    ['Id' => 'bad', 'ReceiptHandle' => 'bogus-handle'],
                ],
            ]);

            // Valid entry succeeds, invalid entry fails
            $this->assertCount(1, $delResult['Successful']);
            $this->assertSame('good', $delResult['Successful'][0]['Id']);
            $this->assertCount(1, $delResult['Failed']);
            $this->assertSame('bad', $delResult['Failed'][0]['Id']);
            $this->assertSame('ReceiptHandleIsInvalid', $delResult['Failed'][0]['Code']);
            $this->assertTrue($delResult['Failed'][0]['SenderFault']);
        } finally {
            $client->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 6: DLQ — message moves after maxReceiveCount
    // -----------------------------------------------------------------------
    public function testDeadLetterQueue(): void
    {
        $client = self::newClient();
        $dlq = self::createQueue($client, 'dlq');
        $source = self::createQueue($client, 'dlq-source');

        try {
            // Get DLQ ARN
            $dlqAttrs = $client->getQueueAttributes([
                'QueueUrl' => $dlq['url'],
                'AttributeNames' => ['QueueArn'],
            ]);
            $dlqArn = $dlqAttrs['Attributes']['QueueArn'];

            // Set RedrivePolicy (maxReceiveCount = 2)
            $client->setQueueAttributes([
                'QueueUrl' => $source['url'],
                'Attributes' => [
                    'RedrivePolicy' => json_encode([
                        'deadLetterTargetArn' => $dlqArn,
                        'maxReceiveCount' => 2,
                    ]),
                ],
            ]);

            // Send a poison message
            $client->sendMessage([
                'QueueUrl' => $source['url'],
                'MessageBody' => 'poison pill',
            ]);

            // Receive twice without deleting (visibility timeout = 1s)
            for ($i = 0; $i < 2; $i++) {
                $messages = self::receiveOne($client, $source['url'], [
                    'VisibilityTimeout' => 1,
                ]);
                $this->assertCount(1, $messages, "receive attempt " . ($i + 1));
                $this->assertSame('poison pill', $messages[0]['Body']);

                // Wait for visibility timeout to expire
                sleep(2);
            }

            // Third receive from source should be empty (message moved to DLQ)
            $empty = self::receiveOne($client, $source['url']);
            $this->assertCount(0, $empty, 'source queue should be empty');

            // DLQ should have the message
            $dlqMessages = self::receiveOne($client, $dlq['url']);
            $this->assertCount(1, $dlqMessages, 'DLQ should have the message');
            $this->assertSame('poison pill', $dlqMessages[0]['Body']);
        } finally {
            $client->deleteQueue(['QueueUrl' => $source['url']]);
            $client->deleteQueue(['QueueUrl' => $dlq['url']]);
        }
    }
}
