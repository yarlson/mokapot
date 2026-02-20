<?php

declare(strict_types=1);

// PHP SDK integration tests for mokapot SNS->SQS.
// Covers all 3 PRD-required scenarios (Section 17.2).
//
// Run against a live mokapot instance:
//   ENDPOINT=http://localhost:4566 vendor/bin/phpunit

use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use PHPUnit\Framework\TestCase;

final class SnsTest extends TestCase
{
    private static int $counter = 0;

    private static function endpoint(): string
    {
        return getenv('ENDPOINT') ?: 'http://localhost:4566';
    }

    private static function uniqueName(string $prefix): string
    {
        return sprintf('%s-%d-%d', $prefix, time(), ++self::$counter);
    }

    private static function newSnsClient(): SnsClient
    {
        return new SnsClient([
            'region' => 'eu-central-1',
            'endpoint' => self::endpoint(),
            'credentials' => [
                'key' => 'test',
                'secret' => 'test',
            ],
            'version' => 'latest',
        ]);
    }

    private static function newSqsClient(): SqsClient
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

    private static function createQueueWithArn(SqsClient $client, string $prefix): array
    {
        $name = self::uniqueName($prefix);
        $result = $client->createQueue(['QueueName' => $name]);
        $attrs = $client->getQueueAttributes([
            'QueueUrl' => $result['QueueUrl'],
            'AttributeNames' => ['QueueArn'],
        ]);
        return [
            'name' => $name,
            'url' => $result['QueueUrl'],
            'arn' => $attrs['Attributes']['QueueArn'],
        ];
    }

    /**
     * Receive one message from a queue using long-poll with retries.
     * SNS->SQS fanout is synchronous in mokapot, but we retry for robustness.
     */
    private static function receiveOne(SqsClient $client, string $queueUrl, int $waitSec = 3, int $retries = 3): array
    {
        for ($i = 0; $i < $retries; $i++) {
            $result = $client->receiveMessage([
                'QueueUrl' => $queueUrl,
                'MaxNumberOfMessages' => 1,
                'WaitTimeSeconds' => $waitSec,
            ]);
            $messages = $result['Messages'] ?? [];
            if (count($messages) > 0) {
                return $messages;
            }
        }
        return [];
    }

    /**
     * Drain a queue and return all messages found.
     */
    private static function drain(SqsClient $client, string $queueUrl, int $waitSec = 2): array
    {
        $result = $client->receiveMessage([
            'QueueUrl' => $queueUrl,
            'MaxNumberOfMessages' => 10,
            'WaitTimeSeconds' => $waitSec,
        ]);
        return $result['Messages'] ?? [];
    }

    // -----------------------------------------------------------------------
    // Scenario 1: CreateTopic -> Subscribe queue -> Publish -> message arrives
    // -----------------------------------------------------------------------
    public function testPublishFansOutToSqs(): void
    {
        $sns = self::newSnsClient();
        $sqs = self::newSqsClient();

        $topicResult = $sns->createTopic(['Name' => self::uniqueName('fanout')]);
        $topicArn = $topicResult['TopicArn'];
        $queue = self::createQueueWithArn($sqs, 'fanout-q');

        try {
            $sns->subscribe([
                'TopicArn' => $topicArn,
                'Protocol' => 'sqs',
                'Endpoint' => $queue['arn'],
            ]);

            $pubResult = $sns->publish([
                'TopicArn' => $topicArn,
                'Message' => 'hello from sns',
                'Subject' => 'Test',
            ]);
            $this->assertNotEmpty($pubResult['MessageId']);

            $messages = self::receiveOne($sqs, $queue['url']);
            $this->assertCount(1, $messages);

            // Body should be an SNS JSON envelope
            $envelope = json_decode($messages[0]['Body'], true);
            $this->assertSame('Notification', $envelope['Type']);
            $this->assertSame('hello from sns', $envelope['Message']);
            $this->assertSame('Test', $envelope['Subject']);
            $this->assertSame($topicArn, $envelope['TopicArn']);
            $this->assertSame($pubResult['MessageId'], $envelope['MessageId']);
            $this->assertNotEmpty($envelope['Timestamp']);
        } finally {
            $sns->deleteTopic(['TopicArn' => $topicArn]);
            $sqs->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 2: RawMessageDelivery on/off changes payload body format
    // -----------------------------------------------------------------------
    public function testRawVsEnvelopeDelivery(): void
    {
        $sns = self::newSnsClient();
        $sqs = self::newSqsClient();

        $topicResult = $sns->createTopic(['Name' => self::uniqueName('raw')]);
        $topicArn = $topicResult['TopicArn'];
        $rawQueue = self::createQueueWithArn($sqs, 'raw-q');
        $envelopeQueue = self::createQueueWithArn($sqs, 'env-q');

        try {
            // Subscribe both queues
            $rawSub = $sns->subscribe([
                'TopicArn' => $topicArn,
                'Protocol' => 'sqs',
                'Endpoint' => $rawQueue['arn'],
            ]);
            $rawSubArn = $rawSub['SubscriptionArn'];

            $sns->subscribe([
                'TopicArn' => $topicArn,
                'Protocol' => 'sqs',
                'Endpoint' => $envelopeQueue['arn'],
            ]);

            // Enable RawMessageDelivery on the raw subscription only
            $sns->setSubscriptionAttributes([
                'SubscriptionArn' => $rawSubArn,
                'AttributeName' => 'RawMessageDelivery',
                'AttributeValue' => 'true',
            ]);

            $sns->publish([
                'TopicArn' => $topicArn,
                'Message' => 'payload text',
            ]);

            // Raw queue: plain body
            $rawMessages = self::receiveOne($sqs, $rawQueue['url']);
            $this->assertCount(1, $rawMessages);
            $this->assertSame('payload text', $rawMessages[0]['Body']);

            // Envelope queue: SNS JSON envelope
            $envMessages = self::receiveOne($sqs, $envelopeQueue['url']);
            $this->assertCount(1, $envMessages);
            $envelope = json_decode($envMessages[0]['Body'], true);
            $this->assertSame('Notification', $envelope['Type']);
            $this->assertSame('payload text', $envelope['Message']);
        } finally {
            $sns->deleteTopic(['TopicArn' => $topicArn]);
            $sqs->deleteQueue(['QueueUrl' => $rawQueue['url']]);
            $sqs->deleteQueue(['QueueUrl' => $envelopeQueue['url']]);
        }
    }

    // -----------------------------------------------------------------------
    // Scenario 3: FilterPolicy blocks/allows delivery based on attributes
    // -----------------------------------------------------------------------
    public function testFilterPolicy(): void
    {
        $sns = self::newSnsClient();
        $sqs = self::newSqsClient();

        $topicResult = $sns->createTopic(['Name' => self::uniqueName('filter')]);
        $topicArn = $topicResult['TopicArn'];
        $queue = self::createQueueWithArn($sqs, 'filter-q');

        try {
            $sub = $sns->subscribe([
                'TopicArn' => $topicArn,
                'Protocol' => 'sqs',
                'Endpoint' => $queue['arn'],
            ]);
            $subArn = $sub['SubscriptionArn'];

            // Set filter policy: only event_type = "order_placed"
            $sns->setSubscriptionAttributes([
                'SubscriptionArn' => $subArn,
                'AttributeName' => 'FilterPolicy',
                'AttributeValue' => json_encode(['event_type' => ['order_placed']]),
            ]);

            // Publish a matching message
            $sns->publish([
                'TopicArn' => $topicArn,
                'Message' => 'order placed',
                'MessageAttributes' => [
                    'event_type' => [
                        'DataType' => 'String',
                        'StringValue' => 'order_placed',
                    ],
                ],
            ]);

            // Publish a non-matching message
            $sns->publish([
                'TopicArn' => $topicArn,
                'Message' => 'user signed up',
                'MessageAttributes' => [
                    'event_type' => [
                        'DataType' => 'String',
                        'StringValue' => 'user_signup',
                    ],
                ],
            ]);

            // Only the matching message should arrive
            $messages = self::receiveOne($sqs, $queue['url']);
            $this->assertCount(1, $messages);

            $envelope = json_decode($messages[0]['Body'], true);
            $this->assertSame('order placed', $envelope['Message']);

            // Drain — nothing else should be there
            $extra = self::drain($sqs, $queue['url']);
            $this->assertCount(0, $extra);
        } finally {
            $sns->deleteTopic(['TopicArn' => $topicArn]);
            $sqs->deleteQueue(['QueueUrl' => $queue['url']]);
        }
    }
}
