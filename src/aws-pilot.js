// SPDX-License-Identifier: Apache-2.0
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { STSClient, GetCallerIdentityCommand } from '@aws-sdk/client-sts';
import {
  KinesisClient, CreateStreamCommand, DescribeStreamSummaryCommand,
  ListShardsCommand, PutRecordsCommand, GetShardIteratorCommand,
  GetRecordsCommand, DeleteStreamCommand
} from '@aws-sdk/client-kinesis';
import {
  SQSClient, CreateQueueCommand, SendMessageBatchCommand, ReceiveMessageCommand,
  DeleteMessageBatchCommand, DeleteQueueCommand
} from '@aws-sdk/client-sqs';

const REGION = /^[a-z]{2}(?:-gov)?-[a-z]+-\d$/;
const ACCOUNT = /^\d{12}$/;

function number(value, fallback, minimum, maximum, label) {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isFinite(parsed) || parsed < minimum || parsed > maximum) throw new Error(`${label} must be between ${minimum} and ${maximum}.`);
  return parsed;
}

export function validateAwsPilotOptions(value = {}) {
  const region = value.region;
  if (!REGION.test(region ?? '')) throw new Error('Set an AWS Region with --region or AWS_REGION.');
  const events = Math.floor(number(value.events, 5, 1, 100, 'events'));
  const maxCostUsd = number(value.maxCostUsd, 1, 0.01, 1, 'max-cost-usd');
  const profile = value.profile || 'default';
  if (!/^[\w+=,.@-]{1,128}$/.test(profile)) throw new Error('AWS profile contains unsupported characters.');
  return { region, events, maxCostUsd, profile, outputDirectory: resolve(value.outputDirectory ?? '.streamplay/aws') };
}

export function buildAwsPilotPlan({ account, ...input }) {
  if (!ACCOUNT.test(account ?? '')) throw new Error('AWS returned an invalid account ID.');
  const options = validateAwsPilotOptions(input);
  const suffix = (input.suffix ?? randomBytes(5).toString('hex')).toLowerCase();
  if (!/^[a-f0-9]{10}$/.test(suffix)) throw new Error('Pilot suffix must be ten lowercase hexadecimal characters.');
  const prefix = `streamplay-pilot-${suffix}`;
  return {
    version: 1,
    account,
    profile: options.profile,
    region: options.region,
    events: options.events,
    maxCostUsd: options.maxCostUsd,
    resources: { kinesisStream: prefix, sqsQueue: prefix },
    limits: { kinesisShards: 1, maximumEvents: 100, retentionHours: 24 },
    costStatement: 'The client bounds resources and events but cannot enforce AWS billing. Verify regional pricing and cleanup evidence.',
    proves: ['AWS identity resolved', 'Kinesis accepted and returned synthetic records', 'SQS accepted and returned synthetic messages', 'owned resources were deleted'],
    doesNotProve: ['application processing', 'Flink execution', 'end-to-end delivery', 'production capacity or resilience']
  };
}

function config(region) { return { region, maxAttempts: 3 }; }
function timed(client, command, timeoutMs = 15000) { return client.send(command, { abortSignal: AbortSignal.timeout(timeoutMs) }); }

export async function discoverAwsIdentity({ region, sts = new STSClient(config(region)) }) {
  const result = await timed(sts, new GetCallerIdentityCommand({}));
  if (!ACCOUNT.test(result.Account ?? '') || !result.Arn) throw new Error('AWS did not return a complete caller identity.');
  return { account: result.Account, arn: result.Arn, userId: result.UserId };
}

async function waitForStream(kinesis, streamName) {
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    try {
      const result = await timed(kinesis, new DescribeStreamSummaryCommand({ StreamName: streamName }));
      if (result.StreamDescriptionSummary?.StreamStatus === 'ACTIVE') return result.StreamDescriptionSummary;
    } catch (error) {
      if (error.name !== 'ResourceNotFoundException') throw error;
    }
    await delay(1500);
  }
  throw new Error(`Kinesis stream ${streamName} did not become active within two minutes.`);
}

async function readKinesis(kinesis, streamName, expected) {
  const listed = await timed(kinesis, new ListShardsCommand({ StreamName: streamName }));
  if (listed.Shards?.length !== 1) throw new Error(`Expected one Kinesis shard; found ${listed.Shards?.length ?? 0}.`);
  let iterator = (await timed(kinesis, new GetShardIteratorCommand({ StreamName: streamName, ShardId: listed.Shards[0].ShardId, ShardIteratorType: 'TRIM_HORIZON' }))).ShardIterator;
  const records = [];
  const deadline = Date.now() + 30000;
  while (iterator && records.length < expected && Date.now() < deadline) {
    const result = await timed(kinesis, new GetRecordsCommand({ ShardIterator: iterator, Limit: expected }));
    iterator = result.NextShardIterator;
    for (const record of result.Records ?? []) records.push(JSON.parse(Buffer.from(record.Data).toString('utf8')));
    if (records.length < expected) await delay(1000);
  }
  return records;
}

async function readSqs(sqs, queueUrl, expected) {
  const messages = [];
  const deadline = Date.now() + 30000;
  while (messages.length < expected && Date.now() < deadline) {
    const result = await timed(sqs, new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: Math.min(10, expected - messages.length), WaitTimeSeconds: 2, VisibilityTimeout: 15 }), 10000);
    if (result.Messages?.length) {
      messages.push(...result.Messages.map(message => ({ ...JSON.parse(message.Body), messageId: message.MessageId })));
      await timed(sqs, new DeleteMessageBatchCommand({ QueueUrl: queueUrl, Entries: result.Messages.map((message, index) => ({ Id: String(index), ReceiptHandle: message.ReceiptHandle })) }));
    }
  }
  return messages;
}

export async function runAwsPilot(plan, {
  kinesis = new KinesisClient(config(plan.region)),
  sqs = new SQSClient(config(plan.region)),
  outputDirectory = '.streamplay/aws',
  now = () => new Date(),
  onProgress = () => {}
} = {}) {
  if (!plan?.resources || plan.limits?.kinesisShards !== 1 || plan.events > 100) throw new Error('Refusing an invalid or unbounded AWS pilot plan.');
  const runId = randomUUID();
  const startedAt = now().toISOString();
  const evidence = { ...plan, runId, startedAt, status: 'running', checks: [], cleanup: {}, errors: [] };
  let queueUrl;
  let streamCreated = false;
  try {
    onProgress(`Creating isolated Kinesis stream ${plan.resources.kinesisStream}`);
    await timed(kinesis, new CreateStreamCommand({ StreamName: plan.resources.kinesisStream, ShardCount: 1, Tags: { Project: 'StreamPlay', Purpose: 'isolated-smoke-test', RunId: runId } }));
    streamCreated = true;
    await waitForStream(kinesis, plan.resources.kinesisStream);
    evidence.checks.push({ name: 'kinesis-active', status: 'passed' });

    onProgress(`Creating isolated SQS queue ${plan.resources.sqsQueue}`);
    queueUrl = (await timed(sqs, new CreateQueueCommand({ QueueName: plan.resources.sqsQueue, Tags: { Project: 'StreamPlay', Purpose: 'isolated-smoke-test', RunId: runId }, Attributes: { MessageRetentionPeriod: '3600' } }))).QueueUrl;
    if (!queueUrl) throw new Error('SQS did not return a queue URL.');
    evidence.checks.push({ name: 'sqs-created', status: 'passed' });

    const payloads = Array.from({ length: plan.events }, (_, index) => ({ event_id: `${runId}:${index + 1}`, run_id: runId, sequence: index + 1, synthetic: true, timestamp: now().toISOString() }));
    const written = await timed(kinesis, new PutRecordsCommand({ StreamName: plan.resources.kinesisStream, Records: payloads.map(value => ({ PartitionKey: value.event_id, Data: Buffer.from(JSON.stringify(value)) })) }));
    if (written.FailedRecordCount) throw new Error(`Kinesis rejected ${written.FailedRecordCount} record(s).`);
    const kinesisRecords = await readKinesis(kinesis, plan.resources.kinesisStream, payloads.length);
    if (new Set(kinesisRecords.map(value => value.event_id)).size !== payloads.length) throw new Error(`Kinesis returned ${kinesisRecords.length}/${payloads.length} expected records.`);
    evidence.checks.push({ name: 'kinesis-round-trip', status: 'passed', records: kinesisRecords.length });

    for (let offset = 0; offset < payloads.length; offset += 10) {
      const batch = payloads.slice(offset, offset + 10);
      const result = await timed(sqs, new SendMessageBatchCommand({ QueueUrl: queueUrl, Entries: batch.map((value, index) => ({ Id: String(index), MessageBody: JSON.stringify(value) })) }));
      if (result.Failed?.length) throw new Error(`SQS rejected ${result.Failed.length} message(s).`);
    }
    const sqsMessages = await readSqs(sqs, queueUrl, payloads.length);
    if (new Set(sqsMessages.map(value => value.event_id)).size !== payloads.length) throw new Error(`SQS returned ${sqsMessages.length}/${payloads.length} expected messages.`);
    evidence.checks.push({ name: 'sqs-round-trip', status: 'passed', messages: sqsMessages.length });
    evidence.status = 'passed';
  } catch (error) {
    evidence.status = 'failed';
    evidence.errors.push({ name: error.name, message: error.message });
  } finally {
    if (queueUrl) {
      try { await timed(sqs, new DeleteQueueCommand({ QueueUrl: queueUrl })); evidence.cleanup.sqs = 'deleted'; }
      catch (error) { evidence.cleanup.sqs = 'failed'; evidence.errors.push({ name: error.name, message: `SQS cleanup: ${error.message}` }); }
    } else evidence.cleanup.sqs = 'not-created';
    if (streamCreated) {
      try { await timed(kinesis, new DeleteStreamCommand({ StreamName: plan.resources.kinesisStream, EnforceConsumerDeletion: true })); evidence.cleanup.kinesis = 'deletion-requested'; }
      catch (error) { evidence.cleanup.kinesis = 'failed'; evidence.errors.push({ name: error.name, message: `Kinesis cleanup: ${error.message}` }); }
    } else evidence.cleanup.kinesis = 'not-created';
    kinesis.destroy?.(); sqs.destroy?.();
    evidence.finishedAt = now().toISOString();
    if (evidence.cleanup.sqs === 'failed' || evidence.cleanup.kinesis === 'failed') evidence.status = 'cleanup-failed';
    const directory = resolve(outputDirectory); await mkdir(directory, { recursive: true });
    evidence.evidencePath = resolve(directory, `${runId}.json`);
    await writeFile(evidence.evidencePath, `${JSON.stringify(evidence, null, 2)}\n`, { flag: 'wx' });
  }
  return evidence;
}
