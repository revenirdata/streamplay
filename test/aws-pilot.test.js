// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildAwsPilotPlan, runAwsPilot, validateAwsPilotOptions } from '../src/aws-pilot.js';

test('AWS pilot validates explicit bounded account, region, events and cost guardrail', () => {
  assert.deepEqual(validateAwsPilotOptions({ profile: 'personal', region: 'us-east-1', events: 5, maxCostUsd: 1, outputDirectory: '.' }).events, 5);
  assert.throws(() => validateAwsPilotOptions({ region: 'invalid' }), /AWS Region/);
  assert.throws(() => validateAwsPilotOptions({ region: 'us-east-1', events: 101 }), /events/);
  assert.throws(() => validateAwsPilotOptions({ region: 'us-east-1', maxCostUsd: 2 }), /max-cost-usd/);
  const plan = buildAwsPilotPlan({ account: '123456789012', profile: 'personal', region: 'us-east-1', suffix: '0123456789' });
  assert.equal(plan.resources.kinesisStream, 'streamplay-pilot-0123456789');
  assert.match(plan.costStatement, /cannot enforce AWS billing/);
});

test('AWS pilot round-trips synthetic records and cleans owned resources', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'streamplay-aws-'));
  const payloads = [];
  const calls = [];
  const kinesis = { async send(command) {
    calls.push(command.constructor.name);
    if (command.constructor.name === 'DescribeStreamSummaryCommand') return { StreamDescriptionSummary: { StreamStatus: 'ACTIVE' } };
    if (command.constructor.name === 'ListShardsCommand') return { Shards: [{ ShardId: 'shardId-000' }] };
    if (command.constructor.name === 'GetShardIteratorCommand') return { ShardIterator: 'iterator' };
    if (command.constructor.name === 'PutRecordsCommand') { for (const item of command.input.Records) payloads.push(Buffer.from(item.Data)); return { FailedRecordCount: 0 }; }
    if (command.constructor.name === 'GetRecordsCommand') return { Records: payloads.map(Data => ({ Data })), NextShardIterator: 'iterator' };
    return {};
  }, destroy() {} };
  const queued = [];
  const sqs = { async send(command) {
    calls.push(command.constructor.name);
    if (command.constructor.name === 'CreateQueueCommand') return { QueueUrl: 'https://sqs.test/queue' };
    if (command.constructor.name === 'SendMessageBatchCommand') { queued.push(...command.input.Entries.map(entry => ({ MessageId: entry.Id, ReceiptHandle: `receipt-${entry.Id}`, Body: entry.MessageBody }))); return { Failed: [] }; }
    if (command.constructor.name === 'ReceiveMessageCommand') return { Messages: queued.splice(0, command.input.MaxNumberOfMessages) };
    return {};
  }, destroy() {} };
  const plan = buildAwsPilotPlan({ account: '123456789012', profile: 'personal', region: 'us-east-1', suffix: '0123456789', events: 3 });
  const evidence = await runAwsPilot(plan, { kinesis, sqs, outputDirectory: directory, now: () => new Date('2026-09-24T00:00:00.000Z') });
  assert.equal(evidence.status, 'passed');
  assert.equal(evidence.cleanup.sqs, 'deleted');
  assert.equal(evidence.cleanup.kinesis, 'deletion-requested');
  assert.ok(calls.includes('DeleteQueueCommand'));
  assert.ok(calls.includes('DeleteStreamCommand'));
  assert.equal(JSON.parse(await readFile(evidence.evidencePath, 'utf8')).checks.length, 4);
  await rm(directory, { recursive: true, force: true });
});
