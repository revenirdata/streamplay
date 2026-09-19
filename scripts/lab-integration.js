// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { connectAsync } from 'mqtt';
import { KinesisClient, CreateStreamCommand, DescribeStreamCommand, GetShardIteratorCommand, GetRecordsCommand, DeleteStreamCommand } from '@aws-sdk/client-kinesis';
import { createLab } from '../src/lab/runtime.js';
import { sqsSandbox, verifyQueueRecovery } from '../src/lab/queues.js';
import mqtt from '../examples/lab/mqtt.js';
import kinesis from '../examples/lab/kinesis-sqs.js';
import { sourcePresets } from '../src/source-profile.js';
import { SQSClient, ListQueuesCommand } from '@aws-sdk/client-sqs';

const until = async fn => {
  const end = Date.now() + 20000; let watchdog;
  try {
    // Keep a referenced deadline while fetch/AbortSignal uses unreferenced handles during startup.
    return await Promise.race([
      (async () => { while (Date.now() < end) { const value = await fn(); if (value) return value; await delay(150); } throw new Error('Integration condition timed out'); })(),
      new Promise((_, reject) => { watchdog = setTimeout(() => reject(new Error('Integration condition timed out')), 20000); })
    ]);
  } finally { clearTimeout(watchdog); }
};
const dir = await mkdtemp(join(tmpdir(), 'streamplay-real-lab-'));
const mode = process.argv[2] ?? 'queues';
const evidence = { mode, startedAt: new Date().toISOString(), checks: [] };
let lab, subscriber, queueApi, queue, client, stream;
try {
  if (mode === 'mqtt') {
    const prefix = `streamplay-fleet-${randomUUID()}`;
    Object.assign(process.env, { STREAMPLAY_MQTT_URL: 'mqtt://127.0.0.1:18884', STREAMPLAY_MQTT_INPUT_TOPIC: `${prefix}/in`, STREAMPLAY_MQTT_OUTPUT_TOPIC: `${prefix}/out` });
    subscriber = await connectAsync(process.env.STREAMPLAY_MQTT_URL, { reconnectPeriod: 0, connectTimeout: 10000 });
    const received = [];
    subscriber.on('message', (_topic, raw) => received.push(JSON.parse(raw.toString())));
    await subscriber.subscribeAsync(process.env.STREAMPLAY_MQTT_INPUT_TOPIC, { qos: 1 });
    lab = await createLab(mqtt, { dataDir: dir });
    await lab.action('profile', sourcePresets.meter);
    await lab.action('configure-device', { id: 'device-001', rate: 60, intervalMs: 100 });
    await lab.action('add', { id: 'device-002', rate: 1, intervalMs: 100 });
    await lab.action('start', { id: 'device-002', durationMs: 60000 });
    await lab.action('episode', { id: 'device-001', targetQuantity: 0.3, baselineMs: 100, stopMs: 100 });
    await until(async () => !(await lab.snapshot()).devices[0].active);
    const snapshot = await lab.snapshot(); assert.equal(snapshot.devices[0].report.status, 'published'); assert.equal(snapshot.devices[1].active, true);
    assert.ok(Math.abs(snapshot.devices[0].total - 0.3) < 1e-9);
    await lab.action('stop', { id: 'device-002' });
    await until(() => received.length === snapshot.devices[0].sent + snapshot.devices[1].sent || received.length > snapshot.devices[0].sent + snapshot.devices[1].sent);
    assert.ok(received.filter(e => e.device_id === 'device-001' && e.value === 0).length >= 2);
    evidence.checks.push('Real subscriber received target episode and independent fleet telemetry');
    await lab.close(); lab = await createLab(mqtt, { dataDir: dir });
    await lab.action('send', { id: 'device-001' });
    assert.ok(Math.abs((await lab.snapshot()).devices[0].total - 0.3) < 1e-9);
    evidence.checks.push('Accumulated input total survived workbench restart');
    evidence.frames = received;
  } else {
    const endpoint = 'http://127.0.0.1:4567'; queueApi = sqsSandbox(endpoint);
    await until(async () => { try { return (await fetch(endpoint + '/_localstack/health', { signal: AbortSignal.timeout(2000) })).ok; } catch { return false; } });
    // LocalStack loads services lazily after its health endpoint is ready.
    const warmup = new SQSClient({ endpoint, region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' }, maxAttempts: 1 });
    try { await warmup.send(new ListQueuesCommand({}), { abortSignal: AbortSignal.timeout(60000) }); }
    finally { warmup.destroy(); }
    evidence.recovery = await verifyQueueRecovery(queueApi);
    assert.equal(evidence.recovery.status, 'passed', JSON.stringify(evidence.recovery));
    queue = await queueApi.create(`sp-output-${randomUUID()}.fifo`);
    client = new KinesisClient({ endpoint, region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' }, maxAttempts: 2 });
    stream = `sp-lab-${randomUUID()}`;
    await client.send(new CreateStreamCommand({ StreamName: stream, ShardCount: 1 }));
    const description = await until(async () => { const result = await client.send(new DescribeStreamCommand({ StreamName: stream })); return result.StreamDescription.StreamStatus === 'ACTIVE' && result.StreamDescription; });
    const iterator = await client.send(new GetShardIteratorCommand({ StreamName: stream, ShardId: description.Shards[0].ShardId, ShardIteratorType: 'TRIM_HORIZON' }));
    Object.assign(process.env, { STREAMPLAY_LOCALSTACK_ENDPOINT: endpoint, STREAMPLAY_KINESIS_STREAM: stream, STREAMPLAY_SQS_QUEUES: JSON.stringify([queue]) });
    lab = await createLab(kinesis, { dataDir: dir });
    await lab.action('send', { id: 'device-001' });
    let next = iterator.ShardIterator;
    const records = await until(async () => { const result = await client.send(new GetRecordsCommand({ ShardIterator: next })); next = result.NextShardIterator; return result.Records.length && result.Records; });
    assert.equal(JSON.parse(Buffer.from(records[0].Data).toString()).device_id, 'device-001');
    evidence.checks.push('Real local Kinesis received the fleet event');
    // Explicit test output, not a simulated claim that Flink processed the input.
    await queueApi.publish(queue, { test_output: 'inspector probe' });
    assert.equal((await lab.snapshot()).queues.paused, true); assert.equal((await lab.snapshot()).outputs.length, 0);
    await lab.action('resume-consumer');
    await until(async () => (await lab.snapshot()).outputs.length === 1);
    await lab.action('pause-consumer');
    const snapshot = await lab.snapshot(); assert.equal(snapshot.queues.queues[0].inspectorDeleted, 1);
    evidence.checks.push('Output inspector stayed paused until resumed, captured and deleted the sandbox output, and paused cleanly');
    evidence.snapshot = snapshot;
  }
  evidence.status = 'passed';
} catch (error) { evidence.status = 'failed'; evidence.error = error.stack; process.exitCode = 1; }
finally {
  try {
    await lab?.close(); await subscriber?.endAsync();
    if (queue) await queueApi.destroy(queue);
    if (stream) await client.send(new DeleteStreamCommand({ StreamName: stream }));
  } catch (error) { evidence.cleanupError = error.message; evidence.status = 'failed'; process.exitCode = 1; }
  queueApi?.close(); client?.destroy(); await rm(dir, { recursive: true, force: true });
  await mkdir('.streamplay/evidence', { recursive: true });
  await writeFile(`.streamplay/evidence/lab-${mode}.json`, JSON.stringify(evidence, null, 2));
}
console.log(JSON.stringify({ status: evidence.status, checks: evidence.checks, recoveryChecks: evidence.recovery?.checks.length, error: evidence.error, cleanupError: evidence.cleanupError }, null, 2));
