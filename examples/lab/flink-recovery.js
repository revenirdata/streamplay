// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Kafka, logLevel } from 'kafkajs';

// Deliberately isolated from the regular example and any production infrastructure.
export async function recoveryCompose(...args) {
  const { stdout } = await promisify(execFile)('docker', ['context', 'show'], { windowsHide: true, timeout: 10000 });
  return new Promise((done, reject) => {
    const child = spawn('docker', ['--context', stdout.trim(), 'compose', '-p', 'streamplay-recovery', '-f', 'examples/kafka-flink/compose.yaml', '-f', 'examples/kafka-flink/recovery.compose.yaml', ...args], { windowsHide: true, timeout: 180000, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = ''; const capture = value => { output = (output + value).slice(-30000); };
    child.stdout.on('data', capture); child.stderr.on('data', capture);
    child.once('error', reject); child.once('close', code => code === 0 ? done(output) : reject(new Error(`Recovery Docker command failed (${code}): ${output}`)));
  });
}
export async function startRecoveryStack() {
  await recoveryCompose('build', 'jobmanager');
  await recoveryCompose('up', '-d', '--wait', 'kafka', 'jobmanager', 'taskmanager');
}
async function rest(path) {
  const response = await fetch(`http://127.0.0.1:18081${path}`, { signal: AbortSignal.timeout(5000) });
  if (!response.ok) throw new Error(`Flink ${path}: HTTP ${response.status}`);
  return response.json();
}

export async function runFlinkRecovery({ directory, onUpdate = () => {}, signal }) {
  await mkdir(directory, { recursive: true });
  const id = randomUUID(), suffix = id.replaceAll('-', ''), jobName = `recovery-${suffix}`;
  const inputTopic = `recovery-in-${suffix}`, outputTopic = `recovery-out-${suffix}`;
  const report = { id, status: 'running', phase: 'starting', startedAt: new Date().toISOString(), inputs: [], outputs: [], publications: [], logs: [], checks: [], state: {}, topics: { inputTopic, outputTopic },
    scope: 'Real Flink 1.20.2 worker loss with JobManager and Kafka retained. Independent Kafka readers expose input/output JSON. Aggregate outputs are updates, not one receipt per input. This does not certify Bluebot, Managed Flink, JobManager loss, EFO or exactly-once external delivery.' };
  let jobId, readerError, readerReady = false, workerStopped = false, cancelled = false;
  const notify = () => onUpdate(structuredClone(report));
  const phase = message => { report.phase = message; report.logs.push(`${new Date().toISOString()} ${message}`); notify(); };
  const kafka = new Kafka({ clientId: jobName, brokers: ['127.0.0.1:19092'], logLevel: logLevel.ERROR, retry: { retries: 8, initialRetryTime: 200, maxRetryTime: 2000 } });
  const admin = kafka.admin(), producer = kafka.producer(), reader = kafka.consumer({ groupId: `observer-${suffix}` });
  const latest = new Map();
  reader.on(reader.events.GROUP_JOIN, () => { readerReady = true; });
  reader.on(reader.events.CRASH, ({ payload }) => { readerReady = false; if (!payload.restart) readerError = payload.error; report.logs.push(`Kafka observer: ${payload.error.message}; restart scheduled: ${payload.restart}`); notify(); });
  async function until(check, label, timeout = 60000) {
    const end = Date.now() + timeout;
    while (Date.now() < end) { signal?.throwIfAborted(); if (readerError) throw readerError; const value = await check(); if (value) return value; await delay(250, undefined, { signal }); }
    throw new Error(`Timed out: ${label}`);
  }
  async function send(n, source, amount) {
    signal?.throwIfAborted();
    const value = { event_id: `${id}:${n}`, source_id: source, amount };
    const acknowledgement = await producer.send({ topic: inputTopic, acks: -1, messages: [{ key: source, value: JSON.stringify(value) }] });
    report.publications.push({ value, acknowledgement, observedAt: new Date().toISOString() }); notify();
  }
  const expectedBefore = [{ source_id: 'sensor-a', event_count: 2, total: 7 }, { source_id: 'sensor-b', event_count: 1, total: 5 }];
  const expectedAfter = [{ source_id: 'sensor-a', event_count: 4, total: 15 }, { source_id: 'sensor-b', event_count: 2, total: 13 }];
  const totals = () => [...latest.values()].sort((a, b) => a.source_id.localeCompare(b.source_id));
  const matches = expected => JSON.stringify(totals()) === JSON.stringify(expected);
  try {
    await admin.connect(); await producer.connect(); await reader.connect();
    await admin.createTopics({ waitForLeaders: true, topics: [inputTopic, outputTopic].map(topic => ({ topic, numPartitions: 1, replicationFactor: 1 })) });
    await reader.subscribe({ topics: [inputTopic, outputTopic], fromBeginning: true });
    await reader.run({ eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString() ?? 'null', value = JSON.parse(raw);
      const record = { json: true, raw, value, topic, partition, offset: message.offset, key: message.key?.toString(), transport: 'kafka', observedAt: new Date().toISOString() };
      (topic === inputTopic ? report.inputs : report.outputs).push(record);
      if (topic === outputTopic && value) latest.set(value.source_id, value);
      if (report.inputs.length + report.outputs.length > 1000) throw new Error('Recovery capture limit exceeded');
      notify();
    } });
    await until(() => readerReady, 'Kafka observer group assignment');
    await until(async () => (await rest('/overview'))['slots-available'] > 0, 'Flink slots');
    const sql = (await readFile('examples/kafka-flink/recovery.sql', 'utf8')).replaceAll('__JOB__', jobName).replaceAll('__INPUT__', inputTopic).replaceAll('__OUTPUT__', outputTopic);
    const sqlPath = resolve(directory, `${id}.sql`); await writeFile(sqlPath, sql, { flag: 'wx' });
    phase('Submitting stateful Flink aggregation');
    await recoveryCompose('run', '--rm', '-T', '--volume', `${sqlPath}:/opt/flink/usrlib/recovery.sql:ro`, 'sql-client', '/opt/flink/bin/sql-client.sh', '-f', '/opt/flink/usrlib/recovery.sql');
    jobId = (await until(async () => (await rest('/jobs/overview')).jobs.find(j => j.name === jobName && j.state === 'RUNNING'), 'running recovery job')).jid;
    report.jobId = jobId;
    phase('Sending baseline events and inspecting actual aggregate output');
    await send(1, 'sensor-a', 3); await send(2, 'sensor-a', 4); await send(3, 'sensor-b', 5);
    await until(() => matches(expectedBefore), 'baseline totals');
    report.checks.push({ name: 'Before failure: independent totals for two sources', passed: true, expected: expectedBefore, actual: totals() });
    const baselineAt = Date.now();
    phase('Waiting for a completed checkpoint containing the baseline');
    const checkpoint = await until(async () => { const state = await rest(`/jobs/${jobId}/checkpoints`); report.state = { jobId, checkpoints: state, latestTotals: totals() }; notify(); return state.latest?.completed?.trigger_timestamp >= baselineAt && state.latest.completed; }, 'completed baseline checkpoint');
    report.checkpointBeforeFailure = checkpoint;
    phase('Killing the dedicated Flink worker; Kafka and JobManager stay running');
    workerStopped = true; await recoveryCompose('kill', '-s', 'SIGKILL', 'taskmanager');
    phase('Publishing two events while the worker is down');
    await send(4, 'sensor-a', 6); await send(5, 'sensor-b', 8);
    await until(() => report.inputs.length === 5, 'observer sees buffered input');
    assert.ok(matches(expectedBefore), 'Flink output must not incorporate events while its worker is stopped');
    report.checks.push({ name: 'Kafka accepts input while Flink worker is unavailable', passed: true, inputIds: report.inputs.map(r => r.value.event_id), outputTotals: totals() });
    await delay(1500, undefined, { signal });
    phase('Restarting the worker and waiting for checkpoint restoration');
    await recoveryCompose('up', '-d', 'taskmanager'); workerStopped = false;
    const restored = await until(async () => { const state = await rest(`/jobs/${jobId}/checkpoints`); report.state = { jobId, checkpoints: state, latestTotals: totals() }; notify(); return state.counts?.restored > 0 && state.latest.restored; }, 'checkpoint restored', 90000);
    assert.ok(restored.id >= checkpoint.id, 'Restore must use the baseline checkpoint or a newer one');
    report.checks.push({ name: 'Flink reports an actual checkpoint restore', passed: true, checkpointBeforeFailure: checkpoint.id, restored });
    await until(async () => (await rest(`/jobs/${jobId}`)).state === 'RUNNING', 'job RUNNING after restore');
    await send(6, 'sensor-a', 2);
    await until(() => matches(expectedAfter), 'correct recovered totals');
    await delay(2000, undefined, { signal });
    assert.deepEqual(totals(), expectedAfter);
    assert.equal(new Set(report.inputs.map(r => r.value.event_id)).size, 6);
    report.checks.push({ name: 'Recovered state includes pre-failure, buffered and post-restart events', passed: true, expected: expectedAfter, actual: totals() });
    report.state = { jobId, checkpoints: await rest(`/jobs/${jobId}/checkpoints`), latestTotals: totals() };
    report.status = 'passed'; phase('Recovery verified');
  } catch (error) { cancelled = Boolean(signal?.aborted); report.status = cancelled ? 'cancelled' : 'failed'; report.error = error.message; phase('Experiment stopped with retained evidence'); }
  finally {
    try {
      if (workerStopped) await recoveryCompose('up', '-d', 'taskmanager');
      if (jobId) { const response = await fetch(`http://127.0.0.1:18081/jobs/${jobId}?mode=cancel`, { method: 'PATCH', signal: AbortSignal.timeout(5000) }); if (!response.ok) throw new Error(`Job cleanup HTTP ${response.status}`); }
    } catch (error) { report.cleanupError = error.message; report.status = 'failed'; }
    for (const client of [reader, producer, admin]) { try { await client.disconnect(); } catch (error) { report.cleanupError = error.message; report.status = 'failed'; } }
    report.finishedAt = new Date().toISOString(); report.phase = 'finished';
    await writeFile(resolve(directory, `${id}.json`), JSON.stringify(report, null, 2), { flag: 'wx' }); notify();
  }
  return report;
}
