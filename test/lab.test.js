// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { openLedger } from '../src/lab/ledger.js';
import { createFleet } from '../src/lab/fleet.js';
import { createLab } from '../src/lab/runtime.js';
import { localEndpoint, createQueueInspector } from '../src/lab/queues.js';
import { runChecks } from '../src/lab/checks.js';
import { runScenario } from '../src/runner.js';
import { thresholdPhases } from '../src/experiment.js';
import sensor from '../examples/lab/in-process.js';

const temporary = () => mkdtemp(join(tmpdir(), 'streamplay-parity-'));
const until = async fn => { const deadline = Date.now() + 10000; while (Date.now() < deadline) { if (await fn()) return; await delay(10); } throw new Error('Condition timed out'); };
test('ledger excludes simultaneous writers and survives restart without rewinding', async () => {
  const dir = await temporary(); let ledger;
  try {
    ledger = await openLedger(dir); await ledger.reserve('broker/device', 12);
    await assert.rejects(openLedger(dir), /locked/);
    await ledger.close(); ledger = await openLedger(dir);
    assert.equal(ledger.get('broker/device'), 12);
    await assert.rejects(ledger.reserve('broker/device', 11), /decrease/);
    await ledger.reserve('broker/device', 13); assert.equal(ledger.get('broker/device'), 13);
  } finally { await ledger?.close(); await rm(dir, { recursive: true, force: true }); }
});
test('independent devices stop independently; target quantity survives delayed publishing and restarts', async () => {
  const dir = await temporary(); const ledger = await openLedger(dir); const frames = [];
  const fleet = createFleet({ namespace: 'test', ledger, publish: async event => { await delay(30); frames.push(event); return 'accepted'; } });
  try {
    fleet.add({ id: 'a', rate: 60, intervalMs: 50 }); fleet.add({ id: 'b', rate: 1, intervalMs: 50 });
    // Independence is the contract; a busy host must not expire B's session before A finishes.
    fleet.start('b', 60000);
    fleet.episode({ id: 'a', baselineMs: 50, stopMs: 50, targetQuantity: 0.2 });
    await until(() => !fleet.snapshot()[0].active);
    assert.equal(fleet.snapshot()[0].report.status, 'published');
    assert.ok(Math.abs(ledger.get('test:a') - 0.2) < 1e-9);
    assert.equal(fleet.snapshot()[1].active, true);
    const before = frames.filter(f => f.device_id === 'b').length;
    await until(() => frames.filter(f => f.device_id === 'b').length > before);
    await fleet.stop('b'); const stopped = frames.length; await delay(80); assert.equal(frames.length, stopped);
    assert.ok(frames.filter(f => f.device_id === 'a' && f.value === 0).length >= 2);
    assert.throws(() => fleet.episode({ id: 'a', rawRate: 0, targetQuantity: 1 }), /positive/);
    assert.throws(() => fleet.configure({ id: 'a', rate: NaN }), /Value/);
    for (let i = 2; i < 20; i++) fleet.add({ id: `d${i}` }); assert.throws(() => fleet.add(), /20/);
  } finally { await fleet.close(); await ledger.close(); await rm(dir, { recursive: true, force: true }); }
});
test('failed publish reserves total and records error instead of claiming application success', async () => {
  const dir = await temporary(), ledger = await openLedger(dir);
  let calls = 0;
  const fleet = createFleet({ namespace: 'test', ledger, publish: async () => { if (++calls > 1) throw new Error('network failed'); } });
  try {
    fleet.add({ id: 'a', rate: 60, intervalMs: 50 }); fleet.start('a', 100);
    await until(() => !fleet.active);
    assert.equal(fleet.snapshot()[0].report.status, 'error'); assert.ok(ledger.get('test:a') > 0);
  } finally { await fleet.close(); await ledger.close(); await rm(dir, { recursive: true, force: true }); }
});
test('lab executes real example assertions, excludes fleet interference and clears only the view', async () => {
  const dir = await temporary(); const lab = await createLab(sensor, { dataDir: dir });
  try {
    await lab.action('configure-device', { id: 'device-001', rate: 4 }); await lab.action('send', { id: 'device-001' });
    assert.equal((await lab.snapshot()).outputs.length, 1);
    await lab.action('clear'); assert.equal((await lab.snapshot()).outputs.length, 0);
    assert.equal((await lab.snapshot()).state.devices['device-001'].received, 1);
    await lab.action('start', { id: 'device-001', durationMs: 1000 });
    await assert.rejects(lab.scenarioAdapter({}), /Stop live devices/); await lab.action('stop', { id: 'device-001' });
    const run = await runScenario({ version: 1, name: 'threshold', adapter: 'local', observeMs: 100,
      events: [2.9, 3, 3.1].map(value => ({ device_id: 'a', value })), expected: [{ device_id: 'a', event: 'above_threshold', value: 3.1 }] }, { adapters: { local: c => lab.scenarioAdapter(c) } });
    assert.equal(run.status, 'passed', run.error); assert.deepEqual((await lab.snapshot()).state.devices, {});
    await assert.rejects(lab.action('shell', { command: 'anything' }), /unsupported/);
    assert.deepEqual(thresholdPhases(3, 0.1).map(p => p.value), [2.9, 3, 3.1]);
  } finally { await lab.close(); await rm(dir, { recursive: true, force: true }); }
});
test('queue inspector pauses after in-flight work and reports unknown age, DLQ and approximate counts', async () => {
  const url = 'http://127.0.0.1:4566/000000000000/test'; let received = 0, removed = 0;
  const api = { endpoint: 'http://127.0.0.1:4566', attributes: async () => ({ ApproximateNumberOfMessages: '2' }), receive: async () => { received++; await delay(50); return [{ Body: '{}', ReceiptHandle: 'receipt' }]; }, remove: async () => { removed++; } };
  const inspector = createQueueInspector(api, [url], async () => {});
  try {
    await delay(280); assert.equal(received, 0); inspector.resume(); await until(() => received > 0); await inspector.pause();
    assert.equal(removed, 1); await delay(300); assert.equal(received, 1);
    const snapshot = await inspector.snapshot(); assert.equal(snapshot.queues[0].oldestMessageAgeSeconds, null); assert.match(snapshot.queues[0].warnings[0], /dead-letter/);
    assert.throws(() => localEndpoint('https://sqs.us-east-1.amazonaws.com'), /loopback/);
  } finally { await inspector.close(); }
});
test('regression command checks require positive evidence and terminate timed out checks', async () => {
  const specs = [
    { name: 'positive', args: ['-e', 'console.log("checks: 4 passed")'], successPattern: 'checks: [1-9][0-9]* passed' },
    { name: 'empty', args: ['-e', 'console.log("checks: 0 passed")'], successPattern: 'checks: [1-9][0-9]* passed' },
    { name: 'timeout', args: ['-e', 'setInterval(()=>{},1000)'], successPattern: 'pass', timeoutMs: 150 }
  ].map(spec => ({ command: process.execPath, timeoutMs: 1000, ...spec }));
  const report = await runChecks(specs); assert.equal(report.status, 'failed'); assert.deepEqual(report.checks.map(c => c.passed), [true, false, false]); assert.equal(report.checks[2].timedOut, true);
  assert.equal((await runChecks()).status, 'not-configured');
  const controller = new AbortController();
  const cancelled = runChecks([{ ...specs[2], timeoutMs: 10000 }], { signal: controller.signal });
  setTimeout(() => controller.abort(), 100);
  assert.equal((await cancelled).checks[0].cancelled, true);
});

test('phase assertions catch an early alert even when overall output counts match', async () => {
  const scenario = { version: 1, name: 'early output', adapter: 'local', events: [{ value: 2 }, { value: 4 }], scheduleMs: [0, 10], phaseNames: ['below', 'above'], observeMs: 100,
    expected: [{ alert: true }], phaseExpectations: [{ phase: 'below', expected: [] }, { phase: 'above', expected: [{ alert: true }] }] };
  const run = await runScenario(scenario, { adapters: { local: async ({ onOutput }) => ({ send: async (events, sent) => { for (const value of events) { sent({ value }); if (value.value === 2) onOutput({ value: { alert: true }, json: true, raw: '{"alert":true}' }); } }, check() {}, close() {} }) } });
  assert.equal(run.assertion.equal, true); assert.equal(run.status, 'failed'); assert.equal(run.phaseAssertions[0].equal, false);
});
