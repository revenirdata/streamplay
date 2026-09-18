// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { runScenario } from '../src/runner.js';
import { assertOutputs, record, validateScenario } from '../src/scenario.js';

const scenario = { version: 1, name: 'Failure probe', adapter: 'local', observeMs: 100, events: [{ id: 1 }], expected: [] };
const adapter = overrides => async () => ({ async send() {}, check() {}, async close() {}, ...overrides });

test('malformed raw output cannot satisfy a JSON-string assertion', () => {
  assert.equal(assertOutputs({ expected: ['bad'] }, [record('bad')]).equal, false);
  assert.equal(assertOutputs({ expected: ['bad'] }, [record('"bad"')]).equal, true);
  assert.equal(assertOutputs({ expected: [null] }, [record('null', { tombstone: true })]).equal, false);
});

test('field matching preserves raw data and fails when a requested field is absent', () => {
  const outputs = [record('{"kind":"offline","id":"generated","detail":{"at":123}}')];
  const before = structuredClone(outputs);
  assert.equal(assertOutputs({ expected: [{ kind: 'offline' }], matchFields: ['kind'] }, outputs).equal, true);
  assert.equal(assertOutputs({ expected: [{}], matchFields: ['missing'] }, outputs).equal, false);
  assert.deepEqual(outputs, before);
});

test('staged inputs respect delays while observing output between sends', async () => {
  const sentAt = [], snapshots = [];
  const run = await runScenario({ ...scenario, events: [1, 2], scheduleMs: [0, 120], expected: [1, 2] }, {
    onUpdate: ({ run }) => snapshots.push({ sent: run.inputs.length, seen: run.outputs.length }),
    adapters: { local: async ({ onOutput }) => ({ ...await adapter({})(), async send(events, onSent) {
      sentAt.push(Date.now()); onSent(record(JSON.stringify(events[0]))); onOutput(record(JSON.stringify(events[0])));
    } }) }
  });
  assert.equal(run.status, 'passed'); assert.ok(sentAt[1] - sentAt[0] >= 100);
  assert.ok(snapshots.some(s => s.sent === 1 && s.seen === 1));
});

test('cancelling a staged run stops later sends, retains partial evidence, and closes the adapter', async () => {
  const controller = new AbortController(); let closed = false;
  const run = await runScenario({ ...scenario, events: [1, 2], scheduleMs: [0, 5000] }, {
    signal: controller.signal,
    adapters: { local: adapter({ async send(events, onSent) { onSent(record('1')); controller.abort(new Error('stop')); }, async close() { closed = true; } }) }
  });
  assert.equal(run.status, 'cancelled'); assert.equal(run.inputs.length, 1); assert.equal(run.observation.complete, false);
  assert.equal(run.assertion, undefined); assert.equal(closed, true);
});

test('an adapter failure cannot masquerade as a passing empty-output assertion', async () => {
  const run = await runScenario(scenario, { adapters: { local: adapter({ check() { throw new Error('Application exited'); } }) } });
  assert.equal(run.status, 'error'); assert.equal(run.assertion, undefined);
});

test('capture overflow fails instead of silently asserting a truncated record set', async () => {
  const run = await runScenario(scenario, { adapters: { local: async ({ onOutput }) => ({ ...await adapter({})(), async send() {
    for (let i = 0; i < 10_001; i++) onOutput(record('1'));
  } }) } });
  assert.equal(run.status, 'error'); assert.match(run.error, /incomplete/); assert.equal(run.outputs.length, 10_000);
});

test('invalid pacing and prototype-traversing field paths are rejected', () => {
  assert.throws(() => validateScenario({ ...scenario, scheduleMs: [120_001] }), /scheduleMs/);
  assert.throws(() => validateScenario({ ...scenario, scheduleMs: [] }), /scheduleMs/);
  assert.throws(() => validateScenario({ ...scenario, matchFields: ['__proto__.secret'] }), /matchFields/);
});

test('cleanup failure remains an error even if an assertion matched', async () => {
  const run = await runScenario(scenario, { adapters: { local: adapter({ async close() { throw new Error('failed to release consumer'); } }) } });
  assert.equal(run.status, 'error'); assert.match(run.error, /Cleanup failed/);
});

test('storage failure returns captured evidence with an error instead of reporting success or discarding it', async () => {
  const run = await runScenario(scenario, { store: { async save() { throw new Error('disk full'); } }, adapters: { local: adapter({}) } });
  assert.equal(run.status, 'error'); assert.match(run.error, /disk full/); assert.equal(run.storage.saved, false);
  assert.deepEqual(run.scenario, scenario);
});
