// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compare, validateScenario } from '../src/scenario.js';
import { createStore } from '../src/store.js';
import { runScenario } from '../src/runner.js';

const fixture = JSON.parse(await readFile(new URL('../examples/scenarios/orders.process.json', import.meta.url), 'utf8'));

test('comparison ignores object key and record order but preserves duplicate counts', () => {
  assert.deepEqual(compare([{ a: 1, b: 2 }, { a: 1, b: 2 }], [{ b: 2, a: 1 }]), { equal: false, added: [], missing: [{ a: 1, b: 2 }] });
  assert.equal(compare([1, { a: [2, 3] }], [{ a: [2, 3] }, 1]).equal, true);
  assert.equal(compare([{ a: [2, 3] }], [{ a: [3, 2] }]).equal, false);
  assert.equal(compare([null], ['null']).equal, false);
});

test('rejects invalid scenarios and strips browser-provided connection or command settings', () => {
  assert.throws(() => validateScenario({ ...fixture, observeMs: -1 }), /Observation/);
  assert.throws(() => validateScenario({ ...fixture, events: [] }), /events/);
  assert.throws(() => validateScenario({ ...fixture, adapter: 'shell' }), /Choose/);
  assert.throws(() => validateScenario({ ...fixture, expected: {} }), /Expected/);
  const valid = validateScenario({ ...fixture, command: 'untrusted', brokers: ['untrusted:9092'] });
  assert.equal(valid.command, undefined); assert.equal(valid.brokers, undefined);
});

test('executes a real process, captures duplicates, and persists an immutable run', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'streamplay-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const store = createStore(dir);
  const run = await runScenario({ ...fixture, observeMs: 1000 }, { store });
  assert.equal(run.status, 'observed', run.error);
  assert.equal(run.inputs.length, 3); assert.equal(run.outputs.length, 2);
  assert.deepEqual(run.outputs.map(r => r.value), [{ order_id: 'order-101', total_cents: 6000 }, { order_id: 'order-101', total_cents: 6000 }]);
  assert.match(run.environment.sourceSha256, /^[a-f0-9]{64}$/);
  assert.deepEqual(await store.get('runs', run.id), run);
  await assert.rejects(store.save('runs', run), { code: 'EEXIST' });
  await assert.rejects(store.get('runs', '../../secrets'), /Invalid artifact/);
});

test('invalid application input produces an error and captured stderr, not a successful empty run', async () => {
  const run = await runScenario({ ...fixture, events: [{ unexpected: true }], observeMs: 1000, expected: [] });
  assert.equal(run.status, 'error');
  assert.match(run.error, /exited with code 1/);
  assert.ok(run.logs.some(log => log.includes('Expected order_id')));
  assert.equal(run.observation.complete, false);
});

test('observer is attached before sends and waits the entire window to catch late duplicates', async () => {
  let observerAttached = false, cleaned = false;
  const run = await runScenario({ ...fixture, observeMs: 120, expected: [{ order: 1 }], events: [{ order: 1 }] }, {
    adapters: { process: async ({ onOutput }) => {
      observerAttached = true;
      return { metadata: { engine: 'test' },
        async send(events, onSent) {
          assert.equal(observerAttached, true);
          onSent(events[0]);
          onOutput({ raw: '{"order":1}', value: { order: 1 } });
          setTimeout(() => onOutput({ raw: '{"order":1}', value: { order: 1 } }), 60);
        }, check() {}, async close() { cleaned = true; } };
    } }
  });
  assert.equal(run.status, 'failed'); assert.equal(run.assertion.added.length, 1); assert.equal(cleaned, true);
});

test('adapter failure preserves partial evidence and cleans up without evaluating assertions', async () => {
  let closed = false;
  const run = await runScenario({ ...fixture, expected: [] }, { adapters: { process: async () => ({
    async send(events, onSent) { onSent(events[0]); throw new Error('Broker disconnected'); },
    async close() { closed = true; }, check() {}
  }) } });
  assert.equal(run.status, 'error'); assert.equal(run.inputs.length, 1); assert.equal(run.assertion, undefined); assert.equal(closed, true);
});

test('zero observed records is not a passing assertion when output was expected', async () => {
  const run = await runScenario({ ...fixture, observeMs: 100, expected: [{ result: 1 }] }, { adapters: { process: async () => ({
    async send() {}, check() {}, async close() {}
  }) } });
  assert.equal(run.status, 'failed'); assert.deepEqual(run.assertion.missing, [{ result: 1 }]);
});
