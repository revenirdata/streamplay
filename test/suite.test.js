// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runSuite, validateSuite } from '../src/suite.js';
import { createStore } from '../src/store.js';
import { workbench } from '../src/server.js';

const scenario = (name, expected = [{ value: 1 }]) => ({ version: 1, name, adapter: 'local', events: [{ value: 1 }], observeMs: 100, ...(expected === null ? {} : { expected }) });
const adapter = ({ onOutput }) => ({ check() {}, async send(events, sent) { for (const value of events) { sent({ value }); onOutput({ value, json: true }); } }, async close() {} });

test('suite retains failed case, continues, persists all evidence, and leaves unasserted cases OBSERVED', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'streamplay-suite-'));
  try {
    const store = createStore(dir);
    const report = await runSuite({ version: 1, name: 'Mixed', scenarios: [scenario('wrong', []), scenario('right'), scenario('capture', null)] }, { store, adapters: { local: adapter } });
    assert.equal(report.status, 'failed');
    assert.deepEqual(report.cases.map(c => c.status), ['failed', 'passed', 'observed']);
    assert.equal(report.cases[0].assertions[0].actual.length, 1);
    assert.equal(report.cases[0].assertions[0].expected.length, 0);
    assert.equal((await store.list('runs')).length, 3);
    assert.deepEqual(await store.get('suites', report.id), JSON.parse(JSON.stringify(report)));
    const observed = await runSuite({ version: 1, name: 'Capture', scenarios: [scenario('capture', null)] }, { adapters: { local: adapter } });
    assert.equal(observed.status, 'observed');
  } finally { await rm(dir, { recursive: true, force: true }); }
});
test('cancellation waits for cleanup and retains untouched cases as NOT RUN', async () => {
  const controller = new AbortController(); let closed = false;
  const report = await runSuite({ version: 1, name: 'Cancel', scenarios: [scenario('first'), scenario('second')] }, {
    signal: controller.signal,
    adapters: { local: callbacks => ({ ...adapter(callbacks), async send() { controller.abort(new Error('stop')); }, async close() { closed = true; } }) },
  });
  assert.equal(closed, true); assert.equal(report.status, 'cancelled');
  assert.deepEqual(report.cases.map(c => c.status), ['cancelled', 'not-run']);
});
test('cleanup error prevents later cases and invalid plans never execute', async () => {
  assert.throws(() => validateSuite({ version: 1, name: 'empty', scenarios: [] }));
  assert.throws(() => validateSuite({ version: 1, name: 'bad', scenarios: [scenario('first'), {}] }));
  const report = await runSuite({ version: 1, name: 'Cleanup', scenarios: [scenario('first'), scenario('second')] }, {
    adapters: { local: callbacks => ({ ...adapter(callbacks), async close() { throw new Error('cleanup failed'); } }) },
  });
  assert.equal(report.status, 'error'); assert.equal(report.cases[1].status, 'not-run');
});
test('suite server reserves the pipeline, exposes progress, cancels and saves the report', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'streamplay-suite-api-'));
  let entered; const ready = new Promise(resolve => { entered = resolve; });
  const server = workbench({ dataDir: dir, kafka: {} }, { local: callbacks => { entered(); return adapter(callbacks); } });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}/api/`;
  const post = (path, body) => fetch(base+path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
  try {
    const pending = post('suites', { version: 1, name: 'API suite', scenarios: [{ ...scenario('first'), observeMs: 3000 }, scenario('second')] });
    await ready;
    assert.equal((await post('runs', scenario('conflict'))).status, 409);
    assert.equal((await post('suites', { version: 1, name: 'conflict', scenarios: [scenario('x')] })).status, 409);
    assert.equal((await (await fetch(base+'suite')).json()).status, 'running');
    assert.equal((await post('cancel', {})).status, 202);
    const report = await (await pending).json(); assert.equal(report.status, 'cancelled');
    assert.equal(report.cases[1].status, 'not-run');
    assert.equal((await (await fetch(base+'suites')).json()).length, 1);
  } finally { server.cancelActiveRun(); await new Promise(resolve => server.close(resolve)); await rm(dir, { recursive: true, force: true }); }
});
