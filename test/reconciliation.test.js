// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, appendFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { reconcileDelivery } from '../src/reconciliation.js';
import { createDeliveryAudit, readDeliveryAudit } from '../src/delivery-audit.js';

test('equal aggregate counts cannot hide a missing ID, an unexpected ID or repeated receipts', () => {
  const bundle = { version: 1, runId: 'a', stages: ['broker', 'sink'], lateAfterMs: 50, entries: [
    ...['a','b'].map(id => ({ kind: 'planned', id, at: 1 })),
    ...['a','b'].map(id => ({ kind: 'receipt', id, stage: 'broker', at: 2 })),
    { kind: 'receipt', id: 'a', stage: 'sink', at: 100 },
    { kind: 'receipt', id: 'a', stage: 'sink', at: 101 },
    { kind: 'receipt', id: 'stranger', stage: 'sink', at: 102 }, { kind: 'closed', at: 200 }
  ] };
  const report = reconcileDelivery(bundle);
  assert.deepEqual(report.checkpoints[1].unaccountedIds, ['b']);
  assert.deepEqual(report.checkpoints[1].delayedIds, ['a']);
  assert.deepEqual(report.checkpoints[1].repeatedObservationIds, ['a']);
  assert.deepEqual(report.unexpectedIds, ['stranger']); assert.equal(report.status, 'incomplete');
  assert.equal(report.rows[1].lastObservedStage, 'broker');
  assert.throws(() => reconcileDelivery({ ...bundle, entries: [...bundle.entries, { kind: 'planned', id: 'a', at: 1 }] }), /unique/);
});

test('durable intents precede publication and evidence reloads after close without hiding partial journals', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'streamplay-delivery-')); let clock = 1;
  const audit = await createDeliveryAudit({ directory, stages: ['attempted', 'accepted', 'sink'], now: () => clock++ });
  try {
    await audit.plan({ id: 'a', sourceId: 's', sequence: 1, payload: { value: 4 } });
    assert.match(await readFile(audit.path, 'utf8'), /payloadSha256/);
    await audit.attempt('a'); await audit.receipt('attempted', 'a'); await audit.receipt('accepted', 'a');
    assert.equal((await readDeliveryAudit(audit.path)).reconciliation.status, 'observing');
    await audit.close();
    const loaded = await readDeliveryAudit(audit.path);
    assert.equal(loaded.reconciliation.status, 'gaps-found');
    assert.deepEqual(loaded.reconciliation.checkpoints[2].unaccountedIds, ['a']);
    await assert.rejects(audit.receipt('sink', 'a'), /closed/);
    await appendFile(audit.path, '{');
    await assert.rejects(readDeliveryAudit(audit.path), /incomplete write/);
  } finally { await audit.close(); await rm(directory, { recursive: true, force: true }); }
});

test('open, empty and failed observations cannot become an accounted-for result', () => {
  const base = { version: 1, stages: ['broker','sink'], lateAfterMs: 1, entries: [] };
  assert.equal(reconcileDelivery(base).status, 'observing');
  assert.equal(reconcileDelivery({ ...base, entries: [{ kind: 'closed', at: 1 }] }).status, 'incomplete');
  assert.equal(reconcileDelivery({ ...base, entries: [{ kind: 'planned', id: 'a', at: 1 }, { kind: 'failure', id: 'a', at: 2 }, { kind: 'closed', at: 3 }] }).status, 'incomplete');
});
