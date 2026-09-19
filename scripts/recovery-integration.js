// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { runRecoveryExperiment } from '../examples/lab/recovery-experiment.js';
import { readDeliveryAudit } from '../src/delivery-audit.js';
const directory = resolve('.streamplay/evidence/delivery'); await mkdir(directory, { recursive: true });
for (const persistent of [false, true]) {
  const bundle = await runRecoveryExperiment({ directory, persistent });
  const report = bundle.reconciliation;
  assert.equal(report.planned, 6); assert.equal(report.checkpoints[1].uniqueObserved, 6);
  assert.equal(report.checkpoints[2].uniqueObserved, persistent ? 6 : 3);
  assert.equal(report.checkpoints[2].unaccountedIds.length, persistent ? 0 : 3);
  assert.equal(report.checkpoints[2].extraObservations, 1);
  assert.ok(!persistent || report.checkpoints[2].delayedIds.length >= 3);
  assert.equal(report.status, persistent ? 'accounted-for' : 'gaps-found');
  assert.deepEqual((await readDeliveryAudit(resolve(directory, `${bundle.runId}.ndjson`))).reconciliation, report);
  await writeFile(resolve(directory, `${persistent ? 'recovered' : 'gaps'}.json`), JSON.stringify(bundle, null, 2));
  console.log(`PASS ${persistent ? 'persistent recovery' : 'ephemeral gaps'}: 6 unique broker-accepted IDs, ${report.checkpoints[2].uniqueObserved} observed downstream, ${report.checkpoints[2].unaccountedIds.length} unaccounted, 1 repeated observation.`);
}
