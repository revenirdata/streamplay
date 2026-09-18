// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { configuration } from '../src/config.js';
import { runScenario } from '../src/runner.js';
import { createStore } from '../src/store.js';
import { flinkJobs } from './example.js';

const config = configuration();
const scenario = JSON.parse(await readFile('examples/scenarios/orders.kafka.json', 'utf8'));
assert.ok((await flinkJobs()).some(j => j.name === 'streamplay-orders' && j.state === 'RUNNING'), 'Real Flink job must be running');
// Two consecutive runs prove we exclude previously observed output offsets.
for (let i = 0; i < 2; i++) {
  const run = await runScenario(scenario, { kafka: config.kafka, store: createStore(config.dataDir) });
  console.log(JSON.stringify(run, null, 2));
  assert.equal(run.status, 'passed', run.error ?? JSON.stringify(run.assertion));
  assert.equal(run.inputs.length, 3);
  assert.equal(run.outputs.length, 2);
  assert.ok(run.outputs.every(record => record.transport === 'kafka' && record.offset !== undefined));
}
console.log('Verified two Kafka → Flink → Kafka runs, including duplicate counts and output offset boundaries.');

// A legitimate filtered input can produce no output; a deliberately wrong expectation must fail.
const filtered = await runScenario({ ...scenario, name: 'Filtered zero-quantity input', events: [{ order_id: 'filtered', quantity: 0, unit_price_cents: 1000 }], expected: [] }, { kafka: config.kafka, store: createStore(config.dataDir) });
assert.equal(filtered.status, 'passed', filtered.error);
const mismatch = await runScenario({ ...scenario, name: 'Detect an unexpected real output', events: [{ order_id: 'extra', quantity: 1, unit_price_cents: 1000 }], expected: [] }, { kafka: config.kafka, store: createStore(config.dataDir) });
assert.equal(mismatch.status, 'failed');
assert.equal(mismatch.assertion.added.length, 1);
console.log('Verified filtered output and a deliberately failing expectation against real Flink.');
