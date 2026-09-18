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
