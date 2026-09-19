// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { configuration } from '../src/config.js';
import { runScenario } from '../src/runner.js';
import { createStore } from '../src/store.js';
import { flinkJobs } from './example.js';
import { buildExperiment } from '../src/experiment.js';

const config = configuration();
const scenario = JSON.parse(await readFile('examples/scenarios/orders.kafka.json', 'utf8'));
const topology = JSON.parse(await readFile('examples/kafka-flink/topology.json', 'utf8'));
assert.ok((await flinkJobs()).some(j => j.name === 'streamplay-orders' && j.state === 'RUNNING'), 'Real Flink job must be running');
// Two consecutive runs prove we exclude previously observed output offsets.
for (let i = 0; i < 2; i++) {
  const run = await runScenario(scenario, { kafka: config.kafka, topology, store: createStore(config.dataDir) });
  console.log(JSON.stringify(run, null, 2));
  assert.equal(run.status, 'passed', run.error ?? JSON.stringify(run.assertion));
  assert.equal(run.inputs.length, 3);
  assert.equal(run.outputs.length, 2);
  assert.ok(run.outputs.every(record => record.transport === 'kafka' && record.offset !== undefined));
  assert.equal(run.topology.nodes.find(node => node.id === 'input').label, config.kafka.inputTopic);
  assert.equal(run.topology.nodes.find(node => node.id === 'output').label, config.kafka.outputTopic);
  assert.equal(run.topology.nodes.find(node => node.id === 'flink').label, 'Apache Flink · orders');
}
console.log('Verified two Kafka → Flink → Kafka runs, including duplicate counts and output offset boundaries.');

// A legitimate filtered input can produce no output; a deliberately wrong expectation must fail.
const filtered = await runScenario({ ...scenario, name: 'Filtered zero-quantity input', events: [{ order_id: 'filtered', quantity: 0, unit_price_cents: 1000 }], expected: [] }, { kafka: config.kafka, store: createStore(config.dataDir) });
assert.equal(filtered.status, 'passed', filtered.error);
const mismatch = await runScenario({ ...scenario, name: 'Detect an unexpected real output', events: [{ order_id: 'extra', quantity: 1, unit_price_cents: 1000 }], expected: [] }, { kafka: config.kafka, store: createStore(config.dataDir) });
assert.equal(mismatch.status, 'failed');
assert.equal(mismatch.assertion.added.length, 1);
console.log('Verified filtered output and a deliberately failing expectation against real Flink.');

const generated = buildExperiment({ name: 'Two entities with silence and zero values', adapter: 'kafka', observeMs: 5000,
  template: { order_id: 'template', quantity: 1, unit_price_cents: 100 }, deviceIds: ['sequence-a', 'sequence-b'],
  deviceField: 'order_id', valueField: 'quantity', phases: [
    { name: 'active', kind: 'emit', durationMs: 1000, intervalMs: 500, value: 2 },
    { name: 'quiet', kind: 'silence', durationMs: 500 },
    { name: 'zero', kind: 'emit', durationMs: 500, intervalMs: 500, value: 0 },
  ], expected: [{order_id:'sequence-a',total_cents:200},{order_id:'sequence-b',total_cents:200},{order_id:'sequence-a',total_cents:200},{order_id:'sequence-b',total_cents:200}] });
const generatedRun = await runScenario(generated, { kafka: config.kafka, topology, store: createStore(config.dataDir) });
assert.equal(generatedRun.status, 'passed', generatedRun.error ?? JSON.stringify(generatedRun.assertion));
assert.equal(generatedRun.inputs.length, 6);
assert.equal(generatedRun.delivery.samples.length, 6);
console.log('Verified generated multi-entity phased sequence against real Kafka and Flink.');
