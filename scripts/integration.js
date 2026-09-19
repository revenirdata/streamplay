// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { configuration } from '../src/config.js';
import { runScenario } from '../src/runner.js';
import { createStore } from '../src/store.js';
import { flinkJobs } from './example.js';
import { buildExperiment } from '../src/experiment.js';
import { createLab } from '../src/lab/runtime.js';
import kafkaLab from '../examples/lab/kafka.js';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

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

const labDir = await mkdtemp(join(tmpdir(), 'streamplay-kafka-lab-'));
let lab;
try {
  lab = await createLab(kafkaLab, { dataDir: labDir });
  await lab.action('add', { id: 'second-order', rate: 2 });
  await lab.action('send', { id: 'device-001' });
  await lab.action('send', { id: 'second-order' });
  const deadline = Date.now() + 15000;
  while ((await lab.snapshot()).outputs.length < 2 && Date.now() < deadline) await delay(100);
  const snapshot = await lab.snapshot();
  assert.equal(snapshot.inputs.length, 2);
  assert.deepEqual(snapshot.outputs.map(r => r.value).sort((a, b) => a.order_id.localeCompare(b.order_id)), [
    { order_id: 'device-001', total_cents: 1000 }, { order_id: 'second-order', total_cents: 2000 }
  ]);
  console.log('Verified generic live-source profiles through real Kafka → Flink → Kafka, including both source identities and calculated totals.');
} finally { await lab?.close(); await rm(labDir, { recursive: true, force: true }); }
