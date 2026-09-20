// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { buildExperiment, quantityDuration } from '../src/experiment.js';
import { runScenario } from '../src/runner.js';
import { validateScenario } from '../src/scenario.js';
import mqttAdapter from '../examples/adapters/mqtt.js';

test('MQTT configuration rejects unsafe endpoints and ambiguous topics before connecting', async () => {
  const callbacks={onOutput(){},onLog(){}};
  for(const env of [{}, {STREAMPLAY_MQTT_URL:'https://example.com'},
    {STREAMPLAY_MQTT_URL:'mqtt://user:secret@localhost'},
    {STREAMPLAY_MQTT_URL:'mqtt://localhost',STREAMPLAY_MQTT_INPUT_TOPIC:'same',STREAMPLAY_MQTT_OUTPUT_TOPIC:'same'},
    {STREAMPLAY_MQTT_URL:'mqtt://localhost',STREAMPLAY_MQTT_INPUT_TOPIC:'a/#',STREAMPLAY_MQTT_OUTPUT_TOPIC:'b'}]) await assert.rejects(mqttAdapter(callbacks,env));
});

const config = { name: 'Two entities', template: { order_id: 'unused', quantity: 2, unit_price_cents: 10 },
  deviceIds: ['a', 'b'], deviceField: 'order_id', valueField: 'quantity', observeMs: 1000,
  phases: [{ name: 'active', kind: 'emit', durationMs: 100, intervalMs: 50, value: 2 },
    { name: 'silence', kind: 'silence', durationMs: 100 },
    { name: 'zero', kind: 'emit', durationMs: 100, intervalMs: 50, value: 0 }] };

test('builder deterministically interleaves identities, preserving zero versus silence', () => {
  const first = buildExperiment(config);
  assert.deepEqual(first, buildExperiment(config));
  assert.deepEqual(first.events.map(e => e.order_id), ['a', 'b', 'a', 'b', 'a', 'b', 'a', 'b']);
  assert.deepEqual(first.scheduleMs, [0, 0, 50, 0, 150, 0, 50, 0]);
  assert.equal(first.tailMs, 50);
  assert.deepEqual(first.phaseNames, ['active', 'active', 'active', 'active', 'zero', 'zero', 'zero', 'zero']);
  assert.equal(config.template.order_id, 'unused');
  assert.equal(first.expected, undefined);
});

test('quantity plans are explicit rate arithmetic and reject impossible durations', () => {
  assert.equal(quantityDuration(60, 8), 8000);
  for (const pair of [[0, 1], [-1, 1], [1, Infinity], [1, 1000]]) assert.throws(() => quantityDuration(...pair));
  const scenario = buildExperiment({ ...config, phases: [{ name: 'quantity', kind: 'emit', value: 60, targetQuantity: 1, intervalMs: 500 }] });
  assert.equal(scenario.scheduleMs.reduce((a,b)=>a+b,0) + scenario.tailMs, 1000);
});

test('rejects unsafe paths, invalid identities, overlapping fields and excessive sequences', () => {
  for (const override of [{ deviceField: '__proto__.bad' }, { valueField: 'constructor.bad' }, { deviceField: 'quantity' },
    { deviceIds: ['a','a'] }, { deviceIds: Array.from({length:21}, (_,i)=>String(i)) },
    { phases: [{name:'too large',kind:'emit',durationMs:120000,intervalMs:50,value:1}] }]) assert.throws(() => buildExperiment({...config,...override}));
  assert.throws(() => validateScenario({ ...buildExperiment(config), tailMs: 120000 }), /120000/);
});

test('generated scheduled events execute in the real process adapter and retain timing evidence', async () => {
  const scenario = buildExperiment(config);
  scenario.expected = [{order_id:'a',total_cents:20},{order_id:'b',total_cents:20},{order_id:'a',total_cents:20},{order_id:'b',total_cents:20}];
  const run = await runScenario(scenario);
  assert.equal(run.status, 'passed', run.error);
  assert.equal(run.inputs.length, 8);
  assert.equal(run.delivery.samples.length, 8);
  assert.equal(run.delivery.samples[4].plannedMs, 200);
  assert.ok(run.delivery.samples.every(s => s.acknowledgedMs >= s.dispatchedMs && s.latenessMs >= 0));
  assert.equal(run.delivery.samples[4].phase, 'zero');
});

test('cancel during the final quiet period preserves partial evidence and closes the adapter', async () => {
  const controller = new AbortController(); let closed = false;
  const run = await runScenario({ ...buildExperiment(config), adapter:'local' }, {
    signal: controller.signal,
    onUpdate: update => { if(update.phase === 'quiet') controller.abort(new Error('stop during quiet period')); },
    adapters: { local: async () => ({ send: async (events,onSent) => { for(const value of events)onSent({value}); }, check(){}, async close(){closed=true;} }) },
  });
  assert.equal(run.status,'cancelled'); assert.equal(closed,true); assert.equal(run.observation.complete,false);
});
