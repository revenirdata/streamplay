// SPDX-License-Identifier: Apache-2.0
// Inspectable demonstration only: JavaScript threshold processing, not Flink or MQTT.
import { record } from '../../src/scenario.js';
export default async function create({ onOutput }) {
  let configuration = { threshold: 3 };
  const state = new Map();
  return {
    name: 'JavaScript sensor example (not Flink)', namespace: 'in-process-sensor-v1',
    async publish(event) {
      const id = String(event.device_id);
      const prior = state.get(id) ?? { received: 0 };
      state.set(id, { received: prior.received + 1, lastValue: event.value, above: event.value > configuration.threshold });
      if (event.value > configuration.threshold) onOutput(record(JSON.stringify({ device_id: id, event: 'above_threshold', value: event.value }), { transport: 'in-process' }));
      return 'JavaScript handler completed; no network or Flink involved';
    },
    configure(value) { if (!Number.isFinite(value.threshold)) throw new Error('Supply a finite threshold.'); configuration = { threshold: value.threshold }; return configuration; },
    state: () => ({ configuration, devices: Object.fromEntries(state) }),
    prepareScenario() { state.clear(); return { state: 'fresh example state', configuration }; },
    cleanupScenario() { state.clear(); },
    topology: { version: 1, adapter: 'local', nodes: [
      { id: 'devices', kind: 'source', label: 'Emulated devices', observe: 'inputs', identityPath: 'device_id' },
      { id: 'processor', kind: 'processor', label: 'JavaScript threshold function', observe: 'none' },
      { id: 'output', kind: 'sink', label: 'Threshold events', observe: 'outputs' }
    ], edges: [{ from: 'devices', to: 'processor', observe: 'inputs' }, { from: 'processor', to: 'output', observe: 'outputs' }] },
    checks: [{ name: 'StreamPlay regression suite', command: process.execPath, args: ['--test', 'test/experiment.test.js', 'test/runner.test.js'], cwd: new URL('../../', import.meta.url), timeoutMs: 60000, successPattern: '(?:# |ℹ )pass [1-9][0-9]*' }]
  };
}
