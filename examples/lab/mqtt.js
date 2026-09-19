// SPDX-License-Identifier: Apache-2.0
import mqttAdapter from '../adapters/mqtt.js';
export default async function create({ onOutput, onLog }) {
  const adapter = await mqttAdapter({ onOutput, onLog });
  return {
    name: 'MQTT sandbox application', namespace: `${adapter.metadata.broker}/${adapter.metadata.inputTopic}`,
    metadata: adapter.metadata,
    async publish(event) { let acknowledgement; await adapter.send([event], record => { acknowledgement = record.acknowledgement; }); return acknowledgement; },
    check: adapter.check, close: adapter.close,
    topology: { version: 1, adapter: 'local', nodes: [
      { id: 'devices', kind: 'source', label: 'Emulated devices', observe: 'inputs', identityPath: 'device_id' },
      { id: 'input', kind: 'topic', label: adapter.metadata.inputTopic, observe: 'inputs' },
      { id: 'app', kind: 'processor', label: 'Your MQTT application', observe: 'none' },
      { id: 'output', kind: 'topic', label: adapter.metadata.outputTopic, observe: 'outputs' }
    ], edges: [{ from: 'devices', to: 'input', observe: 'inputs' }, { from: 'input', to: 'app' }, { from: 'app', to: 'output', observe: 'outputs' }] }
  };
}
