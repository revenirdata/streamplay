// SPDX-License-Identifier: Apache-2.0
import { kafkaAdapter } from '../../src/adapters/kafka.js';
import { configuration } from '../../src/config.js';
export default async function create({ onOutput, onLog }) {
  const kafka = configuration().kafka;
  const adapter = await kafkaAdapter({ onOutput, onLog }, kafka);
  return {
    name: 'Kafka sandbox application', namespace: `${kafka.brokers.join(',')}/${kafka.inputTopic}`, metadata: adapter.metadata,
    profile: { id: 'order-lines', label: 'Order lines for the Kafka/Flink example', template: { unit_price_cents: 1000 }, identityPath: 'order_id', eventIdPath: 'event_id', fields: [{ path: 'quantity', kind: 'value' }], accumulate: false },
    async publish(event) { let evidence; await adapter.send([event], record => { evidence = { topic: record.topic, meaning: 'Kafka accepted input; downstream processing not implied' }; }); return evidence; },
    check: adapter.check, close: adapter.close,
    topology: { version: 1, adapter: 'local', nodes: [
      { id: 'sources', kind: 'source', label: 'Simulated sources', observe: 'inputs', identityPath: 'device_id' },
      { id: 'input', kind: 'topic', label: kafka.inputTopic, observe: 'inputs' },
      { id: 'app', kind: 'processor', label: 'Your Kafka application', observe: 'none' },
      { id: 'output', kind: 'topic', label: kafka.outputTopic, observe: 'outputs' }
    ], edges: [{ from: 'sources', to: 'input', observe: 'inputs' }, { from: 'input', to: 'app' }, { from: 'app', to: 'output', observe: 'outputs' }] }
  };
}
