// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { Aedes } from 'aedes';
import { createServer } from 'node:net';
import { runFleetSimulation } from '../src/simulation/fleet.js';
import { assertTargetAllowed, mqttClientFactory } from '../src/simulation/mqtt-client.js';
import { deterministicReading, renderTemplate, validateFleetProfile } from '../src/simulation/profile.js';

const profile = {
  version: 1, name: 'Two candidate bake-off', runId: 'repeatable-run', seed: 7,
  devices: { count: 4, idPrefix: 'meter-', start: 1, width: 2 },
  traffic: { messagesPerDevice: 3, intervalMs: 0, jitterMs: 0, rampUpMs: 0, connectConcurrency: 2 },
  faults: { duplicateEvery: 5, reconnectEveryDevice: 2, reconnectAfterMessage: 2, reconnectDelayMs: 0 },
  reading: { min: 1, max: 9, decimals: 2 },
  counter: { initial: 10, perDeviceOffset: 2, increment: .5, decimals: 3 },
  topicTemplate: 'devices/{{deviceId}}/events',
  payloadTemplate: { event_id: '{{eventId}}', device_id: '{{deviceId}}', sequence: '{{sequence}}', value: '{{reading}}' },
  targets: [{ id: 'old', url: 'mqtt://old:1883', qos: 1 }, { id: 'new', url: 'mqtt://new:1883', qos: 1 }]
};

test('profile is bounded, strips unknown fields and requires reconcilable IDs', () => {
  const validated = validateFleetProfile({ ...profile, command: 'unsafe' });
  assert.equal(validated.command, undefined);
  assert.throws(() => validateFleetProfile({ ...profile, devices: { ...profile.devices, count: 10_001 } }), /devices.count/);
  assert.throws(() => validateFleetProfile({ ...profile, payloadTemplate: { device_id: '{{deviceId}}' } }), /eventId/);
  assert.deepEqual(renderTemplate({ sequence: '{{sequence}}', label: 'event {{eventId}}' }, { sequence: 2, eventId: 'a' }), { sequence: 2, label: 'event a' });
  assert.equal(deterministicReading(validated, 2, 3), deterministicReading(validated, 2, 3));
});

test('remote and production-like MQTT targets require explicit operator intent', () => {
  assert.equal(assertTargetAllowed('mqtt://127.0.0.1:1883'), 'mqtt://127.0.0.1:1883');
  assert.throws(() => assertTargetAllowed('mqtts://candidate.example.com:8883'), /allow-remote/);
  assert.equal(assertTargetAllowed('mqtts://candidate.example.com:8883', { allowRemote: true }), 'mqtts://candidate.example.com:8883');
  assert.throws(() => assertTargetAllowed('mqtts://mqtt-prod.example.com:8883', { allowRemote: true }), /production-like/);
});

test('derives per-device HMAC credentials without storing the secret in a profile', async () => {
  const captures = [];
  const hmacProfile = { ...profile, devices: { ...profile.devices, count: 1 }, traffic: { ...profile.traffic, messagesPerDevice: 1 },
    faults: { ...profile.faults, duplicateEvery: 0, reconnectEveryDevice: 0, reconnectAfterMessage: 1 },
    targets: [{ id: 'hmac', urlEnv: 'TEST_BROKER', qos: 1, clientIdTemplate: '{{deviceId}}', usernameTemplate: '{{deviceId}}', hmacSecretEnv: 'TEST_SECRET' }] };
  const summary = await runFleetSimulation(hmacProfile, { env: { TEST_BROKER: 'mqtt://example:1883', TEST_SECRET: 'not-in-profile' }, sleep: async () => {},
    async clientFactory(connection) { captures.push(connection); return { async publish() {}, async close() {} }; } });
  assert.equal(summary.status, 'passed');
  assert.equal(captures[0].options.clientId, 'meter-01');
  assert.equal(captures[0].options.username, 'meter-01');
  assert.ok(Buffer.isBuffer(captures[0].options.password));
  assert.equal(JSON.stringify(hmacProfile).includes('not-in-profile'), false);
});

test('publishes identical event IDs to both targets and measures duplicates and reconnects', async () => {
  let clock = 1_000;
  const frames = [], ledger = [];
  const summary = await runFleetSimulation(profile, {
    now: () => ++clock,
    sleep: async () => {},
    async ledger(entry) { ledger.push(entry); },
    async clientFactory({ target, deviceId }) {
      return { async publish(topic, payload) { frames.push({ target: target.id, deviceId, topic, payload: JSON.parse(payload) }); }, async close() {} };
    }
  });
  assert.equal(summary.status, 'passed');
  assert.equal(summary.logicalEvents, 12);
  assert.equal(summary.duplicatePublishes, 2);
  assert.equal(summary.expectedPublishesPerTarget, 14);
  assert.deepEqual(summary.targets.map(target => target.acknowledged), [14, 14]);
  assert.ok(summary.targets.every(target => target.acknowledgedBytes > 0));
  assert.deepEqual(summary.targets.map(target => target.reconnects), [2, 2]);
  const byTarget = id => frames.filter(frame => frame.target === id).map(frame => frame.payload.event_id);
  assert.deepEqual(byTarget('old'), byTarget('new'));
  assert.equal(ledger.length, 28);
  assert.ok(ledger.every(entry => entry.status === 'acknowledged'));
});

test('pads generated payloads to the requested minimum byte size', async () => {
  let payload;
  const sized = { ...profile, devices: { ...profile.devices, count: 1 }, traffic: { ...profile.traffic, messagesPerDevice: 1, targetPayloadBytes: 2048 }, faults: { ...profile.faults, duplicateEvery: 0, reconnectEveryDevice: 0, reconnectAfterMessage: 1 }, targets: [profile.targets[0]] };
  const summary = await runFleetSimulation(sized, { sleep: async () => {}, async clientFactory() { return { async publish(_topic, value) { payload = value; }, async close() {} }; } });
  assert.equal(Buffer.byteLength(payload), 2048);
  assert.equal(summary.targets[0].acknowledgedBytes, 2048);
});

test('reports publish loss as a failed comparison target', async () => {
  let attempts = 0;
  const summary = await runFleetSimulation({ ...profile, targets: [profile.targets[0]], faults: { ...profile.faults, duplicateEvery: 0, reconnectEveryDevice: 0 } }, {
    sleep: async () => {},
    async clientFactory() { return { async publish() { if (++attempts === 4) throw new Error('simulated loss'); }, async close() {} }; }
  });
  assert.equal(summary.status, 'failed');
  assert.equal(summary.targets[0].publishFailures, 1);
  assert.match(summary.targets[0].errors[0].message, /simulated loss/);
});

test('connects real MQTT clients and receives QoS acknowledgements from an ephemeral broker', async t => {
  const broker = await Aedes.createBroker();
  const server = createServer(broker.handle);
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  t.after(async () => {
    await new Promise(resolve => server.close(resolve));
    await broker.close();
  });
  const port = server.address().port;
  const actual = { ...profile, devices: { ...profile.devices, count: 3 }, traffic: { ...profile.traffic, messagesPerDevice: 2 },
    faults: { ...profile.faults, duplicateEvery: 3, reconnectEveryDevice: 2, reconnectAfterMessage: 2 },
    targets: [{ id: 'ephemeral', url: `mqtt://127.0.0.1:${port}`, qos: 1 }] };
  const summary = await runFleetSimulation(actual, { clientFactory: mqttClientFactory, sleep: async () => {} });
  assert.equal(summary.status, 'passed');
  assert.equal(summary.logicalEvents, 6);
  assert.equal(summary.duplicatePublishes, 2);
  assert.equal(summary.targets[0].acknowledged, 8);
  assert.equal(summary.targets[0].reconnects, 1);
});
