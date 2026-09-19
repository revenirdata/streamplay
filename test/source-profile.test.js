// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { sourcePresets, validateProfile, renderSource } from '../src/source-profile.js';
const context = { id: 'sensor-a', eventId: 'event-1', timestamp: 'test', index: 0, value: 4, total: 12 };
test('presets model non-meter events without injecting water-specific fields', () => {
  const temperature = validateProfile(sourcePresets.temperature);
  const event = renderSource(temperature, context);
  assert.ok(event.temperature >= 18 && event.temperature <= 26);
  assert.equal(event.total, undefined); assert.equal(event.rate_per_minute, undefined);
  assert.deepEqual(renderSource(temperature, context), event);
  assert.notEqual(renderSource(temperature, { ...context, id: 'b' }).temperature, event.temperature);
  const orders = validateProfile(sourcePresets.orders);
  assert.equal(renderSource(orders, context).amount_cents, 1500);
  assert.equal(renderSource(orders, { ...context, index: 1 }).amount_cents, 4200);
});
test('custom nested payloads retain their schema and support sequences and negative measurements', () => {
  const profile = validateProfile({ id: 'custom', label: 'Freezer', template: { metadata: { site: 'warehouse' }, readings: { unit: 'C' } }, identityPath: 'metadata.sensor', fields: [{ path: 'readings.temperature', kind: 'value' }, { path: 'sequence', kind: 'sequence', start: 100, step: 2 }] });
  assert.deepEqual(renderSource(profile, { ...context, value: -15, index: 3 }), { metadata: { site: 'warehouse', sensor: 'sensor-a' }, readings: { unit: 'C', temperature: -15 }, sequence: 106 });
});
test('profiles reject prototype paths, overlapping fields, impossible ranges and implicit totals', () => {
  for (const changes of [{ identityPath: '__proto__.polluted' }, { fields: [{ path: 'device_id.child', kind: 'value' }] }, { fields: [{ path: 'temperature', kind: 'random', min: 5, max: 1 }] }, { fields: [{ path: 'total', kind: 'total' }] }]) assert.throws(() => validateProfile({ ...sourcePresets.telemetry, ...changes }));
});
