// SPDX-License-Identifier: Apache-2.0
const reserved = ['__proto__', 'prototype', 'constructor'];
function pathKeys(path) {
  if (typeof path !== 'string' || path.length > 120 || !/^[A-Za-z_]\w*(\.[A-Za-z_]\w*)*$/.test(path) || path.split('.').some(k => reserved.includes(k))) throw new Error('Profile fields need safe dot-separated JSON paths.');
  return path.split('.');
}
function put(object, path, value) {
  const keys = pathKeys(path); let current = object;
  for (const key of keys.slice(0, -1)) {
    current[key] ??= {};
    if (typeof current[key] !== 'object' || Array.isArray(current[key])) throw new Error('Profile path crosses a scalar or array.');
    current = current[key];
  }
  current[keys.at(-1)] = value;
}
export const sourcePresets = {
  telemetry: { id: 'numeric-telemetry', label: 'Numeric telemetry', template: {}, identityPath: 'device_id', eventIdPath: 'event_id', timestampPath: 'timestamp', fields: [{ path: 'value', kind: 'value' }], accumulate: false },
  temperature: { id: 'temperature-celsius', label: 'Temperature sensor', template: { unit: 'celsius' }, identityPath: 'device_id', eventIdPath: 'event_id', timestampPath: 'timestamp', fields: [{ path: 'temperature', kind: 'random', min: 18, max: 26 }], seed: 42, accumulate: false },
  orders: { id: 'order-events', label: 'Order events', template: { type: 'order.created', currency: 'USD' }, identityPath: 'producer_id', eventIdPath: 'event_id', timestampPath: 'created_at', fields: [{ path: 'amount_cents', kind: 'cycle', values: [1500, 4200, 9900] }, { path: 'quantity', kind: 'constant', value: 1 }], accumulate: false },
  meter: { id: 'meter-units', label: 'Accumulating meter', template: {}, identityPath: 'device_id', eventIdPath: 'event_id', timestampPath: 'timestamp', fields: [{ path: 'value', kind: 'value' }, { path: 'total', kind: 'total' }], accumulate: true }
};

export function validateProfile(value) {
  if (!value || typeof value.id !== 'string' || !/^[\w-]{1,80}$/.test(value.id) || typeof value.label !== 'string' || value.label.length > 120) throw new Error('Source profile requires an ID and label.');
  if (!value.template || typeof value.template !== 'object' || Array.isArray(value.template) || JSON.stringify(value.template).length > 32000) throw new Error('Profile template must be a JSON object under 32 KB.');
  pathKeys(value.identityPath);
  if (value.eventIdPath) pathKeys(value.eventIdPath);
  if (value.timestampPath) pathKeys(value.timestampPath);
  if (!Array.isArray(value.fields) || value.fields.length > 16) throw new Error('Use at most 16 generated fields.');
  const paths = [value.identityPath, value.eventIdPath, value.timestampPath].filter(Boolean);
  for (const field of value.fields) {
    pathKeys(field.path); paths.push(field.path);
    if (!['value', 'total', 'constant', 'random', 'cycle', 'sequence'].includes(field.kind)) throw new Error('Unknown generator kind.');
    if (field.kind === 'total' && !value.accumulate) throw new Error('A total field requires accumulating meter mode.');
    if (field.kind === 'random' && (!Number.isFinite(field.min) || !Number.isFinite(field.max) || field.min > field.max || !Number.isFinite(field.max - field.min))) throw new Error('Random range requires finite ordered bounds.');
    if (field.kind === 'cycle' && (!Array.isArray(field.values) || !field.values.length || field.values.length > 100)) throw new Error('Cycle requires 1–100 values.');
    if (field.kind === 'sequence' && (!Number.isFinite(field.start) || !Number.isFinite(field.step))) throw new Error('Sequence requires finite start and step.');
    if (field.kind === 'constant' && field.value === undefined) throw new Error('Constant requires a value.');
  }
  if (paths.some((p, i) => paths.some((q, j) => i !== j && (p === q || p.startsWith(q + '.') || q.startsWith(p + '.'))))) throw new Error('Generated field paths must not overlap.');
  const profile = structuredClone({ id: value.id, label: value.label, template: value.template, identityPath: value.identityPath, eventIdPath: value.eventIdPath, timestampPath: value.timestampPath,
    fields: value.fields, seed: Number.isInteger(value.seed) ? value.seed : 42, accumulate: Boolean(value.accumulate) });
  renderSource(profile, { id: 'validation', eventId: 'test', timestamp: 'test', index: 0, value: 0, total: 0 });
  return profile;
}

export function renderSource(profile, context) {
  const event = structuredClone(profile.template);
  put(event, profile.identityPath, context.id);
  if (profile.eventIdPath) put(event, profile.eventIdPath, context.eventId);
  if (profile.timestampPath) put(event, profile.timestampPath, context.timestamp);
  for (const field of profile.fields) {
    let value;
    if (field.kind === 'value') value = context.value;
    if (field.kind === 'total') value = context.total;
    if (field.kind === 'constant') value = field.value;
    if (field.kind === 'cycle') value = field.values[context.index % field.values.length];
    if (field.kind === 'sequence') value = field.start + context.index * field.step;
    if (field.kind === 'random') {
      let hash = 2166136261;
      for (const char of `${profile.seed}/${context.id}/${context.index}/${field.path}`) hash = Math.imul(hash ^ char.charCodeAt(0), 16777619);
      hash ^= hash >>> 16; hash = Math.imul(hash, 0x45d9f3b); hash ^= hash >>> 16;
      value = field.min + ((hash >>> 0) / 4294967296) * (field.max - field.min);
    }
    if (typeof value === 'number' && !Number.isFinite(value)) throw new Error('Generated numeric value is not finite.');
    put(event, field.path, structuredClone(value));
  }
  if (JSON.stringify(event).length > 50000) throw new Error('Generated event exceeds 50 KB.');
  return event;
}
