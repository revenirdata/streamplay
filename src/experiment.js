// SPDX-License-Identifier: Apache-2.0
import { validateScenario } from './scenario.js';

function field(path) {
  if (typeof path !== 'string' || path.length > 120 || !/^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)*$/.test(path) || path.split('.').some(k => ['__proto__', 'constructor', 'prototype'].includes(k))) throw new Error('Use ordinary dot-separated JSON field paths.');
  return path.split('.');
}
function set(object, keys, value) {
  let current = object;
  for (const key of keys.slice(0, -1)) {
    if (!Object.hasOwn(current, key)) current[key] = {};
    if (!current[key] || typeof current[key] !== 'object' || Array.isArray(current[key])) throw new Error('Field path crosses a scalar or array.');
    current = current[key];
  }
  current[keys.at(-1)] = value;
}

// Convert a rate in units/minute and a target quantity to a bounded duration. This is planning
// arithmetic, not a claim about a physical device or downstream application volume accounting.
export function quantityDuration(rate, quantity) {
  if (!Number.isFinite(rate) || rate <= 0 || !Number.isFinite(quantity) || quantity <= 0) throw new Error('Rate and target quantity must be positive finite numbers.');
  const ms = Math.ceil(quantity * 60000 / rate);
  if (ms < 1 || ms > 120000) throw new Error('Target quantity must produce a duration between 1 ms and 120 seconds.');
  return ms;
}

export function thresholdPhases(threshold, step, durationMs = 1000, intervalMs = 500) {
  if (!Number.isFinite(threshold) || !Number.isFinite(step) || step <= 0) throw new Error('Threshold must be finite and step must be positive.');
  return ['below', 'exactly-at', 'above'].map((name, index) => ({ name, kind: 'emit', value: threshold + (index - 1) * step, durationMs, intervalMs }));
}

export function buildExperiment({ name = 'Event sequence', adapter = 'process', observeMs = 1000, template,
  deviceIds, deviceField = 'device_id', valueField = 'value', phases, expected, matchFields }) {
  if (!template || typeof template !== 'object' || Array.isArray(template)) throw new Error('Template must be a JSON object.');
  if (new TextEncoder().encode(JSON.stringify(template)).length > 512000) throw new Error('Template exceeds the scenario payload limit.');
  if (!Array.isArray(deviceIds) || !deviceIds.length || deviceIds.length > 20 || deviceIds.some(id => typeof id !== 'string' || !id.trim() || id.length > 120) || new Set(deviceIds).size !== deviceIds.length) throw new Error('Supply 1–20 unique device IDs.');
  const identity = field(deviceField), value = field(valueField);
  if (deviceField === valueField || deviceField.startsWith(valueField + '.') || valueField.startsWith(deviceField + '.')) throw new Error('Device and value fields must not overlap.');
  if (!Array.isArray(phases) || !phases.length || phases.length > 20) throw new Error('Supply 1–20 phases.');
  const events = [], scheduleMs = [], phaseNames = [];
  let offset = 0, previous = 0, eventBytes = 0;
  for (const phase of phases) {
    const durationMs = phase.targetQuantity === undefined ? phase.durationMs : quantityDuration(phase.value, phase.targetQuantity);
    if (!Number.isInteger(durationMs) || durationMs < 1 || offset + durationMs > 120000) throw new Error('Phase durations must be positive integers, totaling at most 120000 ms.');
    if (typeof phase.name !== 'string' || !phase.name.trim() || phase.name.length > 60) throw new Error('Each phase needs a name of 1–60 characters.');
    if (!['emit', 'silence'].includes(phase.kind)) throw new Error('Phase kind must be emit or silence.');
    if (phase.kind === 'emit') {
      if (!Number.isFinite(phase.value)) throw new Error('Phase value must be finite.');
      if (!Number.isInteger(phase.intervalMs) || phase.intervalMs < 50 || phase.intervalMs > 60000) throw new Error('Emission interval must be 50–60000 ms.');
      const count = Math.ceil(durationMs / phase.intervalMs) * deviceIds.length;
      if (events.length + count > 1000) throw new Error('Sequence exceeds 1000 events; reduce devices, duration or frequency.');
      for (let at = offset; at < offset + durationMs; at += phase.intervalMs) {
        for (const id of deviceIds) {
          const event = structuredClone(template);
          set(event, identity, id); set(event, value, phase.value);
          eventBytes += new TextEncoder().encode(JSON.stringify(event)).length;
          if (eventBytes > 512000) throw new Error('Generated sequence exceeds the scenario payload limit.');
          events.push(event); scheduleMs.push(at - previous); phaseNames.push(phase.name); previous = at;
        }
      }
    }
    offset += durationMs;
  }
  return validateScenario({ version: 1, name, adapter, observeMs, events, expected, matchFields,
    scheduleMs, timingMode: 'absolute', tailMs: offset - previous, phaseNames });
}
