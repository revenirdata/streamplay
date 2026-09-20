// SPDX-License-Identifier: Apache-2.0
export function parseNdjson(text) {
  if (typeof text !== 'string' || new TextEncoder().encode(text).length > 512_000) throw new Error('NDJSON must fit within 512 KB.');
  const events = [];
  for (const [index, line] of text.replace(/^\uFEFF/, '').split(/\r?\n/).entries()) {
    if (!line.trim()) continue;
    try { events.push(JSON.parse(line)); }
    catch { throw new Error(`Invalid JSON on line ${index + 1}. No events imported.`); }
    if (events.length > 1000) throw new Error('Supply 1–1000 JSON events.');
  }
  if (!events.length) throw new Error('Supply 1–1000 JSON events.');
  if (JSON.stringify(events).length > 512_000) throw new Error('Events must fit within 512 KB.');
  return events;
}

export function validateScenario(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Scenario must be an object.');
  if (value.version !== 1) throw new Error('Scenario version must be 1.');
  if (typeof value.name !== 'string' || !value.name.trim() || value.name.length > 120) throw new Error('Name must be 1–120 characters.');
  if (!['process', 'kafka', 'local'].includes(value.adapter)) throw new Error('Choose process, kafka, or local.');
  if (!Array.isArray(value.events) || value.events.length < 1 || value.events.length > 1000) throw new Error('Supply 1–1000 JSON events.');
  if (JSON.stringify(value.events).length > 512_000) throw new Error('Events must fit within 512 KB.');
  if (!Number.isInteger(value.observeMs) || value.observeMs < 100 || value.observeMs > 60_000) throw new Error('Observation window must be 100–60000 ms.');
  if (value.expected !== undefined && (!Array.isArray(value.expected) || value.expected.length > 1000)) throw new Error('Expected output must be an array of up to 1000 records.');
  if (value.scheduleMs !== undefined && (!Array.isArray(value.scheduleMs) || value.scheduleMs.length !== value.events.length || value.scheduleMs.some(n => !Number.isInteger(n) || n < 0) || value.scheduleMs.reduce((a, b) => a + b, 0) > 120_000)) throw new Error('scheduleMs must contain one nonnegative delay per event, totaling at most 120000 ms.');
  if (value.timingMode !== undefined && !['relative', 'absolute'].includes(value.timingMode)) throw new Error('timingMode must be relative or absolute.');
  if (value.tailMs !== undefined && (!Number.isInteger(value.tailMs) || value.tailMs < 0 || value.tailMs + (value.scheduleMs ?? []).reduce((a, b) => a + b, 0) > 120000)) throw new Error('Schedule plus tailMs must total at most 120000 ms.');
  if (value.phaseNames !== undefined && (!Array.isArray(value.phaseNames) || value.phaseNames.length !== value.events.length || value.phaseNames.some(s => typeof s !== 'string' || !s.trim() || s.length > 60))) throw new Error('phaseNames must supply one name of 1–60 characters per event.');
  if (value.matchFields !== undefined && (!Array.isArray(value.matchFields) || !value.matchFields.length || value.matchFields.length > 20 || value.matchFields.some(s => typeof s !== 'string' || s.length > 120 || !/^[a-zA-Z_][\w]*(\.[a-zA-Z_][\w]*)*$/.test(s) || s.split('.').some(k => ['__proto__', 'constructor', 'prototype'].includes(k))))) throw new Error('matchFields must contain 1–20 ordinary dot-separated JSON field paths.');
  if (value.phaseExpectations !== undefined) {
    if (!Array.isArray(value.phaseExpectations) || !value.phaseExpectations.length || value.phaseExpectations.length > 20 || !value.phaseNames || !value.scheduleMs) throw new Error('Phase expectations require a named scheduled sequence and 1–20 expectations.');
    const names = new Set();
    for (const check of value.phaseExpectations) {
      if (!check || !value.phaseNames.includes(check.phase) || names.has(check.phase) || !Array.isArray(check.expected) || check.expected.length > 1000) throw new Error('Each expected phase must be unique, exist in phaseNames, and contain an expected-output array.');
      names.add(check.phase);
    }
    if (JSON.stringify(value.phaseExpectations).length > 512000) throw new Error('Phase expectations exceed 512 KB.');
  }
  // Copy only the public scenario contract. Connection settings and commands are never accepted from the browser.
  return structuredClone({ version: 1, name: value.name.trim(), adapter: value.adapter, events: value.events,
    observeMs: value.observeMs, ...(value.expected === undefined ? {} : { expected: value.expected }),
    ...(value.scheduleMs === undefined ? {} : { scheduleMs: value.scheduleMs }),
    ...(value.timingMode === undefined ? {} : { timingMode: value.timingMode }),
    ...(value.tailMs === undefined ? {} : { tailMs: value.tailMs }),
    ...(value.phaseNames === undefined ? {} : { phaseNames: value.phaseNames }),
    ...(value.phaseExpectations === undefined ? {} : { phaseExpectations: value.phaseExpectations.map(check => ({ phase: check.phase, expected: check.expected })) }),
    ...(value.matchFields === undefined ? {} : { matchFields: value.matchFields }) });
}

export function assertOutputs(scenario, outputs) {
  const invalidRecords = outputs.filter(r => r.json === false || r.tombstone === true).length;
  const project = value => {
    if (!scenario.matchFields) return value;
    return Object.fromEntries(scenario.matchFields.map(path => {
      let current = value;
      for (const key of path.split('.')) {
        if (current === null || typeof current !== 'object' || !Object.hasOwn(current, key)) throw new Error(`Missing comparison field: ${path}`);
        current = current[key];
      }
      return [path, current];
    }));
  };
  try {
    const result = compare(scenario.expected.map(project), outputs.map(r => project(r.value)));
    return { ...result, equal: result.equal && invalidRecords === 0, invalidRecords, matchFields: scenario.matchFields ?? null };
  } catch (error) { return { equal: false, error: error.message, invalidRecords, added: [], missing: [] }; }
}

export function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}

// Multiset comparison: object key order is irrelevant, duplicate counts are not.
export function compare(expected, actual) {
  const remaining = new Map();
  for (const value of expected) {
    const key = canonical(value);
    const entry = remaining.get(key) ?? { value, count: 0 };
    entry.count++;
    remaining.set(key, entry);
  }
  const added = [];
  for (const value of actual) {
    const entry = remaining.get(canonical(value));
    if (entry?.count) entry.count--;
    else added.push(value);
  }
  const missing = [...remaining.values()].flatMap(({ value, count }) => Array.from({ length: count }, () => value));
  return { equal: !added.length && !missing.length, added, missing };
}

export function record(raw, metadata = {}) {
  let value;
  let json = true;
  try { value = JSON.parse(raw); } catch { value = raw; json = false; }
  return { raw, value, json, observedAt: new Date().toISOString(), ...metadata };
}
