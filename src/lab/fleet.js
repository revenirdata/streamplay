// SPDX-License-Identifier: Apache-2.0
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { renderSource, validateProfile, sourcePresets } from '../source-profile.js';

const number = (value, min, max, label) => {
  if (!Number.isFinite(value) || value < min || value > max) throw new Error(`${label} must be ${min}–${max}.`);
  return value;
};

export function createFleet({ publish, ledger, namespace, onInput = () => {}, onLog = () => {}, limit = 20, profile = sourcePresets.meter }) {
  if (!namespace) throw new Error('A transport-specific ledger namespace is required.');
  const devices = new Map();
  let closed = false;
  profile = validateProfile(profile);
  function device(id) { const item = devices.get(id); if (!item) throw new Error('Unknown device.'); return item; }
  function editable(id) { const item = device(id); if (item.task) throw new Error('Stop this device before changing its settings.'); return item; }
  async function emit(item, rate, elapsedMs, phase) {
    const key = `${namespace}:${item.id}`;
    const total = ledger.get(key) + (profile.accumulate ? rate * elapsedMs / 60000 : 0);
    if (profile.accumulate) await ledger.reserve(key, total);
    const event = renderSource(profile, { id: item.id, eventId: randomUUID(), value: rate, total, index: item.sent, timestamp: new Date().toISOString() });
    const acknowledgement = await publish(event);
    onInput(event, { transport: acknowledgement, elapsedMs, phase, total: profile.accumulate ? total : null });
    item.sent++; item.total = total;
    return event;
  }
  async function phase(item, spec, signal) {
    const start = performance.now();
    let previous = 0;
    if (spec.kind === 'silence') { await delay(spec.durationMs, undefined, { signal }); return; }
    await emit(item, spec.value, 0, spec.name);
    while (previous < spec.durationMs) {
      await delay(Math.min(item.intervalMs, Math.max(0, spec.durationMs - (performance.now() - start))), undefined, { signal });
      signal.throwIfAborted();
      const elapsed = Math.min(spec.durationMs, performance.now() - start);
      await emit(item, spec.value, elapsed - previous, spec.name);
      previous = elapsed;
    }
  }
  function launch(item, phases) {
    if (closed) throw new Error('Fleet is closed.');
    if (item.task) throw new Error('Device is already sending.');
    const controller = new AbortController(); item.controller = controller;
    item.report = { id: randomUUID(), deviceId: item.id, phases: structuredClone(phases), startedAt: new Date().toISOString(), status: 'running', startTotal: ledger.get(`${namespace}:${item.id}`), sentBefore: item.sent };
    item.task = (async () => {
      try {
        for (const spec of phases) { item.phase = spec.name; await phase(item, spec, controller.signal); }
        item.report.status = 'published';
      } catch (error) { item.report.status = controller.signal.aborted ? 'cancelled' : 'error'; item.report.error = error.message; onLog(`${item.id}: ${error.message}`); }
      finally {
        item.phase = 'idle'; item.report.finishedAt = new Date().toISOString(); item.report.endTotal = ledger.get(`${namespace}:${item.id}`);
        item.report.sent = item.sent - item.report.sentBefore; item.report.meaning = 'Published input evidence, not proof of application output or physical volume.';
        item.task = null; item.controller = null;
      }
    })();
    return item.report;
  }
  return {
    snapshot: () => [...devices.values()].map(({ id, rate, intervalMs, sent, task, phase, report }) => ({ id, rate, intervalMs, sent, active: Boolean(task), phase, total: ledger.get(`${namespace}:${id}`), report })),
    get active() { return [...devices.values()].some(item => item.task); },
    get profile() { return profile; },
    setProfile(value) {
      if ([...devices.values()].some(item => item.task)) throw new Error('Stop sources before changing their profile.');
      profile = validateProfile(value);
    },
    add({ id = `device-${String(devices.size + 1).padStart(3, '0')}`, rate = 1, intervalMs = 1000 } = {}) {
      if (devices.size >= limit) throw new Error(`Fleet is limited to ${limit} devices.`);
      if (typeof id !== 'string' || !/^[\w-]{1,80}$/.test(id) || devices.has(id)) throw new Error('Use a unique device ID containing letters, numbers, underscores or hyphens.');
      number(rate, profile.accumulate ? 0 : -1000000, 1000000, 'Value'); number(intervalMs, 50, 60000, 'Interval');
      devices.set(id, { id, rate, intervalMs, sent: 0, task: null, phase: 'idle' }); return id;
    },
    configure({ id, rate, intervalMs, startTotal }) {
      const item = editable(id);
      const nextRate = number(rate ?? item.rate, profile.accumulate ? 0 : -1000000, 1000000, 'Value');
      const nextInterval = number(intervalMs ?? item.intervalMs, 50, 60000, 'Interval');
      if (startTotal !== undefined) throw new Error('Use setTotal while idle to reserve an explicit accumulated reading.');
      item.rate = nextRate; item.intervalMs = nextInterval;
    },
    async setTotal({ id, total }) { editable(id); if (!profile.accumulate) throw new Error('Totals apply only to accumulating meter profiles.'); await ledger.reserve(`${namespace}:${id}`, number(total, 0, Number.MAX_SAFE_INTEGER, 'Total')); },
    async send(id) { const item = editable(id); return emit(item, item.rate, 0, 'single'); },
    start(id, durationMs = 60000) { const item = editable(id); number(durationMs, 50, 600000, 'Session duration'); return launch(item, [{ name: 'telemetry', kind: 'emit', value: item.rate, durationMs }]); },
    episode({ id, baselineMs = 1000, stopMs = 1000, durationMs = 5000, targetQuantity, rawRate, unitsPerRawUnit = 1 }) {
      const item = editable(id);
      if (!profile.accumulate) throw new Error('Quantity episodes apply only to accumulating meter profiles. Use event sequences for other sources.');
      const rate = rawRate === undefined ? item.rate : number(rawRate, 0, 1000000, 'Raw rate') * number(unitsPerRawUnit, 0.000001, 1000000, 'Unit conversion');
      number(rate, 0, 1000000, 'Converted rate');
      if (targetQuantity !== undefined) { number(targetQuantity, Number.MIN_VALUE, 1000000, 'Target quantity'); if (rate <= 0) throw new Error('Target quantity requires a positive rate.'); durationMs = targetQuantity * 60000 / rate; }
      number(durationMs, 50, 600000, 'Flow duration'); number(baselineMs, 0, 60000, 'Baseline'); number(stopMs, 0, 60000, 'Stop phase');
      return launch(item, [{ name: 'baseline', kind: 'emit', value: 0, durationMs: baselineMs }, { name: 'active', kind: 'emit', value: rate, durationMs }, { name: 'stop', kind: 'emit', value: 0, durationMs: stopMs }].filter(p => p.durationMs > 0));
    },
    async stop(id) { const item = device(id); item.controller?.abort(new Error('Device stopped by operator.')); await item.task; },
    async close() { closed = true; for (const item of devices.values()) item.controller?.abort(new Error('Workbench shutdown.')); await Promise.all([...devices.values()].map(item => item.task)); }
  };
}
