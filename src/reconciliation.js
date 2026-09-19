// SPDX-License-Identifier: Apache-2.0
// Shared by the browser and Node. Counts alone are never evidence of end-to-end delivery.
export function reconcileDelivery(bundle) {
  if (bundle?.version !== 1 || !Array.isArray(bundle.stages) || bundle.stages.length < 2 || bundle.stages.length > 12 ||
      new Set(bundle.stages).size !== bundle.stages.length || bundle.stages.some(s => typeof s !== 'string' || !s || s.length > 120) ||
      !Array.isArray(bundle.entries) || bundle.entries.length > 100000 || !Number.isFinite(bundle.lateAfterMs) || bundle.lateAfterMs < 0) throw new Error('Invalid delivery evidence bundle.');
  const events = new Map(), receipts = new Map(), failures = [], markers = [];
  for (const entry of bundle.entries) {
    if (!entry || !Number.isFinite(entry.at)) throw new Error('Every evidence entry needs a local observation timestamp.');
    if (entry.kind === 'planned') {
      if (typeof entry.id !== 'string' || !entry.id || entry.id.length > 200 || events.has(entry.id)) throw new Error('Planned event IDs must be unique non-empty strings.');
      events.set(entry.id, entry);
    } else if (entry.kind === 'receipt') {
      if (!bundle.stages.includes(entry.stage) || typeof entry.id !== 'string') throw new Error('Receipt has an unknown stage or invalid event ID.');
      if (!receipts.has(entry.id)) receipts.set(entry.id, new Map());
      const stages = receipts.get(entry.id); if (!stages.has(entry.stage)) stages.set(entry.stage, []);
      stages.get(entry.stage).push(entry);
    } else if (entry.kind === 'failure') failures.push(entry);
    else if (entry.kind === 'marker') markers.push(entry);
    else if (!['attempt', 'closed'].includes(entry.kind)) throw new Error('Unknown delivery evidence entry.');
  }
  if (events.size > 10000) throw new Error('Delivery experiments are limited to 10,000 unique events.');
  const rows = [...events].map(([id, planned]) => {
    const stages = bundle.stages.map(stage => {
      const observed = receipts.get(id)?.get(stage) ?? [];
      const first = observed.length ? Math.min(...observed.map(r => r.at)) : null;
      return { stage, observations: observed.length, firstObservedAt: first,
        elapsedMs: first === null ? null : first - planned.at,
        status: first === null ? 'unaccounted' : first - planned.at > bundle.lateAfterMs ? 'delayed' : 'observed' };
    });
    return { id, sourceId: planned.sourceId, sequence: planned.sequence, plannedAt: planned.at,
      lastObservedStage: stages.filter(s => s.observations).at(-1)?.stage ?? null, stages };
  });
  const checkpoints = bundle.stages.map(stage => {
    const values = rows.map(row => ({ id: row.id, ...row.stages.find(s => s.stage === stage) }));
    return { stage, uniqueObserved: values.filter(v => v.observations).length,
      unaccountedIds: values.filter(v => !v.observations).map(v => v.id),
      delayedIds: values.filter(v => v.status === 'delayed').map(v => v.id),
      repeatedObservationIds: values.filter(v => v.observations > 1).map(v => v.id),
      extraObservations: values.reduce((sum, v) => sum + Math.max(0, v.observations - 1), 0) };
  });
  const unexpectedIds = [...receipts.keys()].filter(id => !events.has(id));
  const closedAt = bundle.entries.filter(e => e.kind === 'closed').at(-1)?.at ?? null;
  return { version: 1, runId: bundle.runId, planned: events.size, closedAt, lateAfterMs: bundle.lateAfterMs,
    status: closedAt === null ? 'observing' : !events.size || unexpectedIds.length || failures.length ? 'incomplete' : checkpoints.some(s => s.unaccountedIds.length) ? 'gaps-found' : 'accounted-for',
    checkpoints, rows, unexpectedIds, failures, markers,
    boundary: 'Unaccounted means no receipt in this evidence window, not proven permanent loss. Repeated observations may be transport redelivery. Each checkpoint must represent the same event identity; aggregate alert counts cannot substitute for telemetry receipts.' };
}
