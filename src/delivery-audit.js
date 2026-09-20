// SPDX-License-Identifier: Apache-2.0
import { mkdir, open, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { reconcileDelivery } from './reconciliation.js';

export async function createDeliveryAudit({ directory, stages, lateAfterMs = 1000, now = Date.now }) {
  const bundle = { version: 1, runId: randomUUID(), stages, lateAfterMs, entries: [] };
  reconcileDelivery(bundle);
  await mkdir(directory, { recursive: true });
  const path = join(directory, `${bundle.runId}.ndjson`), file = await open(path, 'wx');
  try { await file.writeFile(JSON.stringify({ ...bundle, entries: undefined, kind: 'manifest' }) + '\n'); await file.sync(); }
  catch (error) { await file.close(); throw error; }
  let pending = Promise.resolve(), closed = false;
  let bytes = 0;
  const plannedIds = new Set();
  function append(entry) {
    if (closed) return Promise.reject(new Error('Delivery journal is closed.'));
    const value = structuredClone({ ...entry, at: now() });
    pending = pending.then(async () => {
      const encoded = JSON.stringify(value) + '\n', size = Buffer.byteLength(encoded);
      if (bundle.entries.length >= 100000 || size > 16000 || bytes + size > 8000000) throw new Error('Delivery evidence limit exceeded.');
      if (value.kind === 'planned' && (typeof value.id !== 'string' || !value.id || value.id.length > 200 || plannedIds.has(value.id) || plannedIds.size >= 10000)) throw new Error('Planned IDs must be unique and bounded.');
      if (value.kind === 'receipt' && (!stages.includes(value.stage) || typeof value.id !== 'string' || !value.id || value.id.length > 200)) throw new Error('Invalid receipt checkpoint or event ID.');
      // Publish callers await this fsynced intent before sending.
      await file.writeFile(encoded); await file.sync(); bytes += size; bundle.entries.push(value);
      if (value.kind === 'planned') plannedIds.add(value.id);
    });
    return pending;
  }
  return { path,
    plan({ id, sourceId, sequence, payload }) { return append({ kind: 'planned', id, sourceId, sequence, payloadSha256: createHash('sha256').update(JSON.stringify(payload)).digest('hex') }); },
    attempt(id) { return append({ kind: 'attempt', id }); },
    receipt(stage, id, evidence = {}) { return append({ kind: 'receipt', stage, id, evidence }); },
    failure(id, message) { return append({ kind: 'failure', id, message: String(message).slice(0, 1000) }); },
    marker(message) { return append({ kind: 'marker', message: String(message).slice(0, 1000) }); },
    snapshot() { return { ...structuredClone(bundle), reconciliation: reconcileDelivery(bundle) }; },
    async flush() { await pending; },
    async close() {
      if (closed) return; const final = append({ kind: 'closed' }); closed = true;
      try { await final; } finally { await file.close(); }
    }
  };
}

export async function readDeliveryAudit(path) {
  const text = await readFile(path, 'utf8');
  if (!text.endsWith('\n')) throw new Error('Journal ends with an incomplete write; preserve it for recovery, do not certify delivery.');
  const [manifest, ...entries] = text.trimEnd().split('\n').map(line => JSON.parse(line));
  if (manifest.kind !== 'manifest') throw new Error('Missing delivery journal manifest.');
  const bundle = { ...manifest, entries }; delete bundle.kind;
  return { ...bundle, reconciliation: reconcileDelivery(bundle) };
}
