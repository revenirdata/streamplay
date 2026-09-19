// SPDX-License-Identifier: Apache-2.0
import { mkdir, open, readFile, unlink } from 'node:fs/promises';
import { join } from 'node:path';

// Reserve before publishing: uncertain delivery must never rewind an accumulated reading.
// One writer owns this journal. It is intentionally not a count of delivered physical units.
export async function openLedger(directory) {
  await mkdir(directory, { recursive: true });
  const lockPath = join(directory, 'meter-ledger.lock');
  const lock = await open(lockPath, 'wx').catch(() => { throw new Error(`Ledger is already locked. Stop the other workbench; after a crash inspect ${lockPath} before removing it.`); });
  await lock.writeFile(String(process.pid));
  const path = join(directory, 'meter-ledger.ndjson');
  const totals = new Map();
  let file;
  try {
    const contents = await readFile(path, 'utf8').catch(error => { if (error.code === 'ENOENT') return ''; throw error; });
    for (const line of contents.split('\n').filter(Boolean)) {
      const { key, total } = JSON.parse(line);
      if (typeof key !== 'string' || !Number.isFinite(total) || total < (totals.get(key) ?? 0)) throw new Error('Invalid cumulative meter journal; restore a valid copy before publishing.');
      totals.set(key, total);
    }
    file = await open(path, 'a');
  } catch (error) { await lock.close(); await unlink(lockPath); throw error; }
  let pending = Promise.resolve(), failed;
  return {
    get: key => totals.get(key) ?? 0,
    reserve(key, total) {
      const operation = pending.then(async () => {
        if (failed) throw failed;
        if (!Number.isFinite(total) || total < (totals.get(key) ?? 0)) throw new Error('Cumulative value cannot decrease or be nonfinite.');
        try { await file.writeFile(JSON.stringify({ key, total }) + '\n'); await file.sync(); }
        catch (error) { failed = error; throw error; }
        totals.set(key, total);
      });
      pending = operation.catch(() => {});
      return operation;
    },
    async close() { await pending; await file.close(); await lock.close(); await unlink(lockPath); }
  };
}
