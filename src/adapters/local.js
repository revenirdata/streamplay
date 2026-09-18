// SPDX-License-Identifier: Apache-2.0
import { pathToFileURL } from 'node:url';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';

// The operator selects executable code at startup. HTTP clients cannot select a module or command.
export async function loadLocalAdapter(path) {
  if (!path) return {};
  const sourceSha256 = createHash('sha256').update(await readFile(path)).digest('hex');
  const module = await import(pathToFileURL(path).href);
  if (typeof module.default !== 'function') throw new Error('Local adapter module must export a default async factory.');
  return { local: async context => {
    const adapter = await module.default(context);
    if (!adapter || !['send', 'check', 'close'].every(key => typeof adapter[key] === 'function')) {
      await adapter?.close?.();
      throw new Error('Local adapter must implement send, check, and close.');
    }
    return { ...adapter, metadata: { ...adapter.metadata, adapterSourceSha256: sourceSha256 } };
  } };
}
