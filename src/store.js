// SPDX-License-Identifier: Apache-2.0
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

export function createStore(root) {
  function path(kind, id) {
    if (!['runs', 'scenarios'].includes(kind) || !/^[a-f0-9-]{36}$/.test(id)) throw new Error('Invalid artifact identifier.');
    return join(root, kind, `${id}.json`);
  }
  return {
    async save(kind, value) {
      const id = value.id ?? randomUUID();
      const file = path(kind, id);
      await mkdir(join(root, kind), { recursive: true });
      await writeFile(file, JSON.stringify({ ...value, id }, null, 2) + '\n', { flag: 'wx' });
      return id;
    },
    async get(kind, id) { return JSON.parse(await readFile(path(kind, id), 'utf8')); },
    async list(kind) {
      let files;
      try { files = await readdir(join(root, kind)); } catch (e) { if (e.code === 'ENOENT') return []; throw e; }
      return (await Promise.all(files.filter(f => /^[a-f0-9-]{36}\.json$/.test(f)).map(f => this.get(kind, f.slice(0, -5)))))
        .sort((a, b) => (b.startedAt ?? b.savedAt ?? '').localeCompare(a.startedAt ?? a.savedAt ?? ''));
    }
  };
}
