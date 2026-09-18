// SPDX-License-Identifier: Apache-2.0
import { spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { record } from '../scenario.js';

const defaultFile = fileURLToPath(new URL('../../examples/process/transform.js', import.meta.url));

// This adapter runs one fixed, locally trusted example. It is not a Flink emulator.
export async function processAdapter({ onOutput, onLog }, file = defaultFile) {
  const source = await readFile(file);
  let failure;
  let finished = false;
  const child = spawn(process.execPath, [file], { stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true });
  child.stdin.on('error', error => { failure = error; });
  child.on('error', error => { failure = error; });
  const closed = new Promise(resolve => child.once('close', (code, signal) => {
    finished = true;
    if (code !== 0 && !signal) failure = new Error(`Example process exited with code ${code}.`);
    resolve();
  }));
  createInterface({ input: child.stdout }).on('line', line => onOutput(record(line, { transport: 'stdio' })));
  createInterface({ input: child.stderr }).on('line', onLog);
  return {
    metadata: { engine: 'Node.js example process', version: process.version, sourceSha256: createHash('sha256').update(source).digest('hex'), state: 'fresh process per run' },
    async send(events, onSent) {
      for (const value of events) {
        if (failure) throw failure;
        const raw = JSON.stringify(value);
        await new Promise((resolve, reject) => child.stdin.write(raw + '\n', error => error ? reject(error) : resolve()));
        onSent(record(raw, { transport: 'stdio', acknowledgement: 'written to stdin' }));
      }
      child.stdin.end();
    },
    check() { if (failure) throw failure; if (!finished) throw new Error('Example process did not finish within the observation window. Increase the window.'); },
    async close() { if (!finished) child.kill(); await closed; }
  };
}
