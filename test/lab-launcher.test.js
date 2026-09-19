// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('one-command launcher shuts down gracefully and releases its ledger on Windows and Linux', { timeout: 20000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'streamplay-launch-'));
  const child = spawn(process.execPath, ['scripts/lab.js'], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'], env: { ...process.env, STREAMPLAY_PORT: '0', STREAMPLAY_DATA_DIR: directory } });
  const exited = once(child, 'exit'); let output = '';
  child.stderr.on('data', data => { output += data; });
  try {
    const url = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Lab did not start: ${output}`)), 10000);
      child.once('error', error => { clearTimeout(timer); reject(error); });
      child.stdout.on('data', data => { output += data; const match = output.match(/http:\/\/127\.0\.0\.1:\d+/); if (match) { clearTimeout(timer); resolve(match[0]); } });
    });
    const response = await fetch(`${url}/api/lab`); assert.equal(response.status, 200);
    child.send('shutdown');
    assert.equal((await exited)[0], 0, output);
    await assert.rejects(access(join(directory, 'lab', 'meter-ledger.lock')), { code: 'ENOENT' });
  } finally {
    if (child.exitCode === null) { if (child.connected) child.send('shutdown'); await exited; }
    await rm(directory, { recursive: true, force: true });
  }
});
