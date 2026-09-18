// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { workbench } from '../src/server.js';

test('trusted application controls reject unknown actions and serialize with scenario execution', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'streamplay-application-'));
  let release, started, calls = 0;
  const entered = new Promise(resolve => { started = resolve; });
  const application = { description: { name: 'Test application', actions: [{ id: 'start', label: 'Start' }] },
    snapshot: () => ({ status: 'starting' }),
    act: async () => { calls++; started(); await new Promise(resolve => { release = resolve; }); return { message: 'Ready' }; } };
  const server = workbench({ dataDir: directory, application, kafka: {} });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}/api/`;
  const post = (path, body, headers = {}) => fetch(base + path, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body) });
  try {
    assert.equal((await (await fetch(base + 'config')).json()).application.name, 'Test application');
    assert.equal((await post('application', { action: 'shell', value: 'anything' })).status, 400);
    assert.equal((await post('application', { action: 'start' }, { Origin: 'https://untrusted.example' })).status, 403);
    assert.equal(calls, 0);
    const pending = post('application', { action: 'start' }); await entered;
    assert.equal((await (await fetch(base + 'application')).json()).busy, 'start');
    assert.equal((await post('application', { action: 'start' })).status, 409);
    assert.equal((await post('runs', { version: 1, name: 'conflict', adapter: 'process', events: [{}], observeMs: 100 })).status, 409);
    release(); assert.equal((await pending).status, 200);
    assert.equal((await (await fetch(base + 'application')).json()).busy, null);
    application.act = async () => { throw new Error('startup failed'); };
    assert.equal((await post('application', { action: 'start' })).status, 400);
    assert.equal((await (await fetch(base + 'application')).json()).busy, null);
  } finally { release?.(); await new Promise(resolve => server.close(resolve)); await rm(directory, { recursive: true, force: true }); }
});
