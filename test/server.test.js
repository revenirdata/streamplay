// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { request } from 'node:http';
import { workbench } from '../src/server.js';

test('local API persists runs, rejects concurrent runs and hostile browser requests', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'streamplay-http-'));
  const server = workbench({ port: 0, dataDir: dir, kafka: { brokers: ['localhost:19092'], inputTopic: 'in', outputTopic: 'out' } });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(async () => { await new Promise(resolve => server.close(resolve)); await rm(dir, { recursive: true, force: true }); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const sample = JSON.parse(await readFile(new URL('../examples/scenarios/orders.process.json', import.meta.url), 'utf8'));
  sample.observeMs = 1000;
  const post = (headers = {}, body = sample) => fetch(`${base}/api/runs`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body) });
  assert.equal((await post({ Origin: 'https://hostile.example' })).status, 403);
  assert.equal((await post({ 'Content-Type': 'text/plain' })).status, 415);
  const hostileHostStatus = await new Promise((resolve, reject) => {
    const req = request(base, { headers: { Host: 'hostile.example' } }, res => { res.resume(); resolve(res.statusCode); });
    req.on('error', reject); req.end();
  });
  assert.equal(hostileHostStatus, 403);
  assert.equal((await post({}, { ...sample, events: [] })).status, 400);
  const first = post();
  for (let i = 0; i < 100; i++) {
    const response = await fetch(`${base}/api/config`);
    if ((await response.json()).active) break;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.equal((await post()).status, 409);
  const response = await first; assert.equal(response.status, 201);
  const run = await response.json(); assert.equal(run.status, 'observed', run.error);
  const history = await (await fetch(`${base}/api/runs`)).json();
  assert.equal(history[0].id, run.id);
  const home = await fetch(base); assert.equal(home.status, 200);
  assert.match(home.headers.get('content-security-policy'), /frame-ancestors 'none'/);
});
