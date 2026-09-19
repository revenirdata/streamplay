// SPDX-License-Identifier: Apache-2.0
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
const args = process.argv.slice(2);
if (args.length && !(args.length === 2 && ['--module', '--example'].includes(args[0]))) throw new Error('Use npm run lab, --module <trusted-file>, or --example kafka-flink.');
const kafka = args[0] === '--example';
if (kafka && args[1] !== 'kafka-flink') throw new Error('The supported managed example is kafka-flink.');
const module = args[0] === '--module' ? resolve(args[1]) : resolve(kafka ? 'examples/lab/kafka.js' : 'examples/lab/in-process.js');
// A managed launch never adopts or removes an existing example project's containers.
const exampleEnv = { ...process.env, COMPOSE_PROJECT_NAME: `streamplay-lab-${randomUUID().slice(0, 8)}` };
async function example(action) {
  const child = spawn(process.execPath, ['scripts/example.js', action], { stdio: 'inherit', windowsHide: true, env: exampleEnv });
  const [code] = await once(child, 'exit'); if (code !== 0) throw new Error(`Example ${action} failed (${code}).`);
}
let server;
let stopping = false;
const stop = () => { if (!stopping) { stopping = true; if (server?.connected) server.send('shutdown'); } };
process.on('SIGINT', stop); process.on('SIGTERM', stop);
if (process.send) { process.on('message', message => { if (message === 'shutdown') stop(); }); process.on('disconnect', stop); }
try {
  if (kafka) await example('up');
  if (!stopping) {
    server = spawn(process.execPath, ['src/server.js'], { stdio: ['inherit', 'inherit', 'inherit', 'ipc'], windowsHide: true, env: { ...process.env, STREAMPLAY_LAB_MODULE: module } });
    const [code] = await once(server, 'exit'); process.exitCode = stopping ? 0 : code ?? 1;
  }
}
finally { if (kafka) await example('down'); }
if (process.connected) process.disconnect();
