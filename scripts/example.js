// SPDX-License-Identifier: Apache-2.0
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { setTimeout as delay } from 'node:timers/promises';

export async function compose(...args) {
  await new Promise((resolve, reject) => {
    const child = spawn('docker', ['compose', '-f', 'examples/kafka-flink/compose.yaml', ...args], { stdio: 'inherit', windowsHide: true });
    child.on('error', reject);
    child.on('exit', code => code === 0 ? resolve() : reject(new Error(`Docker Compose exited with ${code}.`)));
  });
}

export async function flinkJobs() {
  const response = await fetch('http://127.0.0.1:18081/jobs/overview', { signal: AbortSignal.timeout(3000) });
  if (!response.ok) throw new Error(`Flink returned ${response.status}.`);
  return (await response.json()).jobs;
}

async function waitFor(check, description) {
  let last;
  for (let i = 0; i < 60; i++) {
    try { if (await check()) return; } catch (error) { last = error; }
    await delay(2000);
  }
  throw new Error(`Timed out waiting for ${description}. ${last?.message ?? ''}`);
}

export async function up() {
  await compose('build', 'jobmanager');
  await compose('up', '-d', '--wait', 'kafka', 'jobmanager', 'taskmanager');
  const admin = new Kafka({ clientId: 'streamplay-example', brokers: ['localhost:19092'], logLevel: logLevel.ERROR }).admin();
  try {
    await admin.connect();
    await admin.createTopics({ waitForLeaders: true, topics: ['streamplay-orders-in', 'streamplay-orders-out'].map(topic => ({ topic, numPartitions: 1, replicationFactor: 1 })) });
  } finally { await admin.disconnect(); }
  await waitFor(async () => {
    const r = await fetch('http://127.0.0.1:18081/overview', { signal: AbortSignal.timeout(3000) });
    return (await r.json())['slots-available'] > 0;
  }, 'Flink task slots');
  const jobs = await flinkJobs();
  const existing = jobs.find(j => j.name === 'streamplay-orders' && j.state === 'RUNNING');
  if (!existing) await compose('run', '--rm', '-T', 'sql-client');
  await waitFor(async () => (await flinkJobs()).some(j => j.name === 'streamplay-orders' && j.state === 'RUNNING'), 'the orders job');
  console.log('Kafka → Flink → Kafka is running. Start the workbench with npm start, then select Kafka execution.');
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  try {
    if (process.argv[2] === 'up') await up();
    else if (process.argv[2] === 'down') await compose('down', '--volumes', '--remove-orphans');
    else throw new Error('Usage: node scripts/example.js up|down');
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
