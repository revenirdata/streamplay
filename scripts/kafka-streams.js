// SPDX-License-Identifier: Apache-2.0
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { Kafka, logLevel } from 'kafkajs';
const exec = promisify(execFile);
export const compose = async (...args) => (await exec('docker', ['compose', '-p', 'streamplay-kafka-streams', '-f', 'examples/kafka-streams/compose.yaml', ...args], { windowsHide: true, timeout: 600000, maxBuffer: 8000000 })).stdout;
export async function ready() {
  for (let i=0;i<90;i++) {
    const running = JSON.parse(await compose('ps', '--format', 'json', 'application'));
    if (running.State !== 'running') throw new Error('Kafka Streams application stopped: '+await compose('logs','--tail','30','application'));
    const { stdout: started } = await exec('docker', ['inspect', '--format', '{{.State.StartedAt}}', running.ID], { windowsHide: true });
    if ((await compose('logs','--since',started.trim(),'application')).includes('STREAMPLAY_READY')) return;
    await delay(1000);
  }
  throw new Error('Kafka Streams readiness timed out');
}
export async function up() {
  await compose('build','application');
  await compose('up','-d','--wait','kafka');
  const admin = new Kafka({ brokers: ['localhost:19094'], logLevel: logLevel.ERROR }).admin();
  try { await admin.connect(); await admin.createTopics({ waitForLeaders: true, topics: ['streamplay-orders-in','streamplay-orders-out'].map(topic=>({topic,numPartitions:1,replicationFactor:1})) }); }
  finally { await admin.disconnect(); }
  await compose('up','-d','application'); await ready();
  console.log('Kafka Streams ready. Set STREAMPLAY_KAFKA_BROKERS=localhost:19094 and run npm start.');
}
if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  try {
    if(process.argv[2]==='up') await up();
    else if(process.argv[2]==='stop') await compose('stop');
    else if(process.argv[2]==='reset') { await compose('down','--volumes','--remove-orphans'); console.log('Removed only streamplay-kafka-streams containers, broker data and application state. Saved .streamplay runs remain. Run up to start fresh.'); }
    else throw new Error('Usage: node scripts/kafka-streams.js up|stop|reset');
  } catch(error) { console.error(error.message); process.exitCode=1; }
}
