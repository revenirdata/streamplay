#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
import { createWriteStream } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { once } from 'node:events';
import { runFleetSimulation } from './simulation/fleet.js';
import { assertTargetAllowed, mqttClientFactory } from './simulation/mqtt-client.js';

const args = process.argv.slice(2);
const allowRemote = args.includes('--allow-remote');
const allowProduction = args.includes('--allow-production');
const positional = args.filter(value => !value.startsWith('--'));
const profilePath = positional[0];
if (!profilePath) throw new Error('Usage: npm run simulate -- path/to/profile.json [output-directory] [--allow-remote]');
const outputDirectory = resolve(positional[1] ?? '.streamplay/simulations');
const profile = JSON.parse(await readFile(resolve(profilePath), 'utf8'));
await mkdir(outputDirectory, { recursive: true });
let ledgerPath, ledgerStream, writeQueue = Promise.resolve();

const summary = await runFleetSimulation(profile, {
  clientFactory(connection) {
    assertTargetAllowed(connection.url, { allowRemote, allowProduction });
    return mqttClientFactory(connection);
  },
  ledger(entry) {
    ledgerPath ??= resolve(outputDirectory, `${entry.runId}.ndjson`);
    ledgerStream ??= createWriteStream(ledgerPath, { flags: 'a', encoding: 'utf8' });
    writeQueue = writeQueue.then(async () => { if (!ledgerStream.write(`${JSON.stringify(entry)}\n`)) await once(ledgerStream, 'drain'); });
    return writeQueue;
  },
  onProgress({ logicalEvents, expected }) { process.stderr.write(`\r${logicalEvents}/${expected} logical events`); }
});
await writeQueue;
if (ledgerStream) { ledgerStream.end(); await once(ledgerStream, 'close'); }
process.stderr.write('\n');
const summaryPath = resolve(outputDirectory, `${summary.runId}.summary.json`);
await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ...summary, ledgerPath, summaryPath }, null, 2));
process.exitCode = summary.status === 'passed' ? 0 : 1;
