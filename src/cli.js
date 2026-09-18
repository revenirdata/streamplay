// SPDX-License-Identifier: Apache-2.0
import { readFile } from 'node:fs/promises';
import { configuration } from './config.js';
import { createStore } from './store.js';
import { runScenario } from './runner.js';

try {
  const path = process.argv[2];
  if (!path) throw new Error('Usage: npm run run:scenario -- path/to/scenario.json');
  const config = configuration();
  const run = await runScenario(JSON.parse(await readFile(path, 'utf8')), { store: createStore(config.dataDir), kafka: config.kafka });
  console.log(JSON.stringify(run, null, 2));
  if (['failed', 'error'].includes(run.status)) process.exitCode = 1;
} catch (error) { console.error(error.message); process.exitCode = 1; }
