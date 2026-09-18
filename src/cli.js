// SPDX-License-Identifier: Apache-2.0
import { readFile } from 'node:fs/promises';
import { configuration } from './config.js';
import { createStore } from './store.js';
import { runScenario } from './runner.js';
import { loadLocalAdapter } from './adapters/local.js';

try {
  const path = process.argv[2];
  if (!path) throw new Error('Usage: npm run run:scenario -- path/to/scenario.json');
  const config = configuration();
  const controller = new AbortController();
  const cancel = () => controller.abort(new Error('Run cancelled by operator.'));
  process.once('SIGINT', cancel);
  const run = await runScenario(JSON.parse(await readFile(path, 'utf8')), { store: createStore(config.dataDir), kafka: config.kafka, adapters: await loadLocalAdapter(config.adapterModule), signal: controller.signal });
  process.removeListener('SIGINT', cancel);
  console.log(JSON.stringify(run, null, 2));
  if (['failed', 'error', 'cancelled'].includes(run.status)) process.exitCode = 1;
} catch (error) { console.error(error.message); process.exitCode = 1; }
