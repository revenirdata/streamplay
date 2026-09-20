// SPDX-License-Identifier: Apache-2.0
import { readFile } from 'node:fs/promises';
import { configuration } from './config.js';
import { createStore } from './store.js';
import { runSuite } from './suite.js';
import { loadLocalAdapter } from './adapters/local.js';
import { validateTopology } from './topology.js';

try {
  if (!process.argv[2]) throw new Error('Usage: npm run run:suite -- path/to/suite.json');
  const config = configuration(), controller = new AbortController();
  const cancel = () => controller.abort(new Error('Suite cancelled by operator.'));
  process.once('SIGINT', cancel);
  const topology = config.topologyFile ? validateTopology(JSON.parse(await readFile(config.topologyFile, 'utf8'))) : undefined;
  const suite = await runSuite(JSON.parse(await readFile(process.argv[2], 'utf8')), {
    store: createStore(config.dataDir), kafka: config.kafka, topology,
    adapters: await loadLocalAdapter(config.adapterModule), signal: controller.signal });
  process.removeListener('SIGINT', cancel);
  console.log(JSON.stringify(suite, null, 2));
  if (suite.status !== 'passed') process.exitCode = 1;
} catch (error) { console.error(error.message); process.exitCode = 1; }
