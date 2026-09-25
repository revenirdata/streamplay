// SPDX-License-Identifier: Apache-2.0
import { readFile } from 'node:fs/promises';
import { estimateKinesisCapacity, experimentCoverage, validateExperimentPlan } from './experiment-plan.js';

const path = process.argv[2];
if (!path) throw new Error('Usage: npm run plan -- path/to/experiment.json');
const plan = validateExperimentPlan(JSON.parse(await readFile(path, 'utf8')));
console.log(JSON.stringify({ plan, capacity: estimateKinesisCapacity(plan), coverage: experimentCoverage }, null, 2));
