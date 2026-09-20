// SPDX-License-Identifier: Apache-2.0
import { resolve } from 'node:path';
import { startRecoveryStack, recoveryCompose, runFlinkRecovery } from '../examples/lab/flink-recovery.js';
const controller = new AbortController();
for (const name of ['SIGINT', 'SIGTERM']) process.on(name, () => controller.abort(new Error('Operator cancelled')));
let last;
try {
  await startRecoveryStack();
  const report = await runFlinkRecovery({ directory: resolve('.streamplay/evidence/flink-recovery'), signal: controller.signal,
    onUpdate: value => { if (value.phase !== last) { console.log(value.phase); last = value.phase; } } });
  console.log(JSON.stringify(report, null, 2));
  if (report.status !== 'passed') process.exitCode = 1;
} finally { await recoveryCompose('down', '--volumes', '--remove-orphans'); }
