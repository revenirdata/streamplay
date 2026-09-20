// SPDX-License-Identifier: Apache-2.0
import { resolve } from 'node:path';
import { workbench } from '../src/server.js';
import { startRecoveryStack, recoveryCompose, runFlinkRecovery } from '../examples/lab/flink-recovery.js';
const dataDir = resolve(process.env.STREAMPLAY_DATA_DIR ?? '.streamplay/flink-recovery-demo');
let active, controller, report, startupError;
function topology() {
  return { version: 1, adapter: 'local', nodes: [
    { id: 'sources', kind: 'source', label: 'Experiment sources', identityPath: 'source_id', observe: 'inputs', detail: 'Source identities from actual input topic records.' },
    { id: 'input', kind: 'topic', label: report?.topics.inputTopic ?? 'Kafka input topic', observe: 'inputs', detail: 'Actual JSON read by an independent Kafka observer. This is not Flink consumption acknowledgement. Open transport metadata for partition and offset.' },
    { id: 'flink', kind: 'processor', label: 'Flink · count and sum by source', observe: 'none', detail: 'State tab: live checkpoint/restore evidence from Flink REST and totals observed at its sink. Logs tab: experiment phases. Internal per-record execution is not traced.' },
    { id: 'output', kind: 'topic', label: report?.topics.outputTopic ?? 'Kafka aggregate updates', observe: 'outputs', detail: 'Actual upsert JSON emitted by Flink. Multiple updates for one source are expected; these are running totals, not duplicated business events.' }
  ], edges: [{ from: 'sources', to: 'input', observe: 'inputs' }, { from: 'input', to: 'flink' }, { from: 'flink', to: 'output', observe: 'outputs' }] };
}
const application = {
  description: { name: 'Stateful Flink worker recovery', view: 'recovery', actions: [
    { id: 'run', label: 'Run worker recovery experiment', input: 'none' }, { id: 'cancel', label: 'Cancel experiment', input: 'none', allowWhileBusy: true }
  ] },
  get busy() { return active ? 'Recovery experiment' : null; },
  snapshot: () => ({ status: startupError ? 'error' : report?.status ?? 'ready', detail: startupError ?? report?.phase ?? 'This experiment kills and restarts its dedicated local Flink worker. Click pipeline nodes to inspect JSON and recovery evidence.',
    topology: topology(), inputs: report?.inputs ?? [], outputs: report?.outputs ?? [], logs: report?.logs ?? [], state: report?.state ?? {}, configuration: { operation: 'GROUP BY source_id: COUNT(*), SUM(amount)', checkpointInterval: '1 second', runtime: 'Flink 1.20.2', verificationBoundary: report?.scope }, reports: report ?? {} }),
  async act(action) {
    if (action === 'cancel') { controller?.abort(new Error('Cancelled by operator')); return { message: 'Cancellation requested; cleanup and evidence preservation continue.' }; }
    if (startupError) throw new Error(startupError);
    controller = new AbortController();
    active = runFlinkRecovery({ directory: dataDir, signal: controller.signal, onUpdate: value => { report = value; } })
      .catch(error => { startupError = error.message; }).finally(() => { active = null; });
    return { message: 'Experiment started. Click the input topic, Flink state, and output topic as it runs.' };
  }
};
try { await startRecoveryStack(); } catch (error) { await recoveryCompose('down', '--volumes'); throw error; }
const server = workbench({ dataDir, kafka: { brokers: [] }, topology: topology(), application });
server.listen(Number(process.env.STREAMPLAY_PORT ?? 4333), '127.0.0.1', () => console.log(`Flink recovery workbench: http://127.0.0.1:${server.address().port}`));
let stopping = false;
async function close() { if (stopping) return; stopping = true; controller?.abort(new Error('Workbench stopped')); await active; await new Promise(done => server.close(done)); await recoveryCompose('down', '--volumes', '--remove-orphans'); if (process.connected) process.disconnect(); }
for (const name of ['SIGINT', 'SIGTERM']) process.on(name, () => void close());
if (process.send) { process.on('message', value => { if (value === 'shutdown') void close(); }); process.on('disconnect', () => void close()); }
