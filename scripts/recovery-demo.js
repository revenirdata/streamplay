// SPDX-License-Identifier: Apache-2.0
import { resolve } from 'node:path';
import { writeFile } from 'node:fs/promises';
import { workbench } from '../src/server.js';
import { runRecoveryExperiment } from '../examples/lab/recovery-experiment.js';
const dataDir = resolve(process.env.STREAMPLAY_DATA_DIR ?? '.streamplay/recovery-demo');
let deliveryAudit, active, controller, failure;
const application = {
  description: { name: 'MQTT interruption and delivery accounting', view: 'delivery', controls: [
    { id: 'session', label: 'Subscriber recovery policy', type: 'select', value: 'ephemeral', options: [{ value: 'ephemeral', label: 'Ephemeral session: expose missing events' }, { value: 'persistent', label: 'Persistent session: recover queued events' }] }
  ], actions: [{ id: 'run', label: 'Run recovery experiment', input: 'controls' }, { id: 'cancel', label: 'Cancel recovery experiment', allowWhileBusy: true }] },
  get busy() { return active ? 'Recovery experiment' : null; },
  snapshot: () => ({ status: active ? 'running' : failure ? 'error' : 'ready', detail: failure ?? 'Real local MQTT. Interrupts a dedicated subscriber, compares broker acknowledgements to exact received IDs, and preserves an on-disk journal.', deliveryAudit }),
  async act(action, value) {
    if (action === 'cancel') { controller?.abort(new Error('Cancelled by operator.')); return { message: 'Cancellation requested; journal retained.' }; }
    if (active || !['ephemeral', 'persistent'].includes(value?.session)) throw new Error('Choose a session policy and wait for the current experiment.');
    controller = new AbortController(); failure = null;
    active = runRecoveryExperiment({ directory: dataDir, persistent: value.session === 'persistent', signal: controller.signal, onUpdate: value => { deliveryAudit = value; } })
      .then(bundle => writeFile(resolve(dataDir, `${bundle.runId}.json`), JSON.stringify(bundle, null, 2), { flag: 'wx' }))
      .catch(error => { failure = error.message; }).finally(() => { active = null; });
    return { message: 'Experiment started. Inspect checkpoint counts and exact event IDs below.' };
  }
};
const server = workbench({ dataDir, kafka: { brokers: [] }, application });
server.listen(Number(process.env.STREAMPLAY_PORT ?? 4317), '127.0.0.1', () => console.log(`Recovery workbench: http://127.0.0.1:${server.address().port}`));
let stopping = false;
async function close() { if (stopping) return; stopping = true; controller?.abort(new Error('Workbench shutdown.')); await active; server.cancelActiveRun(); await new Promise(done => server.close(done)); if (process.connected) process.disconnect(); }
for (const name of ['SIGINT', 'SIGTERM']) process.on(name, close);
if (process.send) { process.on('message', value => { if (value === 'shutdown') void close(); }); process.on('disconnect', close); }
