// SPDX-License-Identifier: Apache-2.0
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { compare, validateScenario } from './scenario.js';
import { processAdapter } from './adapters/process.js';
import { kafkaAdapter } from './adapters/kafka.js';

export async function runScenario(input, { store, kafka, adapters = {}, onUpdate = () => {} } = {}) {
  const scenario = validateScenario(input);
  const run = { id: randomUUID(), startedAt: new Date().toISOString(), scenario, status: 'running', inputs: [], outputs: [], logs: [],
    observation: { complete: false, meaning: 'Records observed during a bounded interval, not proof that a stream is complete.' } };
  let adapter;
  let capture = true;
  let outputBytes = 0;
  let overflow = false;
  const callbacks = {
    onOutput(output) {
      if (!capture) return;
      outputBytes += Buffer.byteLength(output.raw ?? '');
      if (run.outputs.length >= 10_000 || outputBytes > 5_000_000) { overflow = true; return; }
      run.outputs.push(output);
    },
    onLog(message) { if (capture && run.logs.length < 500) run.logs.push(String(message).slice(0, 4000)); }
  };
  try {
    const factory = adapters[scenario.adapter] ?? (scenario.adapter === 'process' ? processAdapter : c => kafkaAdapter(c, kafka));
    adapter = await factory(callbacks);
    run.environment = adapter.metadata;
    onUpdate({ id: run.id, phase: 'sending' });
    await adapter.send(scenario.events, event => run.inputs.push(event));
    run.observation.startedAt = new Date().toISOString();
    onUpdate({ id: run.id, phase: 'observing' });
    await delay(scenario.observeMs);
    adapter.check();
    if (overflow) throw new Error('Output exceeded 10000 records or 5 MB; capture is incomplete. Use a smaller isolated scenario.');
    run.observation.complete = true;
    if (scenario.expected !== undefined) {
      run.assertion = compare(scenario.expected, run.outputs.map(r => r.value));
      run.status = run.assertion.equal ? 'passed' : 'failed';
    } else run.status = 'observed';
  } catch (error) {
    run.status = 'error';
    run.error = error.message;
  } finally {
    capture = false;
    run.observation.endedAt = new Date().toISOString();
    if (adapter) {
      try { await adapter.close(); } catch (error) { run.status = 'error'; run.error = `Cleanup failed: ${error.message}`; }
    }
    run.finishedAt = new Date().toISOString();
  }
  if (store) await store.save('runs', run);
  return run;
}
