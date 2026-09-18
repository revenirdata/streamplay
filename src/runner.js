// SPDX-License-Identifier: Apache-2.0
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { assertOutputs, validateScenario } from './scenario.js';
import { processAdapter } from './adapters/process.js';
import { kafkaAdapter } from './adapters/kafka.js';

export async function runScenario(input, { store, kafka, adapters = {}, onUpdate = () => {}, signal } = {}) {
  const scenario = validateScenario(input);
  const run = { id: randomUUID(), startedAt: new Date().toISOString(), scenario, status: 'running', inputs: [], outputs: [], logs: [],
    observation: { complete: false, meaning: 'Records observed during a bounded interval, not proof that a stream is complete.' } };
  let adapter;
  let capture = true;
  let outputBytes = 0;
  let overflow = false;
  let phase = 'connecting';
  const update = () => onUpdate({ id: run.id, phase, run });
  const checkAbort = () => { if (signal?.aborted) throw signal.reason ?? new Error('Run cancelled.'); };
  const onSent = event => { run.inputs.push(event); update(); };
  const callbacks = {
    onOutput(output) {
      if (!capture) return false;
      outputBytes += Buffer.byteLength(output.raw ?? '');
      if (run.outputs.length >= 10_000 || outputBytes > 5_000_000) { overflow = true; return false; }
      run.outputs.push(output);
      update();
      return true;
    },
    onLog(message) { if (capture && run.logs.length < 500) { run.logs.push(String(message).slice(0, 4000)); update(); } },
    signal,
    scenario
  };
  try {
    update();
    checkAbort();
    const factory = adapters[scenario.adapter] ?? (scenario.adapter === 'process' ? processAdapter : scenario.adapter === 'kafka' ? c => kafkaAdapter(c, kafka) : undefined);
    if (!factory) throw new Error('Local adapter is not configured. Set STREAMPLAY_ADAPTER_MODULE to a trusted local module before starting.');
    adapter = await factory(callbacks);
    run.environment = adapter.metadata;
    checkAbort();
    phase = 'sending'; update();
    if (scenario.scheduleMs) {
      for (let i = 0; i < scenario.events.length; i++) {
        await delay(scenario.scheduleMs[i], undefined, { signal });
        checkAbort(); adapter.check();
        await adapter.send([scenario.events[i]], onSent);
      }
    } else await adapter.send(scenario.events, onSent);
    await adapter.finishInput?.();
    checkAbort();
    run.observation.startedAt = new Date().toISOString();
    phase = 'observing'; update();
    await delay(scenario.observeMs, undefined, { signal });
    adapter.check();
    if (overflow) throw new Error('Output exceeded 10000 records or 5 MB; capture is incomplete. Use a smaller isolated scenario.');
    run.observation.complete = true;
    if (scenario.expected !== undefined) {
      run.assertion = assertOutputs(scenario, run.outputs);
      run.status = run.assertion.equal ? 'passed' : 'failed';
    } else run.status = 'observed';
  } catch (error) {
    run.status = signal?.aborted ? 'cancelled' : 'error';
    run.error = error.message;
  } finally {
    capture = false;
    phase = 'closing'; update();
    run.observation.endedAt = new Date().toISOString();
    if (adapter) {
      try { await adapter.close(); } catch (error) { run.status = 'error'; run.error = `Cleanup failed: ${error.message}`; }
    }
    run.finishedAt = new Date().toISOString();
  }
  if (store) {
    try { await store.save('runs', run); }
    catch (error) { run.status = 'error'; run.error = `Could not save run: ${error.message}`; run.storage = { saved: false }; }
  }
  phase = 'finished'; update();
  return run;
}
