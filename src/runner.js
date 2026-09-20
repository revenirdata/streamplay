// SPDX-License-Identifier: Apache-2.0
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { assertOutputs, validateScenario } from './scenario.js';
import { processAdapter } from './adapters/process.js';
import { kafkaAdapter } from './adapters/kafka.js';
import { topologyFor } from './topology.js';
import { performance } from 'node:perf_hooks';

export async function runScenario(input, { store, kafka, adapters = {}, onUpdate = () => {}, signal, topology } = {}) {
  const scenario = validateScenario(input);
  const run = { id: randomUUID(), startedAt: new Date().toISOString(), scenario, status: 'running', inputs: [], outputs: [], logs: [],
    observation: { complete: false, meaning: 'Records observed during a bounded interval, not proof that a stream is complete.' } };
  let adapter;
  let capture = true;
  let outputBytes = 0;
  let overflow = false;
  let phase = 'connecting';
  let scenarioPhase = null;
  const update = () => onUpdate({ id: run.id, phase, run });
  const checkAbort = () => { if (signal?.aborted) throw signal.reason ?? new Error('Run cancelled.'); };
  const onSent = event => { run.inputs.push(event); update(); };
  const callbacks = {
    onOutput(output) {
      if (!capture) return false;
      outputBytes += Buffer.byteLength(output.raw ?? '');
      if (run.outputs.length >= 10_000 || outputBytes > 5_000_000) { overflow = true; return false; }
      run.outputs.push({ ...output, scenarioPhase });
      update();
      return true;
    },
    onLog(message) { if (capture && run.logs.length < 500) { run.logs.push(String(message).slice(0, 4000)); update(); } },
    signal,
    scenario
  };
  try {
    run.topology = topologyFor(scenario.adapter, topology, kafka);
    update();
    checkAbort();
    const factory = adapters[scenario.adapter] ?? (scenario.adapter === 'process' ? processAdapter : scenario.adapter === 'kafka' ? c => kafkaAdapter(c, kafka) : undefined);
    if (!factory) throw new Error('Local adapter is not configured. Set STREAMPLAY_ADAPTER_MODULE to a trusted local module before starting.');
    adapter = await factory(callbacks);
    run.environment = adapter.metadata;
    checkAbort();
    phase = 'sending'; update();
    if (scenario.scheduleMs) {
      const start = performance.now();
      let plannedMs = 0;
      run.delivery = { timingMode: scenario.timingMode ?? 'relative', samples: [] };
      for (let i = 0; i < scenario.events.length; i++) {
        plannedMs += scenario.scheduleMs[i];
        await delay(scenario.timingMode === 'absolute' ? Math.max(0, plannedMs - (performance.now() - start)) : scenario.scheduleMs[i], undefined, { signal });
        checkAbort(); adapter.check();
        const dispatchedMs = performance.now() - start;
        scenarioPhase = scenario.phaseNames?.[i] ?? null;
        await adapter.send([scenario.events[i]], onSent);
        run.delivery.samples.push({ index: i, phase: scenario.phaseNames?.[i] ?? null, plannedMs,
          dispatchedMs, acknowledgedMs: performance.now() - start, latenessMs: Math.max(0, dispatchedMs - plannedMs) });
      }
      if (scenario.tailMs) {
        phase = 'quiet'; update();
        const remaining = scenario.timingMode === 'absolute' ? Math.max(0, plannedMs + scenario.tailMs - (performance.now() - start)) : scenario.tailMs;
        await delay(remaining, undefined, { signal });
      }
    } else await adapter.send(scenario.events, onSent);
    if (!scenario.scheduleMs && scenario.tailMs) { phase = 'quiet'; update(); await delay(scenario.tailMs, undefined, { signal }); }
    await adapter.finishInput?.();
    checkAbort();
    run.observation.startedAt = new Date().toISOString();
    phase = 'observing'; update();
    await delay(scenario.observeMs, undefined, { signal });
    adapter.check({ complete: true });
    if (overflow) throw new Error('Output exceeded 10000 records or 5 MB; capture is incomplete. Use a smaller isolated scenario.');
    run.observation.complete = true;
    if (scenario.expected !== undefined) {
      run.assertion = assertOutputs(scenario, run.outputs);
      run.status = run.assertion.equal ? 'passed' : 'failed';
    } else run.status = 'observed';
    if (scenario.phaseExpectations) {
      run.phaseAssertions = scenario.phaseExpectations.map(check => ({ phase: check.phase,
        ...assertOutputs({ ...scenario, expected: check.expected }, run.outputs.filter(output => output.scenarioPhase === check.phase)) }));
      run.status = run.phaseAssertions.every(check => check.equal) && run.assertion?.equal !== false ? 'passed' : 'failed';
    }
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
