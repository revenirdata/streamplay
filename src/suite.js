// SPDX-License-Identifier: Apache-2.0
import { randomUUID } from 'node:crypto';
import { validateScenario } from './scenario.js';
import { runScenario } from './runner.js';

export function validateSuite(value) {
  if (value?.version !== 1 || typeof value.name !== 'string' || !value.name.trim() || value.name.length > 120) throw new Error('Suite needs version 1 and a name (1–120 characters).');
  if (!Array.isArray(value.scenarios) || !value.scenarios.length || value.scenarios.length > 20) throw new Error('Choose 1–20 scenarios for the suite.');
  return { version: 1, name: value.name.trim(), scenarios: value.scenarios.map(validateScenario) };
}

export function runAssertions(run) {
  const result = [];
  if (run.assertion) result.push({ name: 'Output records (values and duplicate counts)', expected: run.scenario.expected,
    actual: run.outputs.map(r => r.value), passed: run.assertion.equal, detail: run.assertion });
  for (const check of run.phaseAssertions ?? []) result.push({ name: `Phase: ${check.phase}`,
    expected: run.scenario.phaseExpectations.find(p => p.phase === check.phase).expected,
    actual: run.outputs.filter(o => o.scenarioPhase === check.phase).map(o => o.value), passed: check.equal, detail: check });
  return result;
}

export async function runSuite(input, options = {}) {
  const plan = validateSuite(input);
  const suite = { version: 1, id: randomUUID(), name: plan.name, startedAt: new Date().toISOString(), status: 'running', phase: 'starting',
    boundary: 'Each case uses its configured adapter and observation interval. Inspect run metadata for transport and state-isolation guarantees. Application code is not rebuilt by this runner.',
    cases: plan.scenarios.map((scenario, i) => ({ id: String(i + 1), name: scenario.name, scenario, status: 'not-run', assertions: [], report: null })) };
  const notify = run => options.onUpdate?.({ id: suite.id, phase: suite.phase, suite: structuredClone(suite), ...(run ? { run } : {}) });
  try {
    for (const entry of suite.cases) {
      if (options.signal?.aborted) break;
      entry.status = 'running'; suite.phase = entry.name; notify();
      const run = await runScenario(entry.scenario, { ...options, onUpdate: update => notify(update.run) });
      entry.report = run; entry.status = run.status; entry.assertions = runAssertions(run); notify();
      if (['error', 'cancelled'].includes(run.status)) break;
    }
    suite.status = options.signal?.aborted ? 'cancelled' : suite.cases.some(c => c.status === 'error') ? 'error'
      : suite.cases.some(c => ['failed', 'not-run', 'cancelled'].includes(c.status)) ? 'failed'
      : suite.cases.every(c => c.status === 'passed') ? 'passed' : 'observed';
  } catch (error) { suite.status = 'error'; suite.error = error.message; }
  suite.phase = 'finished'; suite.finishedAt = new Date().toISOString();
  try { await options.store?.save('suites', suite); }
  catch (error) { suite.status = 'error'; suite.error = `Could not save suite: ${error.message}`; }
  notify(); return suite;
}
