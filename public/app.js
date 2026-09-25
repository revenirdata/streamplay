// SPDX-License-Identifier: Apache-2.0
import { createPipelineGraph } from './graph.js';
import { buildExperiment, thresholdPhases } from './experiment.js';
import { validateScenario, parseNdjson } from './scenario.js';
import { initializeLab } from './lab.js';
import { createApplicationControls } from './application-controls.js';
import { initializeDeliveryAudit, showDeliveryAudit } from './reconciliation.js';
import { applicationTestReports, createTestResults } from './test-results.js';
import { initializeSuitePanel } from './suite-panel.js';
import { initializeExperimentDesigner } from './experiment-designer.js';
initializeDeliveryAudit();
initializeExperimentDesigner();
const $ = id => document.getElementById(id);
const graph = createPipelineGraph();
let config, current, runs = [], scenarios = [], tab = 'outputs';
let applicationSnapshot, applicationTab = 'outputs', applicationPending = false;
let lastApplicationConfiguration;
let applicationControls;
let phaseNames;
let phaseExpectations;
const pretty = value => JSON.stringify(value, null, 2);
async function api(path, value) {
  const response = await fetch(`/api/${path}`, value === undefined ? {} : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(value) });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error);
  return body;
}
const suitePanel = initializeSuitePanel(api, refresh);
const applicationResults = createTestResults($('application-test-results'));
function message(text) { $('message').textContent = text; }
function scenario() {
  const expected = $('expected').value.trim();
  const schedule = $('schedule').value.trim(), fields = $('fields').value.trim();
  return { version: 1, name: $('name').value, adapter: $('adapter').value, observeMs: Number($('window').value), events: JSON.parse($('events').value), ...(expected ? { expected: JSON.parse(expected) } : {}), ...(schedule ? { scheduleMs: JSON.parse(schedule) } : {}), ...(fields ? { matchFields: fields.split(',').map(s => s.trim()) } : {}), timingMode: $('timing-mode').value, tailMs: Number($('tail').value), ...(phaseNames ? { phaseNames } : {}), ...($('phase-expected').value.trim() ? { phaseExpectations: JSON.parse($('phase-expected').value) } : {}) };
}
function connection() {
  $('connection').textContent = $('adapter').value === 'process'
    ? 'Runs examples/process/transform.js in a fresh Node.js process. Edit that file and rerun. This example does not run Flink.'
    : $('adapter').value === 'local' ? 'Uses the trusted adapter selected when this workbench was started. Input/output transports and application state are recorded in run metadata.'
    : `${config.kafka.brokers.join(', ')} · ${config.kafka.inputTopic} → ${config.kafka.outputTopic}. Start your application first. Dedicated sandbox topics only; application state is retained.`;
}
function fill(value) {
  $('name').value = value.name; $('adapter').value = value.adapter; $('window').value = value.observeMs;
  $('events').value = pretty(value.events); $('expected').value = value.expected === undefined ? '' : pretty(value.expected); connection();
  $('schedule').value = value.scheduleMs ? pretty(value.scheduleMs) : ''; $('fields').value = value.matchFields?.join(', ') ?? '';
  $('timing-mode').value = value.timingMode ?? 'relative'; $('tail').value = value.tailMs ?? 0; phaseNames = value.phaseNames;
  phaseExpectations = value.phaseExpectations; $('phase-expected').value = phaseExpectations ? pretty(phaseExpectations) : '';
}
function pre(value) { const el = document.createElement('pre'); el.textContent = value; return el; }
function showRecords() {
  if (!current) return;
  const root = $('record-list'); root.replaceChildren();
  if (tab === 'metadata') { root.append(pre(pretty({ id: current.id, startedAt: current.startedAt, finishedAt: current.finishedAt, environment: current.environment, observation: current.observation, delivery: current.delivery, error: current.error }))); return; }
  if (tab === 'logs') { root.append(pre(current.logs.join('\n') || 'No process or adapter logs captured. Kafka mode does not yet collect application logs.')); return; }
  const values = current[tab];
  if (!values.length) { root.append(pre('No records captured in this run.')); return; }
  values.forEach((record, i) => {
    const article = document.createElement('article'); article.className = 'record';
    const head = document.createElement('div'); head.className = 'record-head';
    head.textContent = `#${String(i + 1).padStart(2, '0')} · ${record.topic ?? record.transport ?? 'stdio'}${record.partition === undefined ? '' : ` · partition ${record.partition} · offset ${record.offset}`}`;
    const details = document.createElement('details'); const summary = document.createElement('summary'); summary.textContent = 'Raw payload & record metadata';
    const { value, ...metadata } = record; details.append(summary, pre(pretty(metadata)));
    article.append(head, pre(record.json ? pretty(record.value) : record.raw), details); root.append(article);
  });
}
function showRun(run) {
  graph.showRun(run);
  current = run; $('status').textContent = run.status; $('status').dataset.state = run.status;
  $('input-count').textContent = run.inputs.length; $('output-count').textContent = run.outputs.length;
  $('duration').textContent = `${run.scenario.observeMs / 1000}s`;
  $('result-note').textContent = run.error ?? (run.status === 'running' ? 'Live capture in progress. Assertions are evaluated when the observation window ends.' : run.status === 'observed' ? 'Capture finished. No assertions were supplied; inspect these outputs to decide what to test.' : 'Assertions compare captured outputs over the declared interval. Later outputs and external application state are outside this check.');
  $('assertion').replaceChildren();
  if (run.assertion) $('assertion').append(pre(run.assertion.equal ? 'Expected output matched, including duplicate counts.' : pretty(run.assertion)));
  if (run.phaseAssertions) $('assertion').append(pre(pretty(run.phaseAssertions)));
  showRecords();
}
function options(id, values, placeholder, label) {
  const select = $(id); const previous = select.value; select.replaceChildren(new Option(placeholder, ''));
  for (const value of values) select.add(new Option(label(value), value.id));
  if (values.some(v => v.id === previous)) select.value = previous;
}
async function refresh() {
  [runs, scenarios] = await Promise.all([api('runs'), api('scenarios')]);
  const runLabel = r => `${r.scenario.name} · ${r.status} · ${new Date(r.startedAt).toLocaleTimeString()}`;
  options('history', runs, 'Choose a saved run', runLabel); options('baseline', runs, 'Choose a reference run', runLabel);
  options('saved', scenarios, 'Choose a saved scenario', s => s.scenario.name);
  suitePanel.updateScenarios(scenarios);
}
function safe(fn) { return async () => { try { await fn(); } catch (error) { message(error.message); } }; }
$('sequence-build').onclick = safe(() => {
  const value = buildExperiment({ name: $('name').value, adapter: $('adapter').value, observeMs: Number($('window').value),
    template: JSON.parse($('events').value)[0], deviceIds: $('sequence-devices').value.split(',').map(s => s.trim()),
    deviceField: $('sequence-identity').value, valueField: $('sequence-value').value, phases: JSON.parse($('sequence-phases').value) });
  fill(value); graph.preview(value.adapter);
  message(`Generated ${value.events.length} events. Inspect the JSON and add expected outputs to make this a test; no result is assumed.`);
});
$('threshold-build').onclick = safe(() => {
  $('sequence-phases').value = pretty(thresholdPhases(Number($('threshold-value').value), Number($('threshold-step').value), Number($('threshold-duration').value)));
  message('Below, exactly at, and above phases prepared. Generate the sequence, then define expected outputs for your application.');
});
$('events').addEventListener('input', () => { phaseNames = undefined; });
$('import-ndjson').onchange = safe(async () => {
  const file = $('import-ndjson').files[0]; if (!file) return;
  try {
    if (file.size > 512000) throw new Error('NDJSON must fit within 512 KB.');
    const events = parseNdjson(await file.text());
    $('events').value = pretty(events);
    // A new fixture cannot inherit the old fixture's timing or phase assertions.
    $('schedule').value = ''; $('tail').value = '0'; $('phase-expected').value = '';
    phaseNames = undefined; phaseExpectations = undefined;
    message(`Imported ${events.length} events. Duplicates and JSON values preserved. Review expected output before running.`);
  } finally { $('import-ndjson').value = ''; }
});
$('schedule').addEventListener('input', () => { phaseNames = undefined; });
$('import-scenario').onchange = safe(async () => {
  const file = $('import-scenario').files[0]; if (!file) return;
  if (file.size > 1000000) throw new Error('Import must fit within 1 MB.');
  const value = JSON.parse(await file.text());
  const scenario = validateScenario(value.scenario ?? value);
  fill(scenario); graph.preview(scenario.adapter);
  message('Settings imported. Review the adapter and expected output, then run. No connection settings or executable commands were imported.');
});
function showApplication() {
  if (!applicationSnapshot) return;
  const snapshot = applicationSnapshot;
  applicationResults.render(applicationTestReports(snapshot));
  if (snapshot.deliveryAudit) showDeliveryAudit(snapshot.deliveryAudit);
  applicationControls?.update(snapshot);
  applicationControls?.setDisabled(applicationPending);
  graph.update({ snapshot });
  $('application-status').textContent = snapshot.status;
  $('application-status').dataset.state = snapshot.status;
  $('application-detail').textContent = snapshot.detail ?? '';
  const value = snapshot[applicationTab];
  $('application-records').textContent = applicationTab === 'logs' ? (value ?? []).join('\n') : pretty(value ?? []);
  for (const button of $('application-actions').querySelectorAll('button')) button.disabled = applicationPending || (Boolean(snapshot.busy) && button.dataset.allowBusy !== 'true') || snapshot.runningScenario;
}
async function refreshApplication() {
  applicationSnapshot = await api('application');
  const next = pretty(applicationSnapshot.configuration);
  if (next && next !== lastApplicationConfiguration) {
    if ($('application-config').value === lastApplicationConfiguration) {
      $('application-config').value = next;
      if (applicationSnapshot.input) $('application-input').value = pretty(applicationSnapshot.input);
      if (applicationSnapshot.sample && $('events').value === pretty(config.sample.events)) { fill(applicationSnapshot.sample); config.sample = applicationSnapshot.sample; }
    }
    lastApplicationConfiguration = next;
  }
  showApplication();
}
function initializeApplication(description) {
  if (!description) return;
  if (description.view === 'recovery') {
    for (const element of document.querySelectorAll('.workspace, .history, #application-advanced, #delivery-audit, #scenario-suite')) element.hidden = true;
    document.querySelector('h1').textContent = 'Flink recovery test';
    document.querySelector('.intro .sub').textContent = 'Input records, checkpoints, and output records during a worker restart.';
  }
  if (description.view === 'delivery') {
    for (const element of document.querySelectorAll('.pipeline, .workspace, .history, #application-advanced, #application > .tabs, #application-records, #application-export, #scenario-suite')) element.hidden = true;
    document.querySelector('h1').textContent = 'Delivery checks';
    document.querySelector('.intro .sub').textContent = 'Published events and downstream observations by event ID.';
  }
  $('application').hidden = false; $('application-name').textContent = description.name;
  applicationControls = createApplicationControls($('application-controls'), description.controls);
  $('application-advanced').open = !description.controls?.length;
  $('application-config').value = pretty(description.configuration ?? {});
  lastApplicationConfiguration = $('application-config').value;
  $('application-input').value = pretty(description.input ?? []);
  for (const action of description.actions) {
    const button = document.createElement('button'); button.textContent = action.label;
    button.dataset.action = action.id;
    button.dataset.allowBusy = String(Boolean(action.allowWhileBusy));
    button.onclick = async () => {
      try {
        const value = action.input === 'configuration' ? JSON.parse($('application-config').value) : action.input === 'input' ? JSON.parse($('application-input').value) : action.input === 'controls' ? applicationControls.read() : undefined;
        applicationPending = true; showApplication(); $('application-message').textContent = `${action.label}…`;
        const result = await api('application', { action: action.id, value });
        $('application-message').textContent = result.message ?? 'Action complete.';
        if (result.configuration) $('application-config').value = pretty(result.configuration);
        if (result.input) $('application-input').value = pretty(result.input);
        if (result.sample) fill(result.sample);
        await refreshApplication();
        if (result.controls) applicationControls.fill(result.controls);
      } catch (error) { $('application-message').textContent = error.message; }
      finally { applicationPending = false; showApplication(); }
    };
    $('application-actions').append(button);
  }
  document.querySelectorAll('[data-application-tab]').forEach(button => { button.onclick = () => {
    applicationTab = button.dataset.applicationTab;
    document.querySelectorAll('[data-application-tab]').forEach(b => b.setAttribute('aria-selected', String(b === button)));
    showApplication();
  }; });
  let polling = false;
  const poll = async () => {
    if (polling) return; polling = true;
    try { await refreshApplication(); }
    catch (error) { $('application-status').textContent = 'unavailable'; $('application-detail').textContent = error.message; graph.update({ snapshot: { ...applicationSnapshot, status: 'unavailable', detail: error.message } }); }
    finally { polling = false; }
  };
  void poll(); setInterval(poll, 1000);
  $('application-export').onclick = () => {
    const url = URL.createObjectURL(new Blob([pretty(applicationSnapshot)], { type: 'application/json' }));
    const link = document.createElement('a'); link.href = url; link.download = 'streamplay-application-evidence.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
}
$('run').onclick = safe(async () => {
  const value = scenario(); $('run').disabled = true; $('status').textContent = 'Running'; $('status').dataset.state = 'running';
  $('cancel').disabled = false;
  $('compare').disabled = true;
  let live = true, polling = false;
  const poll = setInterval(async () => {
    if (polling) return;
    polling = true;
    try { const active = await api('active'); if (live && active?.run) { showRun({ ...active.run, status: 'running' }); message(`Run ${active.phase} · ${active.id}`); } } catch { /* final request reports failures */ }
    finally { polling = false; }
  }, 500);
  message('Connecting, sending events, and observing output…');
  try {
    const run = await api('runs', value); live = false;
    showRun(run); message(run.storage?.saved === false ? 'Run could not be saved. Export the captured run JSON now to retain it.' : `Run saved locally · ${run.id}`);
    void refresh().then(() => { if (current?.id === run.id) $('history').value = run.id; })
      .catch(error => message(`History could not be loaded: ${error.message}`));
  }
  finally { live = false; clearInterval(poll); $('run').disabled = false; $('cancel').disabled = true; $('compare').disabled = false; }
});
$('cancel').onclick = safe(async () => { await api('cancel', {}); message('Cancellation requested. Closing the adapter and preserving captured evidence…'); });
$('export-run').onclick = () => {
  if (!current) return message('Run a scenario or load a saved run first.');
  const url = URL.createObjectURL(new Blob([pretty(current) + '\n'], { type: 'application/json' }));
  const link = document.createElement('a'); link.href = url; link.download = `streamplay-run-${current.id ?? 'active'}.json`; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
};
$('save').onclick = safe(async () => { await api('scenarios', scenario()); await refresh(); message('Scenario saved locally. Export JSON to commit it with your application.'); });
$('export').onclick = safe(() => {
  const value = scenario(); const link = document.createElement('a'); const url = URL.createObjectURL(new Blob([pretty(value) + '\n'], { type: 'application/json' }));
  link.href = url; link.download = 'scenario.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); message('Scenario exported.');
});
$('reset').onclick = () => { fill(config.sample); graph.preview(config.sample.adapter); message('Example loaded.'); };
$('adapter').onchange = () => { if ($('adapter').value === 'kafka' && Number($('window').value) < 5000) $('window').value = 5000; connection(); graph.preview($('adapter').value); };
$('refresh').onclick = safe(refresh);
$('load-run').onclick = () => { const run = runs.find(r => r.id === $('history').value); if (run) { showRun(run); fill(run.scenario); message('Loaded the saved run and its original scenario.'); } else message('Choose a saved run first.'); };
$('load-scenario').onclick = () => { const saved = scenarios.find(s => s.id === $('saved').value); if (saved) { fill(saved.scenario); graph.preview(saved.scenario.adapter); message('Saved scenario loaded.'); } else message('Choose a saved scenario first.'); };
const canonical = value => Array.isArray(value) ? `[${value.map(canonical).join(',')}]` : value && typeof value === 'object' ? `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}` : JSON.stringify(value);
$('compare').onclick = () => {
  const selected = runs.find(r => r.id === $('history').value), reference = runs.find(r => r.id === $('baseline').value);
  if (!selected || !reference) return message('Choose both a saved run and a reference run.');
  const left = reference.outputs.map(r => canonical(r.value)); const added = [];
  for (const output of selected.outputs) { const index = left.indexOf(canonical(output.value)); if (index < 0) added.push(output.value); else left.splice(index, 1); }
  $('comparison').hidden = false;
  $('comparison').textContent = pretty({ reference: reference.id, selected: selected.id, outputsEqual: !added.length && !left.length, added, missing: left.map(JSON.parse), scenarioChanged: canonical(reference.scenario) !== canonical(selected.scenario), referenceEnvironment: reference.environment, selectedEnvironment: selected.environment });
};
document.querySelectorAll('[data-tab]').forEach(button => { button.onclick = () => { tab = button.dataset.tab; document.querySelectorAll('[data-tab]').forEach(b => b.setAttribute('aria-selected', String(b === button))); showRecords(); }; });
try {
  config = await api('config'); graph.update({ config, adapter: config.sample.adapter }); $('local-option').disabled = !config.localAdapter; fill(config.sample); initializeApplication(config.application); initializeLab(config.lab, { api, graph, config }); await refresh();
  if (config.active) {
    $('run').disabled = true; $('cancel').disabled = false; $('compare').disabled = true;
    let id = config.active.id, loaded = false;
    try {
      for (;;) {
        const active = await api('active');
        if (!active) break;
        id = active.id ?? id;
        if (active.run) {
          if (!loaded) { fill(active.run.scenario); loaded = true; }
          showRun({ ...active.run, status: 'running' }); message(`Reconnected to run · ${active.phase}`);
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      await refresh(); const saved = runs.find(run => run.id === id);
      if (saved) { showRun(saved); $('history').value = saved.id; message(`Run saved locally · ${saved.id}`); }
      else message('The run ended but no saved artifact was found. Check the server output.');
    } finally { $('run').disabled = false; $('cancel').disabled = true; $('compare').disabled = false; }
  }
} catch (error) { message(error.message); }
