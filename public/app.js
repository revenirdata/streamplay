// SPDX-License-Identifier: Apache-2.0
const $ = id => document.getElementById(id);
let config, current, runs = [], scenarios = [], tab = 'outputs';
const pretty = value => JSON.stringify(value, null, 2);
async function api(path, value) {
  const response = await fetch(`/api/${path}`, value === undefined ? {} : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(value) });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error);
  return body;
}
function message(text) { $('message').textContent = text; }
function scenario() {
  const expected = $('expected').value.trim();
  return { version: 1, name: $('name').value, adapter: $('adapter').value, observeMs: Number($('window').value), events: JSON.parse($('events').value), ...(expected ? { expected: JSON.parse(expected) } : {}) };
}
function connection() {
  $('connection').textContent = $('adapter').value === 'process'
    ? 'Runs examples/process/transform.js in a fresh Node.js process. Edit that file and rerun. This example does not run Flink.'
    : `${config.kafka.brokers.join(', ')} · ${config.kafka.inputTopic} → ${config.kafka.outputTopic}. Start your application first. Dedicated sandbox topics only; application state is retained.`;
}
function fill(value) {
  $('name').value = value.name; $('adapter').value = value.adapter; $('window').value = value.observeMs;
  $('events').value = pretty(value.events); $('expected').value = value.expected === undefined ? '' : pretty(value.expected); connection();
}
function pre(value) { const el = document.createElement('pre'); el.textContent = value; return el; }
function showRecords() {
  if (!current) return;
  const root = $('record-list'); root.replaceChildren();
  if (tab === 'metadata') { root.append(pre(pretty({ id: current.id, startedAt: current.startedAt, finishedAt: current.finishedAt, environment: current.environment, observation: current.observation, error: current.error }))); return; }
  if (tab === 'logs') { root.append(pre(current.logs.join('\n') || 'No process or adapter logs captured. Kafka mode does not yet collect application logs.')); return; }
  const values = current[tab];
  if (!values.length) { root.append(pre('No records captured in this run.')); return; }
  values.forEach((record, i) => {
    const article = document.createElement('article'); article.className = 'record';
    const head = document.createElement('div'); head.className = 'record-head';
    head.textContent = `#${String(i + 1).padStart(2, '0')} · ${record.topic ?? 'stdio'}${record.partition === undefined ? '' : ` · partition ${record.partition} · offset ${record.offset}`}`;
    const details = document.createElement('details'); const summary = document.createElement('summary'); summary.textContent = 'Raw payload & record metadata';
    const { value, ...metadata } = record; details.append(summary, pre(pretty(metadata)));
    article.append(head, pre(record.json ? pretty(record.value) : record.raw), details); root.append(article);
  });
}
function showRun(run) {
  current = run; $('status').textContent = run.status; $('status').dataset.state = run.status;
  $('input-count').textContent = run.inputs.length; $('output-count').textContent = run.outputs.length;
  $('duration').textContent = `${run.scenario.observeMs / 1000}s`;
  $('result-note').textContent = run.error ?? (run.status === 'observed' ? 'Capture finished. No assertions were supplied; inspect these outputs to decide what to test.' : 'Assertions compare all captured outputs over the declared interval. Later outputs and external application state are outside this check.');
  $('assertion').replaceChildren();
  if (run.assertion) $('assertion').append(pre(run.assertion.equal ? 'Expected output matched, including duplicate counts.' : pretty(run.assertion)));
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
}
function safe(fn) { return async () => { try { await fn(); } catch (error) { message(error.message); } }; }
$('run').onclick = safe(async () => {
  const value = scenario(); $('run').disabled = true; $('status').textContent = 'Running'; $('status').dataset.state = 'running';
  message('Connecting, sending events, and observing output…');
  try { const run = await api('runs', value); showRun(run); await refresh(); $('history').value = run.id; message(`Run saved locally · ${run.id}`); }
  finally { $('run').disabled = false; }
});
$('save').onclick = safe(async () => { await api('scenarios', scenario()); await refresh(); message('Scenario saved locally. Export JSON to commit it with your application.'); });
$('export').onclick = safe(() => {
  const value = scenario(); const link = document.createElement('a'); const url = URL.createObjectURL(new Blob([pretty(value) + '\n'], { type: 'application/json' }));
  link.href = url; link.download = 'scenario.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); message('Scenario exported.');
});
$('reset').onclick = () => { fill(config.sample); message('Example loaded.'); };
$('adapter').onchange = () => { if ($('adapter').value === 'kafka' && Number($('window').value) < 5000) $('window').value = 5000; connection(); };
$('refresh').onclick = safe(refresh);
$('load-run').onclick = () => { const run = runs.find(r => r.id === $('history').value); if (run) { showRun(run); fill(run.scenario); message('Loaded the saved run and its original scenario.'); } else message('Choose a saved run first.'); };
$('load-scenario').onclick = () => { const saved = scenarios.find(s => s.id === $('saved').value); if (saved) { fill(saved.scenario); message('Saved scenario loaded.'); } else message('Choose a saved scenario first.'); };
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
try { config = await api('config'); fill(config.sample); await refresh(); } catch (error) { message(error.message); }
