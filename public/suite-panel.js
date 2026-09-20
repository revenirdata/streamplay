// SPDX-License-Identifier: Apache-2.0
import { createTestResults, downloadReport } from './test-results.js';
export function initializeSuitePanel(api, refresh) {
  const $ = id => document.getElementById(id);
  const view = createTestResults($('suite-results'));
  let saved = [], active = false, polling = false;
  const error = e => { $('suite-message').textContent = e.message; };
  async function load() {
    if (polling) return; polling = true;
    try {
      const [report, operation] = await Promise.all([api('suite'), api('active')]);
      active = Boolean(operation?.suite);
      $('suite-cancel').disabled = !active; $('suite-run').disabled = Boolean(operation) || !saved.length;
      if (report) view.render([report]);
    } catch (e) { error(e); } finally { polling = false; }
  }
  $('suite-run').onclick = async () => {
    try {
      const chosen = [...$('suite-choices').querySelectorAll('input:checked')].map(el => saved.find(s => s.id === el.value).scenario);
      if (!chosen.length) throw new Error('Select at least one saved scenario.');
      $('suite-message').textContent = 'Running selected scenarios sequentially…'; $('suite-run').disabled = true;
      const pending = api('suites', { version: 1, name: 'Saved scenario suite', scenarios: chosen });
      setTimeout(load, 100);
      const report = await pending; view.render([report]);
      $('suite-message').textContent = `${report.status.toUpperCase()} · report saved locally`; await refresh();
    } catch (e) { error(e); } finally { await load(); }
  };
  $('suite-cancel').onclick = async () => { try { await api('cancel', {}); $('suite-message').textContent = 'Cancelling current case; remaining cases stay NOT RUN.'; } catch (e) { error(e); } };
  $('suite-plan-export').onclick = () => {
    const scenarios = [...$('suite-choices').querySelectorAll('input:checked')].map(el => saved.find(s => s.id === el.value).scenario);
    if (!scenarios.length) return error(new Error('Select at least one saved scenario.'));
    downloadReport({ version: 1, name: 'Saved scenario suite', scenarios }, 'streamplay-suite.json');
  };
  void load(); setInterval(load, 1000);
  return { updateScenarios(values) {
    const selected = new Set([...$('suite-choices').querySelectorAll('input:checked')].map(el => el.value));
    const previous = new Set(saved.map(s => s.id)); saved = values;
    $('suite-choices').replaceChildren(...saved.map(item => {
      const label = document.createElement('label'), input = document.createElement('input'); input.type = 'checkbox'; input.value = item.id; input.checked = !previous.has(item.id) || selected.has(item.id);
      label.append(input, document.createTextNode(item.scenario.name)); return label;
    }));
    if (!saved.length) $('suite-message').textContent = 'Save scenarios from the event editor to build a suite.';
    $('suite-run').disabled = active || !saved.length;
  } };
}
