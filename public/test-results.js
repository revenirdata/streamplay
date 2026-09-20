// SPDX-License-Identifier: Apache-2.0
const pretty = value => JSON.stringify(value, null, 2);
const cellText = value => value === undefined ? '—' : typeof value === 'number' ? String(Number(value.toPrecision(10)))
  : typeof value === 'string' ? value : JSON.stringify(value);
export function downloadReport(report, filename) {
  const url = URL.createObjectURL(new Blob([pretty(report)], { type: 'application/json' }));
  const link = document.createElement('a'); link.href = url; link.download = filename; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
const element = (tag, text) => { const el = document.createElement(tag); if (text !== undefined) el.textContent = text; return el; };

export function applicationTestReports(snapshot) {
  const reports = snapshot?.reports ?? {};
  const result = [];
  if (reports.suite) result.push({ ...reports.suite, name: reports.suite.name ?? 'Application scenario suite' });
  if (reports.scenario && (!reports.suite || reports.scenario.startedAt > (reports.suite.finishedAt ?? Infinity))) {
    const r = reports.scenario;
    result.push({ ...r, name: 'Application scenario', cases: [{ id: r.id, name: r.plan?.kind ?? 'Scenario', status: r.status, assertions: r.assertions ?? [], report: r }] });
  }
  if (reports.checks) {
    const r = reports.checks;
    result.push({ ...r, name: 'Regression checks', cases: [...(r.checks ?? []), ...(r.queue?.checks ?? [])].map((check, i) => ({
      id: String(i), name: check.name, status: check.passed ? 'passed' : 'failed', report: check,
      assertions: [{ name: check.name, expected: check.expected ?? true, actual: check.actual ?? check.passed, passed: check.passed }] })) });
  }
  return result;
}

export function createTestResults(root) {
  let signature = '', selected = null, reports = [], category = 'assertions';
  const tables = element('div'), inspector = element('section'); inspector.hidden = true; inspector.className = 'test-inspector';
  const heading = element('h3'), controls = element('div'), json = element('pre'); controls.className = 'actions'; json.className = 'application-records';
  for (const name of ['assertions', 'inputs', 'configuration', 'outputs', 'logs', 'full report']) {
    const button = element('button', name); button.onclick = () => { category = name; inspect(); }; controls.append(button);
  }
  const exportButton = element('button', 'Export selected test JSON');
  exportButton.onclick = () => { const entry = selectedEntry(); if (entry) downloadReport(entry.report ?? entry, 'streamplay-test-evidence.json'); };
  inspector.append(heading, controls, json, exportButton); root.append(tables, inspector);
  const selectedEntry = () => reports.find(r => r.id === selected?.report)?.cases.find(c => c.id === selected.case);
  function inspect() {
    const entry = selectedEntry(); inspector.hidden = !entry; if (!entry) return;
    heading.textContent = `${entry.name} · ${entry.status.toUpperCase()}`;
    const report = entry.report ?? entry;
    const value = category === 'assertions' ? entry.assertions : category === 'configuration' ? report.configs ?? report.scenario ?? report.plan
      : category === 'outputs' ? report.outputs ?? report.alerts : category === 'logs' ? report.logs ?? report.output : category === 'full report' ? report : report[category];
    const scroll = json.scrollTop;
    json.textContent = pretty(value ?? { message: 'No evidence captured for this category.' }); json.scrollTop = scroll;
    for (const button of controls.children) button.setAttribute('aria-pressed', String(button.textContent === category));
  }
  return { render(next) {
    reports = next; root.hidden = !reports.length;
    const nextSignature = pretty(reports); if (nextSignature === signature) return; signature = nextSignature;
    tables.replaceChildren();
    for (const report of reports) {
      const section = element('section'); section.className = 'test-report';
      section.append(element('h3', report.name ?? 'Scenario suite'));
      const passed = report.cases.filter(c => c.status === 'passed').length;
      const status = element('p', `${report.status.toUpperCase()} · ${passed}/${report.cases.length} cases passed · ${report.phase ?? 'finished'}`); status.setAttribute('role', 'status'); section.append(status);
      if (report.boundary) section.append(element('p', report.boundary));
      if (report.error) section.append(element('p', report.error));
      const wrapper = element('div'); wrapper.className = 'test-table-scroll';
      const table = element('table'); table.className = 'test-table';
      const head = element('thead'), hr = element('tr'); for (const title of ['Test / assertion', 'Expected', 'Actual', 'Result', 'Evidence']) hr.append(element('th', title)); head.append(hr); table.append(head);
      const body = element('tbody');
      for (const entry of report.cases) {
        const pending = entry.expectations?.map(expected => ({ expected })) ?? [{ expected: entry.scenario?.expected ?? 'No completed assertion' }];
        const assertions = entry.assertions?.length ? entry.assertions : pending;
        assertions.forEach((check, index) => {
          const row = element('tr');
          const result = typeof check.passed === 'boolean' ? check.passed ? 'PASS' : 'FAIL' : entry.status.toUpperCase().replace('-', ' ');
          const name = check.name ?? check.alertType ?? '';
          const expected = check.observedMinutes === undefined ? check.expected : `${check.expected} events in 1 minute`;
          const actual = check.observedMinutes === undefined ? check.actual : `${check.actual} events across ${check.observedMinutes} minute(s)`;
          for (const value of [index ? name : `${entry.name}${name ? ' · '+name : ''}`, expected, actual, result]) {
            const cell = element('td', cellText(value)); row.append(cell);
          }
          row.children[3].dataset.result = result;
          const evidence = element('td');
          if (!index && entry.report) {
            const button = element('button', 'Inspect test'); button.setAttribute('aria-label', `Inspect test: ${entry.name}`);
            button.onclick = () => { selected = { report: report.id, case: entry.id }; inspect(); }; evidence.append(button);
          }
          row.append(evidence); body.append(row);
        });
        if (entry.report?.error || entry.error || entry.report?.cleanupErrors?.length) {
          const row = element('tr'), cell = element('td', `${entry.status}: ${entry.error ?? entry.report.error ?? ''} ${(entry.report?.cleanupErrors ?? []).join('; ')}`); cell.colSpan = 5; row.append(cell); body.append(row);
        }
      }
      table.append(body); wrapper.append(table); section.append(wrapper);
      for (const note of report.notVerified ?? []) section.append(element('p', `NOT VERIFIED — ${note}`));
      const button = element('button', 'Export report JSON'); button.onclick = () => downloadReport(report, 'streamplay-suite-report.json'); section.append(button); tables.append(section);
    }
    inspect();
  } };
}
