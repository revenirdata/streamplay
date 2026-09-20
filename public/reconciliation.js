// SPDX-License-Identifier: Apache-2.0
import { reconcileDelivery } from './delivery-model.js';
let current;
export function showDeliveryAudit(bundle) {
  const report = reconcileDelivery(bundle); current = bundle;
  const root = document.getElementById('delivery-audit'); root.hidden = false;
  document.getElementById('delivery-summary').textContent = `${report.planned} unique events · ${report.status} · delayed means more than ${report.lateAfterMs} ms`;
  const table = document.getElementById('delivery-checkpoints');
  const opened = new Set([...table.querySelectorAll('details[open]')].map(element => element.dataset.stage)); table.replaceChildren();
  for (const point of report.checkpoints) {
    const card = document.createElement('article'); card.className = 'record';
    const title = document.createElement('h3'); title.textContent = point.stage;
    const counts = document.createElement('p'); counts.textContent = `${point.uniqueObserved} observed · ${point.unaccountedIds.length} unaccounted · ${point.delayedIds.length} delayed · ${point.extraObservations} repeat observations`;
    const details = document.createElement('details'), summary = document.createElement('summary'), pre = document.createElement('pre');
    details.dataset.stage = point.stage; details.open = opened.has(point.stage);
    summary.textContent = 'Inspect exact event IDs'; pre.textContent = JSON.stringify(point, null, 2); details.append(summary, pre); card.append(title, counts, details); table.append(card);
  }
  document.getElementById('delivery-evidence').textContent = JSON.stringify({ markers: report.markers, failures: report.failures, unexpectedIds: report.unexpectedIds, events: report.rows }, null, 2);
  document.getElementById('delivery-boundary').textContent = report.boundary;
}
export function initializeDeliveryAudit() {
  document.getElementById('delivery-import').onchange = async event => {
    try { const file = event.target.files[0]; if (!file || file.size > 10000000) throw new Error('Choose a delivery evidence JSON file under 10 MB.'); showDeliveryAudit(JSON.parse(await file.text())); }
    catch (error) { document.getElementById('delivery-summary').textContent = error.message; }
  };
  document.getElementById('delivery-export').onclick = () => {
    if (!current) return;
    const url = URL.createObjectURL(new Blob([JSON.stringify(current, null, 2)], { type: 'application/json' }));
    const link = document.createElement('a'); link.href = url; link.download = `delivery-${current.runId}.json`; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
}
