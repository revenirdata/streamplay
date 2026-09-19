// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';
test('actual boundary metadata stays expanded and paused inspection freezes only the view', async ({ page }) => {
  const topology = { version: 1, adapter: 'local', nodes: [{ id: 'in', kind: 'topic', label: 'live-input', observe: 'inputs' }, { id: 'app', kind: 'processor', label: 'Flink', observe: 'none' }, { id: 'out', kind: 'topic', label: 'live-output', observe: 'outputs' }], edges: [{ from: 'in', to: 'app' }, { from: 'app', to: 'out', observe: 'outputs' }] };
  await page.route('**/api/config', async route => { const original = await (await route.fetch()).json(); await route.fulfill({ json: { ...original, application: { name: 'Inspection fixture', actions: [] } } }); });
  let count = 1;
  await page.route('**/api/application', route => route.fulfill({ json: { status: 'running', topology, inputs: Array.from({ length: count }, (_, index) => ({ json: true, value: { sequence: index }, raw: JSON.stringify({ sequence: index }), topic: 'live-input', partition: 0, offset: String(index), observedAt: `2026-09-19T00:00:0${index}Z` })), outputs: [], state: { checkpoint: 7 }, logs: [] } }));
  await page.goto('/'); await page.getByRole('button', { name: 'KAFKA TOPIC: live-input', exact: true }).click();
  await page.getByText('Transport metadata', { exact: true }).click();
  count = 2; await expect(page.locator('.graph-record')).toHaveCount(2);
  await expect(page.locator('.graph-record details[open]')).toHaveCount(1);
  await expect(page.locator('.graph-record details[open]')).toContainText('"offset": "0"');
  await page.getByRole('button', { name: 'Pause view', exact: true }).click();
  count = 3;
  // Polling continues elsewhere; the graph intentionally keeps the older evidence.
  await expect(page.locator('#application-records')).toHaveText('[]');
  await expect(page.locator('#graph-status')).toContainText('Paused view');
  await page.waitForTimeout(1200);
  await expect(page.locator('.graph-record')).toHaveCount(2);
  await page.getByRole('button', { name: 'Resume view', exact: true }).click();
  await expect(page.locator('.graph-record')).toHaveCount(3);
  await expect(page.locator('.graph-record details[open]')).toHaveCount(1);
});

test('recovery view keeps long application names and real topic identifiers inside a narrow screen', async ({ page }) => {
  const topology = { version: 1, adapter: 'local', nodes: [{ id: 'input', kind: 'topic', label: 'recovery-in-9ea90c15d8984940b378156763320636', observe: 'inputs' }, { id: 'flink', kind: 'processor', label: 'Flink · count and sum by source', observe: 'none' }, { id: 'output', kind: 'topic', label: 'recovery-out-9ea90c15d8984940b378156763320636', observe: 'outputs' }], edges: [{ from: 'input', to: 'flink' }, { from: 'flink', to: 'output', observe: 'outputs' }] };
  await page.route('**/api/config', async route => { const original = await (await route.fetch()).json(); await route.fulfill({ json: { ...original, topology, application: { name: 'Stateful Flink worker recovery', view: 'recovery', actions: [{ id: 'run', label: 'Run worker recovery experiment', input: 'none' }] } } }); });
  await page.route('**/api/application', route => route.fulfill({ json: { status: 'passed', topology, inputs: [], outputs: [{ json: true, value: { source_id: 'sensor-a', total: 15 }, raw: '{"source_id":"sensor-a","total":15}', offset: '5', topic: topology.nodes[2].label }], state: { restored: 1 } } }));
  await page.goto('/'); await page.locator('[data-graph-node="output"]').click();
  await expect(page.locator('#application-status')).toHaveText('passed');
  await page.setViewportSize({ width: 390, height: 900 });
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await expect(page.locator('.workspace')).toBeHidden();
});
