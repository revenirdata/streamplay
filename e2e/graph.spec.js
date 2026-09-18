// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';

test('named Kafka topics, raw process evidence, saved topology and accessible inspection', async ({ page }) => {
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1600, height: 1100 });
  await page.goto('/');
  await expect(page.locator('.graph-node')).toHaveCount(3);
  await page.locator('#adapter').selectOption('kafka');
  await page.getByRole('button', { name: 'KAFKA TOPIC: streamplay-orders-in', exact: true }).click();
  await expect(page.locator('#graph-inspector-title')).toHaveText('streamplay-orders-in');
  await expect(page.locator('.graph-edge--selected')).toHaveCount(2);
  await page.locator('#graph-search').fill('orders-out');
  await expect(page.locator('.graph-node:not(.graph-node--dim)')).toHaveCount(1);
  await page.locator('#graph-search').fill('');
  const initialZoom = await page.locator('#graph-zoom').innerText();
  await page.getByRole('button', { name: 'Zoom in', exact: true }).click();
  await expect(page.locator('#graph-zoom')).not.toHaveText(initialZoom);
  await page.getByRole('button', { name: 'Fit', exact: true }).click();
  await expect(page.locator('#graph-zoom')).toHaveText(initialZoom);
  await page.locator('.pipeline').screenshot({ path: 'test-results/kafka-topics.png' });
  await page.getByRole('button', { name: 'Load example', exact: true }).click();
  await expect(page.locator('.graph-node[data-kind="topic"]')).toHaveCount(0);
  await page.locator('#window').fill('1000');
  await page.getByRole('button', { name: 'Run scenario' }).click();
  await expect(page.locator('#status')).toHaveText('observed');
  await expect(page.locator('#graph-status')).toHaveText('Saved run · snapshot');
  await page.getByRole('button', { name: 'OUTPUT: Standard output', exact: true }).click();
  await expect(page.locator('.graph-record')).toHaveCount(2);
  await expect(page.locator('#graph-inspector-body')).toContainText('6000');
  await expect(page.locator('.graph-edge--pulse')).toHaveCount(0);
  const recordsTab = page.locator('#graph-inspector-tabs').getByRole('tab', { name: 'Records', exact: true });
  await recordsTab.focus(); await page.keyboard.press('ArrowRight');
  await expect(page.locator('#graph-inspector-tabs').getByRole('tab', { name: 'Details', exact: true })).toBeFocused();
  await expect(page.locator('#graph-inspector-body')).toContainText('snapshot');
  await page.setViewportSize({ width: 390, height: 900 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  expect(errors).toEqual([]);
});

test('declared graph branches expose individual devices and focused tabs continue updating', async ({ page }) => {
  const topology = JSON.parse(await readFile(new URL('../examples/kafka-flink/topology.json', import.meta.url), 'utf8'));
  topology.adapter = 'local'; topology.nodes[0].identityPath = 'device.id';
  topology.nodes.push({ id: 'config', kind: 'configuration', label: 'Rule configuration', observe: 'none' });
  topology.edges.push({ from: 'config', to: 'flink', observe: 'none' });
  await page.route('**/api/config', async route => {
    const response = await route.fetch(), config = await response.json();
    await route.fulfill({ json: { ...config, topology, localAdapter: true, application: { name: 'Graph fixture', actions: [], configuration: {}, input: [] } } });
  });
  let timeout = 5;
  await page.route('**/api/application', route => route.fulfill({ json: { status: 'ready', configuration: { timeout }, state: { count: 7 }, inputs: ['meter-A', 'meter-B'].map(id => ({ value: { device: { id } }, raw: JSON.stringify({ device: { id } }) })), outputs: [], logs: ['ready'] } }));
  await page.goto('/');
  await page.getByRole('button', { name: 'SOURCE / DEVICE: meter-B', exact: true }).click();
  await expect(page.locator('.graph-record')).toHaveCount(1);
  await expect(page.locator('#graph-inspector-body')).toContainText('meter-B');
  await expect(page.locator('#graph-inspector-body')).not.toContainText('meter-A');
  await page.getByRole('button', { name: 'CONFIGURATION: Rule configuration', exact: true }).click();
  await expect(page.locator('#graph-inspector-body')).toContainText('"timeout": 5');
  await page.locator('#graph-inspector-tabs').getByRole('tab', { name: 'Configuration', exact: true }).click();
  timeout = 7;
  await expect(page.locator('#graph-inspector-body')).toContainText('"timeout": 7');
  await expect(page.locator('#graph-inspector-tabs').getByRole('tab', { name: 'Configuration', exact: true })).toBeFocused();
  await page.locator('.pipeline').screenshot({ path: 'test-results/branched-graph.png' });
});
