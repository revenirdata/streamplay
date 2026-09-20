// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';

test('saved suite runs actual process scenarios and exposes expected/actual with persistent inspection', async ({ page, request }) => {
  const config = await (await request.get('/api/config')).json();
  const right = `suite-right-${Date.now()}`, wrong = `suite-wrong-${Date.now()}`;
  for (const name of [right, wrong]) {
    await request.post('/api/scenarios', { data: { ...config.sample, name, observeMs: 100, expected: name === wrong ? [] : [{ order_id: 'order-101', total_cents: 6000 }, { order_id: 'order-101', total_cents: 6000 }] } });
  }
  await page.goto('/');
  const suite = page.getByRole('region', { name: 'Saved scenario suite' });
  for (const checkbox of await suite.getByRole('checkbox').all()) await checkbox.uncheck();
  await suite.getByRole('checkbox', { name: right, exact: true }).check();
  await suite.getByRole('checkbox', { name: wrong, exact: true }).check();
  await suite.getByRole('button', { name: 'Run selected suite', exact: true }).click();
  await expect(suite.locator('.test-report [role=status]')).toContainText('FAILED · 1/2 cases passed');
  await suite.getByRole('button', { name: `Inspect test: ${wrong}` }).click();
  await expect(suite.locator('.test-inspector pre')).toContainText('"passed": false');
  await suite.getByRole('button', { name: 'outputs', exact: true }).click();
  await expect(suite.locator('.test-inspector pre')).toContainText('order_id');
  await page.waitForTimeout(1200);
  await expect(suite.getByRole('button', { name: 'outputs', exact: true })).toHaveAttribute('aria-pressed', 'true');
  const download = page.waitForEvent('download'); await suite.getByRole('button', { name: 'Export selected test JSON' }).click();
  expect((await download).suggestedFilename()).toBe('streamplay-test-evidence.json');
  await page.setViewportSize({ width: 390, height: 850 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});

test('application suite shows adapter assertions and keeps raw JSON data inert', async ({ page }) => {
  const report = { id: 'suite', status: 'passed', phase: 'finished', cases: [{ id: 'case', name: 'Reconnect', status: 'passed',
    assertions: [{ alertType: 'offline', expected: 2, actual: 2, observedMinutes: 1, passed: true }],
    report: { inputs: [{ payload: '<img src=x onerror=alert(1)>' }], configs: [{ threshold: 3 }], alerts: [{ status: 'offline' }] } }] };
  await page.route('**/api/config', async route => {
    const body = await (await route.fetch()).json(); await route.fulfill({ json: { ...body, application: { name: 'Test adapter', actions: [] } } });
  });
  await page.route('**/api/application', route => route.fulfill({ json: { status: 'ready', reports: { suite: report } } }));
  await page.goto('/');
  const results = page.locator('#application-test-results');
  await expect(results).toContainText('2 events in 1 minute');
  await results.getByRole('button', { name: 'Inspect test: Reconnect' }).click();
  await results.getByRole('button', { name: 'inputs', exact: true }).click();
  await expect(results.locator('pre')).toContainText('<img'); await expect(results.locator('img')).toHaveCount(0);
  await results.getByRole('button', { name: 'configuration', exact: true }).click();
  await expect(results.locator('pre')).toContainText('threshold');
});
