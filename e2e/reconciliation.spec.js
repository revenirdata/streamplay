// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';
test('delivery evidence exposes exact gaps and preserves expanded checkpoints across live polling', async ({ page }) => {
  const bundle = { version: 1, runId: 'test-audit', stages: ['broker','sink'], lateAfterMs: 50, entries: [
    { kind: 'planned', id: 'event-a', at: 1 }, { kind: 'planned', id: 'event-b', at: 1 },
    { kind: 'receipt', id: 'event-a', stage: 'broker', at: 2 }, { kind: 'receipt', id: 'event-b', stage: 'broker', at: 2 },
    { kind: 'receipt', id: 'event-a', stage: 'sink', at: 100 }, { kind: 'closed', at: 150 }
  ] };
  await page.route('**/api/config', async route => { const response = await route.fetch(); const config = await response.json(); await route.fulfill({ json: { ...config, application: { name: 'Audit test', actions: [] } } }); });
  await page.route('**/api/application', route => route.fulfill({ json: { status: 'ready', deliveryAudit: bundle } }));
  await page.goto('/');
  await expect(page.locator('#delivery-summary')).toContainText('2 unique events · gaps-found');
  const sink = page.locator('#delivery-checkpoints article').nth(1);
  await expect(sink).toContainText('1 observed · 1 unaccounted · 1 delayed');
  await sink.getByText('Inspect exact event IDs').click(); await page.waitForTimeout(1200);
  await expect(sink.locator('details')).toHaveAttribute('open', '');
  await expect(sink.locator('pre')).toContainText('event-b');
  const download = page.waitForEvent('download'); await page.getByRole('button', { name: 'Export delivery evidence' }).click();
  expect((await download).suggestedFilename()).toBe('delivery-test-audit.json');
  await page.setViewportSize({ width: 390, height: 900 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});
