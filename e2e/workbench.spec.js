// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';

test('designs a production-shaped experiment without claiming it ran', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('#experiment-average-rate')).toHaveText('10/s');
  await page.locator('#experiment-preset').selectOption('burst');
  await expect(page.locator('#experiment-peak-rate')).toHaveText('2.5k/s');
  await expect(page.locator('#experiment-shards')).toHaveText('7');
  await page.locator('#experiment-entities').fill('1000');
  await page.locator('#experiment-rate').fill('2');
  await page.locator('#experiment-payload').fill('1000');
  await page.locator('#experiment-burst').fill('2');
  await page.locator('#experiment-partitions').fill('8');
  await page.locator('#experiment-hotKey').fill('60');
  await page.locator('#experiment-calculate').click();
  await expect(page.locator('#experiment-capacity-note')).toContainText('hottest partition key');
  await expect(page.locator('#experiment-message')).toHaveText('Plan updated. No traffic was sent.');
  await page.getByText('What StreamPlay can test', { exact: true }).click();
  await expect(page.locator('#experiment-coverage')).toContainText('Event time');
});

test('edit, execute, inspect raw data, save, reload and compare actual runs', async ({ page }) => {
  const errors = []; page.on('pageerror', e => errors.push(e.message));
  await page.goto('/');
  await expect(page.locator('#events')).toHaveValue(/order-101/);
  await page.locator('#window').fill('1000');
  await page.getByRole('button', { name: 'Run scenario' }).click();
  await expect(page.locator('#status')).toHaveText('observed');
  await expect(page.locator('#output-count')).toHaveText('2');
  await expect(page.locator('.record')).toHaveCount(2);
  await page.locator('.record summary').first().click();
  await expect(page.locator('.record details').first()).toContainText('observedAt');
  const firstId = await page.locator('#history').inputValue();
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.locator('#message')).toContainText('Scenario saved');
  await page.locator('#events').fill(JSON.stringify([{ order_id: '<img src=x onerror=alert(1)>', quantity: 3, unit_price_cents: 1000 }]));
  await page.getByRole('button', { name: 'Run scenario' }).click();
  await expect(page.locator('#output-count')).toHaveText('1');
  await expect(page.locator('#run')).toBeEnabled();
  await expect(page.locator('.record')).toContainText('3000');
  await expect(page.locator('.record img')).toHaveCount(0);
  await page.locator('#baseline').selectOption(firstId);
  await page.getByRole('button', { name: 'Compare outputs' }).click();
  await expect(page.locator('#comparison')).toContainText('"outputsEqual": false');
  await page.reload();
  await page.locator('#history').selectOption(firstId);
  await page.getByRole('button', { name: 'Load run & scenario' }).click();
  await expect(page.locator('#output-count')).toHaveText('2');
  await expect(page.locator('#events')).toHaveValue(/order-101/);
  await page.locator('summary').filter({ hasText: /^Expected output optional$/ }).click();
  await page.locator('#expected').fill('[]');
  await page.getByRole('button', { name: 'Run scenario' }).click();
  await expect(page.locator('#status')).toHaveText('failed');
  expect(errors).toEqual([]);
});

test('shows live output before completion and cancels without claiming a passed assertion', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('#events')).toHaveValue(/order-101/);
  await page.locator('#window').fill('10000');
  await page.getByRole('button', { name: 'Run scenario' }).click();
  await expect(page.locator('#output-count')).toHaveText('2');
  await expect(page.locator('#status')).toHaveText('running');
  await expect(page.locator('#compare')).toBeDisabled();
  await page.reload();
  await expect(page.locator('#status')).toHaveText('running');
  await expect(page.locator('#output-count')).toHaveText('2');
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(page.locator('#status')).toHaveText('cancelled');
  await expect(page.locator('#run')).toBeEnabled();
  await expect(page.locator('#output-count')).toHaveText('2');
});

for (const width of [1440, 390]) test(`workbench fits ${width}px and keeps inputs accessible`, async ({ page }) => {
  await page.setViewportSize({ width, height: 1000 }); await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Streaming workbench' })).toBeVisible();
  await expect(page.getByLabel('Input events')).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: `test-results/workbench-${width}.png`, fullPage: true });
});

test('generates a multi-entity sequence, checks real output, and imports a saved plan', async ({ page }) => {
  const errors = []; page.on('pageerror', e => errors.push(e.message));
  await page.goto('/');
  await expect(page.locator('#events')).toHaveValue(/order-101/);
  await page.getByText('Build a device event sequence', { exact: true }).click();
  await page.locator('#window').fill('1000');
  await page.locator('#sequence-build').click();
  await expect(page.locator('#message')).toContainText('Generated 6 events');
  await page.locator('summary').filter({ hasText: /^Expected output optional$/ }).click();
  await page.locator('#expected').fill(JSON.stringify([
    { order_id: 'device-001', total_cents: 6000 }, { order_id: 'device-002', total_cents: 6000 },
    { order_id: 'device-001', total_cents: 6000 }, { order_id: 'device-002', total_cents: 6000 },
  ]));
  await page.locator('#run').click();
  await expect(page.locator('#status')).toHaveText('passed');
  await expect(page.locator('#input-count')).toHaveText('6');
  await expect(page.locator('#output-count')).toHaveText('4');
  await page.getByRole('tab', { name: 'Run metadata', exact: true }).click();
  await expect(page.locator('#record-list')).toContainText('latenessMs');
  const events = JSON.parse(await page.locator('#events').inputValue());
  await page.locator('#import-scenario').setInputFiles({ name:'run.json', mimeType:'application/json', buffer:Buffer.from(JSON.stringify({scenario:{version:1,name:'Imported sequence',adapter:'process',observeMs:1000,events,scheduleMs:[0,0,500,0,1000,0],tailMs:500,timingMode:'absolute'}})) });
  await expect(page.locator('#message')).toContainText('Settings imported');
  await expect(page.locator('#name')).toHaveValue('Imported sequence');
  expect(errors).toEqual([]);
  await page.screenshot({path:'test-results/event-sequence.png',fullPage:true});
});
