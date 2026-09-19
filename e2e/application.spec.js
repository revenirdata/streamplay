// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';

test('application panel preserves draft configuration and displays trusted controls without interpreting data as HTML', async ({ page }) => {
  let configuration = { timeout: 5 }, status = 'stopped';
  const actions = [{ id: 'start', label: 'Start application' }, { id: 'apply', label: 'Apply configuration', input: 'configuration' }];
  await page.route('**/api/config', async route => {
    const response = await route.fetch(); const body = await response.json();
    await route.fulfill({ json: { ...body, application: { name: 'Local example', configuration, input: [], actions } } });
  });
  await page.route('**/api/application', async route => {
    if (route.request().method() === 'GET') return route.fulfill({ json: { status, configuration, busy: null, runningScenario: false, outputs: [{ raw: '<img src=x onerror=alert(1)>' }], logs: ['application ready'] } });
    const body = route.request().postDataJSON();
    if (body.action === 'start') status = 'ready';
    if (body.action === 'apply') configuration = body.value;
    return route.fulfill({ json: { message: 'Action complete', configuration } });
  });
  await page.goto('/');
  await expect(page.locator('#application')).toBeVisible();
  await page.getByRole('button', { name: 'Start application', exact: true }).click();
  await expect(page.locator('#application-status')).toHaveText('ready');
  await expect(page.locator('#application-records')).toContainText('<img');
  await expect(page.locator('#application-records img')).toHaveCount(0);
  await page.locator('#application-config').fill('{"timeout":10}');
  configuration = { timeout: 7 };
  await page.waitForTimeout(1200); // cross the snapshot poll and ensure it does not replace an edited draft
  await expect(page.locator('#application-config')).toHaveValue('{"timeout":10}');
  await page.getByRole('button', { name: 'Apply configuration', exact: true }).click();
  await expect(page.locator('#application-message')).toHaveText('Action complete');
  expect(configuration).toEqual({ timeout: 10 });
  await page.getByRole('tab', { name: 'Application logs', exact: true }).click();
  await expect(page.locator('#application-records')).toHaveText('application ready');
  await page.setViewportSize({ width: 390, height: 900 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});

test('typed adapter controls retain edits across polling and leave cancellation available', async ({ page }) => {
  let received, busy = null;
  await page.route('**/api/config', async route => {
    const response = await route.fetch(); const body = await response.json();
    await route.fulfill({ json: { ...body, application: { name: 'Typed test adapter', controls: [
      { id: 'deviceId', label: 'Device', type: 'select', optionsFrom: 'devices' },
      { id: 'value', label: 'Measured value', type: 'number', value: 4, min: 0, max: 100 }
    ], actions: [{ id: 'send', label: 'Send measured event', input: 'controls' }, { id: 'cancel', label: 'Cancel operation', allowWhileBusy: true }] } } });
  });
  await page.route('**/api/application', async route => {
    if (route.request().method() === 'GET') return route.fulfill({ json: { status: 'ready', busy, devices: [{ id: 'sensor-a' }, { id: 'sensor-b' }], reports: { passed: true } } });
    received = route.request().postDataJSON(); return route.fulfill({ json: { message: 'Accepted' } });
  });
  await page.goto('/');
  await page.getByLabel('Device', { exact: true }).selectOption('sensor-b');
  await page.getByLabel('Measured value', { exact: true }).fill('7');
  await page.waitForTimeout(1100);
  await page.getByRole('button', { name: 'Send measured event', exact: true }).click();
  expect(received.value).toEqual({ deviceId: 'sensor-b', value: 7 });
  busy = 'test';
  await expect(page.getByRole('button', { name: 'Send measured event', exact: true })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Cancel operation', exact: true })).toBeEnabled();
  const downloaded = page.waitForEvent('download'); await page.getByRole('button', { name: 'Export application evidence' }).click();
  expect((await downloaded).suggestedFilename()).toBe('streamplay-application-evidence.json');
});
