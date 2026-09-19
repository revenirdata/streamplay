// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createLab } from '../src/lab/runtime.js';
import { workbench } from '../src/server.js';
import sensor from '../examples/lab/in-process.js';

test('presets, independent sources, editable payloads, graph records and evidence export', async ({ page }) => {
  const directory = await mkdtemp(join(tmpdir(), 'streamplay-browser-lab-'));
  const lab = await createLab(sensor, { dataDir: directory });
  const server = workbench({ dataDir: directory, kafka: {}, lab, topology: lab.topology }, { local: c => lab.scenarioAdapter(c) });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    await expect(page.locator('#lab')).toBeVisible();
    await expect(page.locator('#lab-device')).toHaveValue('device-001');
    await expect(page.locator('#lab-meter-controls')).toBeHidden();
    await page.locator('#lab-preset').selectOption('temperature');
    await expect(page.locator('#lab-profile-preview')).toContainText('temperature');
    await page.locator('#lab-profile-apply').click();
    await expect(page.locator('#lab-message')).toContainText('profile completed');
    await page.locator('#lab-add').click();
    await expect(page.locator('#lab-device option')).toHaveCount(2);
    await page.locator('#lab-session').fill('30');
    await page.locator('#lab-start').click();
    await expect(page.locator('#lab-device-status')).toContainText('Sending');
    await page.locator('#lab-device').selectOption('device-002');
    await page.locator('#lab-send').click();
    await expect(page.locator('#lab-inputs')).toContainText('temperature');
    await expect(page.locator('#graph-summary')).toContainText('inputs captured');
    await expect(page.locator('.graph-node[data-kind="source"]')).toHaveCount(2);
    // Unrelated polling must not collapse the JSON inspector disclosure.
    await page.locator('summary').filter({ hasText: 'Captured input JSON' }).click();
    await expect(page.locator('#lab-inputs')).toBeVisible();
    await page.waitForTimeout(1200);
    await expect(page.locator('#lab-inputs')).toBeVisible();
    await page.locator('#lab-device').selectOption('device-001');
    await page.locator('#lab-stop').click();
    await expect(page.locator('#lab-device-status')).toContainText('Idle');
    await page.locator('#lab-preset').selectOption('meter');
    await page.locator('#lab-profile-apply').click();
    await expect(page.locator('#lab-meter-controls')).not.toHaveAttribute('hidden');
    const download = page.waitForEvent('download'); await page.locator('#lab-export').click();
    expect((await download).suggestedFilename()).toBe('streamplay-lab-evidence.json');
    await page.locator('#lab').screenshot({ path: 'test-results/live-source-profiles.png' });
    expect(errors).toEqual([]);
  } finally { await lab.close(); await new Promise(resolve => server.close(resolve)); await rm(directory, { recursive: true, force: true }); }
});
