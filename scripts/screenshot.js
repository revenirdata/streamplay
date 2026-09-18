// SPDX-License-Identifier: Apache-2.0
// Run against a local workbench: npm start, then node scripts/screenshot.js.
import { chromium, expect } from '@playwright/test';
import { mkdir } from 'node:fs/promises';
const browser = await chromium.launch(process.env.STREAMPLAY_CHROME ? { executablePath: process.env.STREAMPLAY_CHROME } : {});
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1060 }, deviceScaleFactor: 1 });
  await page.goto('http://127.0.0.1:4317');
  await expect(page.locator('#events')).toHaveValue(/order-101/);
  await page.getByRole('button', { name: 'Run scenario' }).click();
  await page.locator('#status').filter({ hasText: 'observed' }).waitFor();
  await mkdir('docs/images', { recursive: true });
  await page.screenshot({ path: 'docs/images/workbench.png' });
} finally { await browser.close(); }
