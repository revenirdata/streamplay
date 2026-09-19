// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from '@playwright/test';
const directory = resolve('.streamplay/evidence/delivery'), port = '4332', base = `http://127.0.0.1:${port}`;
await mkdir(directory, { recursive: true });
const child = spawn(process.execPath, ['scripts/recovery-demo.js'], { windowsHide: true, stdio: ['ignore','pipe','pipe','ipc'], env: { ...process.env, STREAMPLAY_PORT: port, STREAMPLAY_DATA_DIR: directory } });
child.stderr.on('data', bytes => process.stderr.write(bytes));
let browser;
async function until(fn, message) { const end = Date.now() + 30000; while (Date.now() < end) { if (await fn()) return; await delay(100); } throw new Error(message); }
try {
  await until(async () => { try { return (await fetch(base)).ok; } catch { return false; } }, 'Demo startup failed');
  browser = await chromium.launch({ headless: true, ...(process.env.STREAMPLAY_CHROME ? { executablePath: process.env.STREAMPLAY_CHROME } : {}) });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1080 } }); const errors = [];
  page.on('pageerror', error => errors.push(error.message)); await page.goto(base);
  await until(async () => (await page.locator('h1').textContent()).includes('Account for every event'), 'Delivery view did not load');
  assert.equal(await page.locator('.workspace').isVisible(), false, 'Unrelated scenario editor must be hidden in the recovery view');
  for (const session of ['ephemeral','persistent']) {
    await page.getByLabel('Subscriber recovery policy').selectOption(session);
    await page.getByRole('button', { name: 'Run recovery experiment', exact: true }).click();
    await until(async () => { const text = await page.locator('#application-message').textContent(); return text.includes('Experiment started'); }, 'Run was not accepted');
    await until(async () => { const snapshot = await (await fetch(base + '/api/application')).json(); return !snapshot.busy && snapshot.deliveryAudit?.reconciliation?.closedAt; }, 'Experiment did not finish');
    const expected = session === 'persistent' ? 'accounted-for' : 'gaps-found';
    await until(async () => (await page.locator('#delivery-summary').textContent()).includes(expected), 'Wrong reconciliation result');
    await page.locator('#delivery-checkpoints article').last().getByText('Inspect exact event IDs').click();
    await page.screenshot({ path: resolve(directory, `${session}-workbench.png`), fullPage: true });
    const download = page.waitForEvent('download'); await page.getByRole('button', { name: 'Export delivery evidence', exact: true }).click();
    await (await download).saveAs(resolve(directory, `${session}-browser-export.json`));
    console.log(`PASS actual browser recovery: ${session} → ${expected}`);
  }
  assert.deepEqual(errors, []);
} finally {
  await browser?.close(); const exited = new Promise(done => child.once('exit', done));
  if (child.connected) child.send('shutdown');
  await Promise.race([exited, delay(15000, undefined, { ref: false }).then(() => { throw new Error('Recovery workbench failed to stop'); })]);
}
