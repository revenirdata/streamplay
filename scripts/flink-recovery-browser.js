// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from '@playwright/test';
const directory = resolve('.streamplay/evidence/flink-recovery'), base = 'http://127.0.0.1:4333';
await mkdir(directory, { recursive: true });
const child = spawn(process.execPath, ['scripts/flink-recovery-demo.js'], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'], env: { ...process.env, STREAMPLAY_DATA_DIR: directory, STREAMPLAY_PORT: '4333' } });
let diagnostic = '', browser;
child.stderr.on('data', bytes => { diagnostic = (diagnostic + bytes).slice(-20000); });
child.stdout.on('data', bytes => { diagnostic = (diagnostic + bytes).slice(-20000); });
async function until(check, label, timeout = 120000) { const end = Date.now() + timeout; while (Date.now() < end) { if (child.exitCode !== null) throw new Error(`Workbench exited: ${diagnostic}`); const value = await check(); if (value) return value; await delay(200); } throw new Error(`${label}: ${diagnostic}`); }
try {
  await until(async () => { try { return (await fetch(base)).ok; } catch { return false; } }, 'Startup', 180000);
  browser = await chromium.launch({ headless: true, ...(process.env.STREAMPLAY_CHROME ? { executablePath: process.env.STREAMPLAY_CHROME } : {}) });
  const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } }); const errors = [];
  page.on('pageerror', error => errors.push(error.message)); await page.goto(base);
  await page.getByRole('button', { name: 'Run worker recovery experiment', exact: true }).click();
  await until(async () => { const value = await (await fetch(base + '/api/application')).json(); if (value.reports?.status === 'failed') throw new Error(value.reports.error); return value.inputs.length >= 3; }, 'Baseline inputs');
  await page.locator('[data-graph-node="input"]').click();
  await until(async () => (await page.locator('#graph-inspector-body').textContent()).includes('event_id'), 'Visible input JSON');
  await page.locator('#graph-inspector-body').getByText('Transport metadata', { exact: true }).first().click();
  await page.getByRole('button', { name: 'Pause view', exact: true }).click();
  await page.locator('.pipeline').screenshot({ path: resolve(directory, 'input-json.png') });
  const result = await until(async () => { const value = await (await fetch(base + '/api/application')).json(); return !value.busy && value.reports?.phase === 'finished' && value; }, 'Recovery completion');
  assert.equal(result.reports.status, 'passed', JSON.stringify(result.reports));
  assert.equal(result.reports.checks.length, 4);
  await page.getByRole('button', { name: 'Resume view', exact: true }).click();
  await until(async () => (await page.locator('#application-status').textContent()) === 'passed', 'UI completion');
  await page.locator('[data-graph-node="flink"]').click();
  await page.locator('#graph-inspector-tabs').getByRole('tab', { name: 'State', exact: true }).click();
  assert.ok((await page.locator('#graph-inspector-body').textContent()).includes('restored'));
  await page.locator('.pipeline').screenshot({ path: resolve(directory, 'checkpoint-restore.png') });
  await page.locator('[data-graph-node="output"]').click();
  assert.ok((await page.locator('#graph-inspector-body').textContent()).includes('total'));
  await page.screenshot({ path: resolve(directory, 'recovered-output.png'), fullPage: true });
  const download = page.waitForEvent('download'); await page.locator('#application-export').click();
  await (await download).saveAs(resolve(directory, 'browser-evidence.json'));
  await page.setViewportSize({ width: 390, height: 900 });
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
  assert.deepEqual(errors, []);
  console.log('PASS real Flink browser: input JSON, metadata, paused view, checkpoint restore, output JSON, export and mobile layout');
} finally {
  await browser?.close();
  if (child.exitCode === null) { const exit = new Promise(done => child.once('exit', done)); if (child.connected) child.send('shutdown'); await Promise.race([exit, delay(45000, undefined, { ref: false }).then(() => { throw new Error('Recovery workbench cleanup timeout'); })]); }
}
