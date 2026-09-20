// SPDX-License-Identifier: Apache-2.0
import { test, expect } from '@playwright/test';
test('NDJSON import preserves the editor on error, runs duplicates and exports the unchanged scenario format', async ({ page }) => {
  await page.goto('/');
  const initial = await page.locator('#events').inputValue();
  await page.locator('#import-ndjson').setInputFiles({ name: 'bad.ndjson', mimeType: 'application/x-ndjson', buffer: Buffer.from('{}\n\nnope') });
  await expect(page.locator('#message')).toContainText('line 3');
  await expect(page.locator('#events')).toHaveValue(initial);
  const values = [{ order_id: 'ndjson', quantity: 2, unit_price_cents: 100 }, { order_id: 'ndjson', quantity: 2, unit_price_cents: 100 }];
  await page.locator('#import-ndjson').setInputFiles({ name: 'orders.ndjson', mimeType: 'application/x-ndjson', buffer: Buffer.from(values.map(JSON.stringify).join('\n')) });
  await expect(page.locator('#message')).toContainText('Imported 2 events');
  await page.locator('#window').fill('100');
  await page.getByRole('button', { name: 'Run scenario', exact: false }).click();
  await expect(page.locator('#status')).toHaveText('observed');
  await expect(page.locator('#output-count')).toHaveText('2');
  const downloading = page.waitForEvent('download'); await page.locator('#export').click();
  const download = await downloading;
  const chunks = []; for await (const chunk of await download.createReadStream()) chunks.push(chunk);
  const exported = JSON.parse(Buffer.concat(chunks).toString());
  expect(exported.version).toBe(1); expect(exported.events).toEqual(values);
});
