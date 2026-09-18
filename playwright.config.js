// SPDX-License-Identifier: Apache-2.0
import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './e2e', fullyParallel: false, workers: 1, retries: 0,
  use: { baseURL: 'http://127.0.0.1:4318', trace: 'retain-on-failure',
    launchOptions: process.env.STREAMPLAY_CHROME ? { executablePath: process.env.STREAMPLAY_CHROME } : {} },
  webServer: { command: 'node src/server.js', url: 'http://127.0.0.1:4318', reuseExistingServer: false,
    env: { STREAMPLAY_PORT: '4318', STREAMPLAY_DATA_DIR: '.streamplay/e2e' } }
});
