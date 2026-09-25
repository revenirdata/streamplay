// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';

const run = (...args) => spawnSync(process.execPath, ['src/streamplay-cli.js', ...args], { encoding: 'utf8' });

test('unified CLI documents the code-first command surface', () => {
  const result = run('help');
  assert.equal(result.status, 0);
  for (const command of ['start', 'lab', 'run', 'suite', 'plan', 'simulate', 'example']) assert.match(result.stdout, new RegExp(`\\b${command}\\b`));
  assert.match(result.stdout, /JSON files are the source of truth/);
  assert.match(result.stdout, /splay <command>/);
});

test('unified CLI returns a useful error for unknown commands', () => {
  const result = run('not-a-command');
  assert.equal(result.status, 1);
  assert.match(result.stderr, /Unknown command/);
});

test('unified CLI delegates experiment planning', () => {
  const result = run('plan', 'examples/experiments/streaming-smoke.json');
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).plan.name, 'Streaming smoke test');
});
