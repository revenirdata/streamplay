// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { parseNdjson } from '../src/scenario.js';
test('NDJSON preserves duplicates, nulls, arrays and scalar values with CRLF and BOM', () => {
  assert.deepEqual(parseNdjson('\uFEFF{"x":1}\r\n\r\n{"x":1}\nnull\n[1,2]\nfalse\n3\n"hi"\n'), [{x:1},{x:1},null,[1,2],false,3,'hi']);
});
test('NDJSON rejects malformed original lines and enforces both fixture limits', () => {
  assert.throws(() => parseNdjson('{}\n\n{bad}'), /line 3/);
  assert.throws(() => parseNdjson(' \n'), /1–1000/);
  assert.throws(() => parseNdjson('null\n'.repeat(1001)), /1–1000/);
  assert.throws(() => parseNdjson(JSON.stringify('é'.repeat(256000))), /512 KB/);
  assert.equal(parseNdjson('null\n'.repeat(1000)).length, 1000);
});
