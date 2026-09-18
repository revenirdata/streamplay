// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { defaultTopology, validateTopology, layoutTopology, expandSources, recordsForNode, graphEvidence } from '../src/topology.js';
import { runScenario } from '../src/runner.js';

test('rejects ambiguous or unsafe graphs and accepts branches merging into a processor', () => {
  const graph = defaultTopology('kafka');
  graph.nodes.push({ id: 'config', kind: 'configuration', label: 'Rules', observe: 'none' });
  graph.edges.push({ from: 'config', to: 'app', observe: 'none' });
  const validated = validateTopology(graph), layout = layoutTopology(validated);
  assert.ok(layout.nodes.find(n => n.id === 'config').x < layout.nodes.find(n => n.id === 'app').x);
  assert.throws(() => validateTopology({ ...graph, edges: [...graph.edges, { from: 'app', to: 'source' }] }), /acyclic/);
  assert.throws(() => validateTopology({ ...graph, edges: [{ from: 'missing', to: 'app' }] }), /endpoints/);
  assert.throws(() => validateTopology({ ...graph, nodes: [...graph.nodes, graph.nodes[0]] }), /unique/);
  assert.throws(() => validateTopology({ ...graph, nodes: [{ kind: 'source', label: 'Missing ID' }] }), /unique/);
  assert.throws(() => validateTopology({ ...graph, edges: [...graph.edges, graph.edges[0]] }), /duplicated/);
  assert.throws(() => validateTopology({ ...graph, edges: [{ from: 'config', to: 'app', observe: 'inputs' }] }), /boundary/);
  graph.nodes[0].identityPath = '__proto__.secret';
  assert.throws(() => validateTopology(graph), /ordinary/);
});

test('device expansion is bounded and each source exposes only its own records', () => {
  const graph = defaultTopology('kafka'); graph.nodes[0].identityPath = 'device.id';
  const inputs = Array.from({ length: 10 }, (_, id) => ({ value: { device: { id } } }));
  inputs.push({ value: {} });
  const expanded = expandSources(validateTopology(graph), inputs);
  const sources = expanded.nodes.filter(n => n.kind === 'source');
  assert.equal(sources.length, 7);
  assert.equal(recordsForNode(sources[0], { inputs }).length, 1);
  assert.equal(recordsForNode(sources.at(-1), { inputs }).length, 5);
  assert.equal(sources.flatMap(n => recordsForNode(n, { inputs })).length, inputs.length);
  assert.doesNotThrow(() => layoutTopology(expanded));
});

test('saved snapshots never borrow current state, configuration or live animation', () => {
  const evidence = graphEvidence('run', { id: 'old', status: 'observed', environment: { configuration: { threshold: 1 } } }, { state: 'new', configuration: { threshold: 5 } });
  assert.deepEqual(evidence.configuration, { threshold: 1 });
  assert.equal(evidence.state, undefined); assert.equal(evidence.live, false);
  assert.equal(graphEvidence('application', null, { status: 'unavailable' }).live, false);
});

test('real process run saves a trusted topology snapshot and ignores scenario-supplied topology', async () => {
  const trusted = defaultTopology('process'); trusted.nodes[1].label = 'Orders transform';
  const run = await runScenario({ version: 1, name: 'graph', adapter: 'process', events: [{ order_id: 'graph-1', quantity: 2, unit_price_cents: 100 }], observeMs: 500, topology: { evil: true } }, { topology: trusted });
  assert.equal(run.status, 'observed', run.error);
  assert.equal(run.outputs.length, 1);
  trusted.nodes[1].label = 'Changed later';
  assert.equal(run.topology.nodes[1].label, 'Orders transform');
  assert.equal(run.scenario.topology, undefined);
});
