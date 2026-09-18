// SPDX-License-Identifier: Apache-2.0
// Shared pure graph contract. Labels are data, never markup or executable commands.
const kinds = ['source', 'stream', 'topic', 'queue', 'processor', 'sink', 'configuration'];
const observations = ['inputs', 'outputs', 'none'];
const idPattern = /^[a-zA-Z0-9_-]{1,64}$/;
const fieldPattern = /^[a-zA-Z_][\w]*(\.[a-zA-Z_][\w]*)*$/;
export function validateTopology(value) {
  if (!value || value.version !== 1 || !['process', 'kafka', 'local'].includes(value.adapter)) throw new Error('Topology requires version 1 and an adapter (process, kafka or local).');
  if (!Array.isArray(value.nodes) || !value.nodes.length || value.nodes.length > 32 || !Array.isArray(value.edges) || value.edges.length > 64) throw new Error('Topology supports 1–32 nodes and at most 64 edges.');
  const ids = new Set();
  const nodes = value.nodes.map(node => {
    if (!node || !idPattern.test(node.id) || ids.has(node.id)) throw new Error('Topology nodes need unique alphanumeric IDs.');
    ids.add(node.id);
    if (!kinds.includes(node.kind) || typeof node.label !== 'string' || !node.label.trim() || node.label.length > 160) throw new Error('Topology nodes need a kind and a label of 1–160 characters.');
    if (!observations.includes(node.observe ?? 'none')) throw new Error('Node observation must be inputs, outputs or none.');
    if (node.identityPath !== undefined && (node.kind !== 'source' || node.observe !== 'inputs' || typeof node.identityPath !== 'string' || !fieldPattern.test(node.identityPath) || node.identityPath.split('.').some(k => ['__proto__', 'constructor', 'prototype'].includes(k)))) throw new Error('identityPath requires an input source and an ordinary dot-separated field path.');
    if (node.detail !== undefined && (typeof node.detail !== 'string' || node.detail.length > 500)) throw new Error('Node detail must be text up to 500 characters.');
    return { id: node.id, kind: node.kind, label: node.label, observe: node.observe ?? 'none', ...(node.identityPath ? { identityPath: node.identityPath } : {}), ...(node.detail ? { detail: node.detail } : {}) };
  });
  const pairs = new Set();
  const edges = value.edges.map(edge => {
    if (!edge || !ids.has(edge.from) || !ids.has(edge.to) || edge.from === edge.to || pairs.has(`${edge.from}/${edge.to}`)) throw new Error('Topology edges need distinct existing endpoints and cannot be duplicated.');
    pairs.add(`${edge.from}/${edge.to}`);
    if (!observations.includes(edge.observe ?? 'none')) throw new Error('Edge observation must be inputs, outputs or none.');
    // Only an edge touching the corresponding boundary can display that observation.
    if (edge.observe && edge.observe !== 'none' && !nodes.some(n => [edge.from, edge.to].includes(n.id) && n.observe === edge.observe)) throw new Error('An observed edge must touch its matching observation boundary.');
    return { from: edge.from, to: edge.to, observe: edge.observe ?? 'none' };
  });
  const topology = { version: 1, adapter: value.adapter, nodes, edges };
  layoutTopology(topology); // Cycles are rejected rather than given fabricated ordering.
  return topology;
}

export function defaultTopology(adapter, kafka = {}) {
  if (adapter === 'kafka') return { version: 1, adapter, nodes: [
    { id: 'source', kind: 'source', label: 'Scenario events', observe: 'inputs' },
    { id: 'input', kind: 'topic', label: kafka.inputTopic ?? 'Kafka input topic', observe: 'inputs', detail: 'Producer acknowledgments, not proof of application consumption.' },
    { id: 'app', kind: 'processor', label: 'Your application', observe: 'none', detail: 'Application internals are not observed by the Kafka adapter.' },
    { id: 'output', kind: 'topic', label: kafka.outputTopic ?? 'Kafka output topic', observe: 'outputs' }
  ], edges: [{ from: 'source', to: 'input', observe: 'inputs' }, { from: 'input', to: 'app', observe: 'none' }, { from: 'app', to: 'output', observe: 'outputs' }] };
  return { version: 1, adapter, nodes: [
    { id: 'source', kind: 'source', label: 'Scenario events', observe: 'inputs' },
    { id: 'app', kind: 'processor', label: adapter === 'process' ? 'Node.js example' : 'Local application', observe: 'none', detail: adapter === 'process' ? 'A real, fresh Node.js process per run. This is not Flink.' : 'Configure topology to describe this application’s actual transports.' },
    { id: 'output', kind: 'sink', label: adapter === 'process' ? 'Standard output' : 'Captured outputs', observe: 'outputs' }
  ], edges: [{ from: 'source', to: 'app', observe: 'inputs' }, { from: 'app', to: 'output', observe: 'outputs' }] };
}

export function topologyFor(adapter, declared, kafka) {
  return validateTopology(declared?.adapter === adapter ? declared : defaultTopology(adapter, kafka));
}

export function layoutTopology(topology) {
  const levels = new Map(), pending = new Set(topology.nodes.map(n => n.id));
  while (pending.size) {
    let progress = false;
    for (const id of pending) {
      const parents = topology.edges.filter(e => e.to === id).map(e => e.from);
      if (parents.every(p => levels.has(p))) {
        levels.set(id, parents.length ? Math.max(...parents.map(p => levels.get(p))) + 1 : 0);
        pending.delete(id); progress = true;
      }
    }
    if (!progress) throw new Error('Topology must be an acyclic graph with valid endpoints.');
  }
  const maxLevel = Math.max(...levels.values()), columns = Array.from({ length: maxLevel + 1 }, () => []);
  topology.nodes.forEach(node => columns[levels.get(node.id)].push(node));
  const height = Math.max(280, Math.max(...columns.map(c => c.length)) * 132 + 80);
  const nodes = columns.flatMap((column, level) => column.map((node, i) => ({ ...node, x: 38 + level * 290, y: (height - column.length * 132) / 2 + i * 132, width: 218, height: 96 })));
  return { nodes, width: 294 + maxLevel * 290, height };
}

function field(value, path) {
  for (const key of path.split('.')) {
    if (!value || typeof value !== 'object' || !Object.hasOwn(value, key)) return undefined;
    value = value[key];
  }
  return typeof value === 'string' || typeof value === 'number' ? String(value) : undefined;
}

export function expandSources(topology, inputs = []) {
  let nodes = [], edges = [...topology.edges];
  for (const node of topology.nodes) {
    if (!node.identityPath) { nodes.push(node); continue; }
    const identities = [...new Set(inputs.map(record => field(record.value, node.identityPath)).filter(v => v !== undefined))];
    if (!identities.length) { nodes.push(node); continue; }
    const shown = identities.slice(0, 6);
    const replacements = shown.map((identity, i) => ({ ...node, id: `${node.id}~${i}`, label: identity, identity }));
    if (identities.length > 6 || inputs.some(record => field(record.value, node.identityPath) === undefined)) replacements.push({ ...node, id: `${node.id}~other`, label: 'Other / unidentified sources', excludeIdentities: shown });
    nodes.push(...replacements);
    edges = edges.flatMap(edge => edge.from === node.id ? replacements.map(n => ({ ...edge, from: n.id })) : edge.to === node.id ? replacements.map(n => ({ ...edge, to: n.id })) : [edge]);
  }
  return { ...topology, nodes, edges };
}

export function recordsForNode(node, evidence) {
  const records = evidence[node.observe] ?? [];
  if (node.identity !== undefined) return records.filter(r => field(r.value, node.identityPath) === node.identity);
  if (node.excludeIdentities) return records.filter(r => !node.excludeIdentities.includes(field(r.value, node.identityPath)));
  return records;
}

// Saved-run evidence never borrows the current application's state or configuration.
export function graphEvidence(mode, run, snapshot) {
  if (mode === 'run' && run) return { inputs: run.inputs ?? [], outputs: run.outputs ?? [], logs: run.logs ?? [], configuration: run.environment?.configuration, state: undefined, metadata: run.environment, status: run.status, live: run.status === 'running', identity: run.id, label: run.status === 'running' ? 'Capturing this run' : 'Saved run · snapshot', error: run.error };
  if (mode === 'application' && snapshot) return { ...snapshot, inputs: snapshot.inputs ?? [], outputs: snapshot.outputs ?? [], logs: snapshot.logs ?? [], live: !['stopped', 'unavailable', 'error'].includes(snapshot.status), identity: 'application', label: 'Application · captured window' };
  return { inputs: [], outputs: [], logs: [], live: false, identity: 'preview', label: 'Configured topology · no run selected' };
}
