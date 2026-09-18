// SPDX-License-Identifier: Apache-2.0
import { topologyFor, validateTopology, layoutTopology, expandSources, recordsForNode, graphEvidence } from './topology.js';

const icons = { source: '◉', stream: '≋', topic: '≡', queue: '⋮', processor: 'ƒ', sink: '↳', configuration: '⌘' };
const labels = { source: 'SOURCE / DEVICE', stream: 'STREAM', topic: 'KAFKA TOPIC', queue: 'QUEUE', processor: 'PROCESSOR', sink: 'OUTPUT', configuration: 'CONFIGURATION' };
const svgNS = 'http://www.w3.org/2000/svg';
const pretty = value => JSON.stringify(value, null, 2);
const element = (tag, className, text) => { const e = document.createElement(tag); e.className = className; if (text !== undefined) e.textContent = text; return e; };

export function createPipelineGraph() {
  const $ = id => document.getElementById(id);
  const viewport = $('graph-viewport'), canvas = $('graph-canvas'), stage = $('graph-stage');
  let context = {}, selected, selectedView = 'records', structure = '', layout, nodes = new Map(), paths = [], previous = new Map(), inspectorKey;
  let evidence = graphEvidence('preview'), mode = 'preview', zoom = 1, autoFit = true, lastIdentity;
  const scale = () => {
    if (!layout) return;
    if (autoFit) zoom = Math.max(.25, Math.min(1, (viewport.clientWidth - 24) / layout.width, (viewport.clientHeight - 48) / layout.height));
    canvas.style.transform = `scale(${zoom})`;
    stage.style.width = `${layout.width * zoom}px`; stage.style.height = `${layout.height * zoom}px`;
    $('graph-zoom').textContent = `${Math.round(zoom * 100)}%`;
  };
  new ResizeObserver(scale).observe(viewport);
  $('graph-fit').onclick = () => { autoFit = true; scale(); viewport.scrollTo(0, 0); };
  for (const [id, delta] of [['graph-in', .15], ['graph-out', -.15]]) $(id).onclick = () => { autoFit = false; zoom = Math.max(.25, Math.min(1.6, zoom + delta)); scale(); };
  $('graph-mode').onchange = () => { mode = $('graph-mode').value; previous.clear(); render(); };
  $('graph-search').oninput = () => filter();
  function filter() {
    const query = $('graph-search').value.trim().toLowerCase();
    for (const { button, node } of nodes.values()) button.classList.toggle('graph-node--dim', Boolean(query) && !`${node.label} ${node.kind} ${node.detail ?? ''}`.toLowerCase().includes(query));
  }
  function select(id, focus = false) {
    selected = id;
    for (const [key, item] of nodes) item.button.setAttribute('aria-pressed', String(id === key));
    for (const { edge, path } of paths) path.classList.toggle('graph-edge--selected', edge.from === id || edge.to === id);
    if (focus) nodes.get(id)?.button.focus();
    inspect();
  }
  function inspect() {
    const item = nodes.get(selected), root = $('graph-inspector-body');
    if (!item) return;
    const node = item.node;
    const views = node.observe !== 'none' ? ['records', 'details'] : node.kind === 'configuration' ? ['configuration', 'details'] : ['logs', 'configuration', 'state', 'details'];
    if (!views.includes(selectedView)) selectedView = views[0];
    const records = selectedView === 'records' ? recordsForNode(node, evidence) : [];
    const value = selectedView === 'details' ? { node, view: evidence.label, status: evidence.status ?? 'not observed', error: evidence.error, environment: evidence.metadata, limitation: 'Graph edges describe topology, not a per-event causal trace.' }
      : selectedView === 'logs' ? (evidence.logs.length ? evidence.logs.slice(-80).join('\n') : 'No application logs captured in this view.')
      : evidence[selectedView] ?? `${selectedView === 'state' ? 'Internal state' : 'Configuration'} was not captured in this view.`;
    const key = pretty({ node, selectedView, count: records.length, records: records.slice(-25), value: selectedView === 'records' ? null : value });
    if (key === inspectorKey) return;
    inspectorKey = key;
    const focusedTab = $('graph-inspector-tabs').contains(document.activeElement);
    root.replaceChildren(); $('graph-inspector-tabs').replaceChildren();
    $('graph-inspector-kind').textContent = labels[node.kind];
    $('graph-inspector-title').textContent = node.label;
    $('graph-inspector-note').textContent = node.detail ?? (node.observe === 'inputs' ? 'Acknowledged sends at this boundary. This does not prove downstream consumption.' : node.observe === 'outputs' ? 'Records captured at the output boundary. Not a count of all application traffic.' : 'Configured application stage. Internal event processing is not traced.');
    for (const view of views) {
      const button = element('button', '', view[0].toUpperCase() + view.slice(1));
      button.type = 'button'; button.setAttribute('role', 'tab'); button.setAttribute('aria-selected', String(view === selectedView));
      button.tabIndex = view === selectedView ? 0 : -1;
      button.onclick = () => { selectedView = view; inspect(); $('graph-inspector-tabs').querySelector('[aria-selected="true"]').focus(); };
      button.onkeydown = event => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
        event.preventDefault();
        const index = views.indexOf(view);
        selectedView = views[event.key === 'Home' ? 0 : event.key === 'End' ? views.length - 1 : (index + (event.key === 'ArrowRight' ? 1 : -1) + views.length) % views.length];
        inspect(); $('graph-inspector-tabs').querySelector('[aria-selected="true"]').focus();
      };
      $('graph-inspector-tabs').append(button);
    }
    if (selectedView === 'records') {
      root.append(element('p', 'graph-inspector-count', `${records.length} captured · showing latest ${Math.min(records.length, 25)}`));
      if (!records.length) root.append(element('p', 'hint', 'No records captured here in this view. Send inputs or select a saved run.'));
      for (const record of records.slice(-25).reverse()) {
        const card = element('article', 'graph-record');
        card.append(element('div', 'graph-record-time', record.observedAt ?? 'Observation time unavailable'), element('pre', '', record.json ? pretty(record.value) : record.raw ?? pretty(record.value ?? record)));
        const details = element('details', ''), summary = element('summary', '', 'Transport metadata');
        const { raw, value, ...metadata } = record;
        details.append(summary, element('pre', '', pretty(metadata))); card.append(details);
        if (record.raw !== undefined) { const rawDetails = element('details', ''); rawDetails.append(element('summary', '', 'Exact raw payload'), element('pre', '', record.raw)); card.append(rawDetails); }
        root.append(card);
      }
    } else {
      root.append(element('pre', '', typeof value === 'string' ? value : pretty(value)));
    }
    if (focusedTab) $('graph-inspector-tabs').querySelector('[aria-selected="true"]').focus({ preventScroll: true });
  }
  function render() {
    if (!context.config) return;
    const { config, run, snapshot, adapter } = context;
    if (mode === 'run' && !run) mode = 'preview';
    if (mode === 'application' && !config.application) mode = 'preview';
    $('graph-mode').value = mode;
    $('graph-mode').querySelector('[value="run"]').disabled = !run;
    $('graph-mode').querySelector('[value="application"]').disabled = !config.application;
    evidence = graphEvidence(mode, run, snapshot);
    const graphAdapter = mode === 'run' ? run.scenario.adapter : mode === 'application' ? 'local' : adapter ?? config.sample.adapter;
    let base;
    try { base = mode === 'run' ? (run.topology ? validateTopology(run.topology) : topologyFor(graphAdapter)) : topologyFor(graphAdapter, config.topology, config.kafka); }
    catch (error) { $('graph-status').textContent = `Topology unavailable: ${error.message}`; canvas.replaceChildren(); nodes.clear(); $('graph-inspector-body').replaceChildren(); structure = ''; return; }
    const graph = expandSources(base, evidence.inputs);
    const key = JSON.stringify(graph);
    $('graph-status').textContent = evidence.label + (mode === 'run' && !run.topology ? ' · inferred topology' : '');
    $('graph-status').dataset.live = String(evidence.live);
    $('graph-summary').textContent = `${evidence.inputs.length} inputs captured · ${evidence.outputs.length} outputs captured${evidence.status ? ` · ${evidence.status}` : ''}`;
    if (lastIdentity !== evidence.identity) { previous.clear(); lastIdentity = evidence.identity; }
    if (key !== structure) {
      const focused = document.activeElement?.dataset.graphNode;
      structure = key; canvas.replaceChildren(); nodes.clear(); paths = [];
      layout = layoutTopology(graph); canvas.style.width = `${layout.width}px`; canvas.style.height = `${layout.height}px`;
      const svg = document.createElementNS(svgNS, 'svg'); svg.classList.add('graph-edges'); svg.setAttribute('width', layout.width); svg.setAttribute('height', layout.height); svg.setAttribute('aria-hidden', 'true');
      canvas.append(svg);
      for (const edge of graph.edges) {
        const from = layout.nodes.find(n => n.id === edge.from), to = layout.nodes.find(n => n.id === edge.to);
        const x1 = from.x + from.width, y1 = from.y + from.height / 2, x2 = to.x, y2 = to.y + to.height / 2, bend = (x2 - x1) / 2;
        const path = document.createElementNS(svgNS, 'path');
        path.setAttribute('d', `M ${x1} ${y1} C ${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}`);
        path.classList.add('graph-edge'); path.dataset.observed = String(edge.observe !== 'none');
        const title = document.createElementNS(svgNS, 'title'); title.textContent = edge.observe === 'none' ? 'Configured connection; internal transfer not observed' : `${edge.observe} boundary observations; not a causal trace`; path.append(title); svg.append(path);
        paths.push({ edge, path, from, to });
      }
      for (const node of layout.nodes) {
        const button = element('button', 'graph-node'); button.type = 'button'; button.dataset.kind = node.kind; button.dataset.graphNode = node.id;
        button.style.left = `${node.x}px`; button.style.top = `${node.y}px`;
        button.setAttribute('aria-label', `${labels[node.kind]}: ${node.label}`);
        button.title = node.label;
        const icon = element('span', 'graph-node-icon', icons[node.kind]); icon.setAttribute('aria-hidden', 'true');
        const body = element('span', 'graph-node-text');
        const count = element('span', 'graph-node-count');
        body.append(element('span', 'graph-node-kind', labels[node.kind]), element('span', 'graph-node-label', node.label), count);
        button.append(icon, body); button.onclick = () => { selectedView = node.observe === 'none' ? (node.kind === 'configuration' ? 'configuration' : 'logs') : 'records'; select(node.id); };
        nodes.set(node.id, { node, button, count }); canvas.append(button);
      }
      if (!nodes.has(selected)) selected = layout.nodes.find(n => n.kind === 'processor')?.id ?? layout.nodes[0].id;
      select(selected); if (focused) nodes.get(focused)?.button.focus(); scale(); filter();
    }
    for (const { node, count } of nodes.values()) count.textContent = node.observe === 'none' ? (node.kind === 'configuration' ? 'Inspect configuration' : 'Inspect logs & context') : `${recordsForNode(node, evidence).length} ${node.observe === 'inputs' ? 'acknowledged' : 'observed'}`;
    for (const { edge, path, from, to } of paths) {
      const boundary = from.observe === edge.observe ? from : to;
      const records = edge.observe === 'none' ? [] : recordsForNode(boundary, evidence);
      const signature = `${records.length}:${records.at(-1)?.observedAt ?? ''}:${records.at(-1)?.raw ?? ''}`;
      const id = `${edge.from}/${edge.to}`, changed = previous.has(id) && previous.get(id) !== signature;
      path.classList.toggle('graph-edge--captured', records.length > 0);
      if (changed && evidence.live && records.length) { path.classList.remove('graph-edge--pulse'); void path.getBoundingClientRect(); path.classList.add('graph-edge--pulse'); path.onanimationend = () => path.classList.remove('graph-edge--pulse'); }
      if (!evidence.live) path.classList.remove('graph-edge--pulse');
      previous.set(id, signature);
    }
    inspect();
  }
  return {
    update(next) { const first = !context.config; context = { ...context, ...next }; if (first && next.config?.application) mode = 'application'; render(); },
    showRun(run) { context.run = run; mode = 'run'; render(); },
    preview(adapter) { context.adapter = adapter; mode = 'preview'; render(); }
  };
}
