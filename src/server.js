// SPDX-License-Identifier: Apache-2.0
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { configuration } from './config.js';
import { createStore } from './store.js';
import { runScenario } from './runner.js';
import { validateScenario } from './scenario.js';
import { loadLocalAdapter } from './adapters/local.js';
import { validateTopology } from './topology.js';

const publicDir = new URL('../public/', import.meta.url);
const assets = { '/': ['index.html', 'text/html'], '/app.js': ['app.js', 'text/javascript'], '/style.css': ['style.css', 'text/css'], '/graph.js': ['graph.js', 'text/javascript'], '/topology.js': ['../src/topology.js', 'text/javascript'] };
assets['/experiment.js'] = ['../src/experiment.js', 'text/javascript'];
assets['/scenario.js'] = ['../src/scenario.js', 'text/javascript'];
const sample = JSON.parse(await readFile(new URL('../examples/scenarios/orders.process.json', import.meta.url), 'utf8'));

export function workbench(config = configuration(), adapters = {}) {
  const store = createStore(config.dataDir);
  let active = null;
  let controller = null;
  let applicationAction = null;
  const application = config.application;
  const topology = config.topology ? validateTopology(config.topology) : null;
  const server = createServer(async (req, res) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'");
    res.setHeader('Cache-Control', 'no-store');
    const send = (status, body) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body)); };
    try {
      const allowed = `127.0.0.1:${server.address().port}`;
      if (req.headers.host !== allowed && req.headers.host !== `localhost:${server.address().port}`) return send(403, { error: 'Localhost access only.' });
      if (req.headers.origin && ![`http://${allowed}`, `http://localhost:${server.address().port}`].includes(req.headers.origin)) return send(403, { error: 'Cross-origin access is disabled.' });
      const url = new URL(req.url, `http://${allowed}`);
      if (req.method === 'GET' && assets[url.pathname]) {
        const [file, mime] = assets[url.pathname];
        res.writeHead(200, { 'Content-Type': `${mime}; charset=utf-8` });
        return res.end(await readFile(new URL(file, publicDir)));
      }
      if (req.method === 'GET' && url.pathname === '/api/config') return send(200, { sample: (typeof config.sample === 'function' ? config.sample() : config.sample) ?? sample, kafka: config.kafka, topology, localAdapter: Boolean(adapters.local), application: application?.description ?? null, active: active ? { id: active.id, phase: active.phase } : null });
      if (req.method === 'GET' && url.pathname === '/api/application') return application ? send(200, { ...await application.snapshot(), busy: applicationAction, runningScenario: Boolean(active) }) : send(404, { error: 'No application configured.' });
      if (req.method === 'GET' && url.pathname === '/api/active') return send(200, active);
      if (req.method === 'GET' && url.pathname === '/api/runs') return send(200, await store.list('runs'));
      if (req.method === 'GET' && url.pathname === '/api/scenarios') return send(200, await store.list('scenarios'));
      if (req.method !== 'POST' || !['/api/runs', '/api/scenarios', '/api/cancel', '/api/application'].includes(url.pathname)) return send(404, { error: 'Not found.' });
      if (!req.headers['content-type']?.startsWith('application/json')) return send(415, { error: 'Send application/json.' });
      if (url.pathname === '/api/cancel') {
        if (!controller) return send(409, { error: 'No active run.' });
        controller.abort(new Error('Run cancelled by operator.'));
        return send(202, { status: 'cancelling' });
      }
      let bytes = 0;
      const chunks = [];
      for await (const chunk of req) {
        bytes += chunk.length;
        if (bytes > 1_000_000) return send(413, { error: 'Request exceeds 1 MB.' });
        chunks.push(chunk);
      }
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      if (url.pathname === '/api/application') {
        if (!application) return send(404, { error: 'No application configured.' });
        if (active || applicationAction) return send(409, { error: 'Wait for the current run or application action to finish.' });
        if (!application.description.actions.some(action => action.id === body?.action)) return send(400, { error: 'Unknown application action.' });
        applicationAction = body.action;
        try { return send(200, await application.act(body.action, body.value)); }
        finally { applicationAction = null; }
      }
      const scenario = validateScenario(body);
      if (url.pathname === '/api/scenarios') {
        const id = await store.save('scenarios', { scenario, savedAt: new Date().toISOString() });
        return send(201, { id });
      }
      if (active || applicationAction) return send(409, { error: 'A run or application action is already active. Wait for it to finish.' });
      active = { phase: 'connecting' };
      controller = new AbortController();
      try {
        const run = await runScenario(scenario, { store, kafka: config.kafka, topology, adapters, signal: controller.signal, onUpdate: update => { active = update; } });
        return send(201, run);
      } finally { active = null; controller = null; }
    } catch (error) { send(400, { error: error.message }); }
  });
  return server;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const config = configuration();
  if (config.topologyFile) config.topology = validateTopology(JSON.parse(await readFile(config.topologyFile, 'utf8')));
  const server = workbench(config, await loadLocalAdapter(config.adapterModule));
  server.listen(config.port, '127.0.0.1', () => console.log(`StreamPlay → http://127.0.0.1:${server.address().port}\nLocal artifacts: ${config.dataDir}`));
  server.on('error', error => { console.error(error.message); process.exitCode = 1; });
}
