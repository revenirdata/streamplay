// SPDX-License-Identifier: Apache-2.0
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { configuration } from './config.js';
import { createStore } from './store.js';
import { runScenario } from './runner.js';
import { validateScenario } from './scenario.js';

const publicDir = new URL('../public/', import.meta.url);
const assets = { '/': ['index.html', 'text/html'], '/app.js': ['app.js', 'text/javascript'], '/style.css': ['style.css', 'text/css'] };
const sample = JSON.parse(await readFile(new URL('../examples/scenarios/orders.process.json', import.meta.url), 'utf8'));

export function workbench(config = configuration()) {
  const store = createStore(config.dataDir);
  let active = null;
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
      if (req.method === 'GET' && url.pathname === '/api/config') return send(200, { sample, kafka: config.kafka, active });
      if (req.method === 'GET' && url.pathname === '/api/runs') return send(200, await store.list('runs'));
      if (req.method === 'GET' && url.pathname === '/api/scenarios') return send(200, await store.list('scenarios'));
      if (req.method !== 'POST' || !['/api/runs', '/api/scenarios'].includes(url.pathname)) return send(404, { error: 'Not found.' });
      if (!req.headers['content-type']?.startsWith('application/json')) return send(415, { error: 'Send application/json.' });
      let bytes = 0;
      const chunks = [];
      for await (const chunk of req) {
        bytes += chunk.length;
        if (bytes > 1_000_000) return send(413, { error: 'Request exceeds 1 MB.' });
        chunks.push(chunk);
      }
      const scenario = validateScenario(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      if (url.pathname === '/api/scenarios') {
        const id = await store.save('scenarios', { scenario, savedAt: new Date().toISOString() });
        return send(201, { id });
      }
      if (active) return send(409, { error: 'A run is already active. Wait for it to finish.' });
      active = { phase: 'connecting' };
      try {
        const run = await runScenario(scenario, { store, kafka: config.kafka, onUpdate: update => { active = update; } });
        return send(201, run);
      } finally { active = null; }
    } catch (error) { send(400, { error: error.message }); }
  });
  return server;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const config = configuration();
  const server = workbench(config);
  server.listen(config.port, '127.0.0.1', () => console.log(`StreamPlay → http://127.0.0.1:${server.address().port}\nLocal artifacts: ${config.dataDir}`));
  server.on('error', error => { console.error(error.message); process.exitCode = 1; });
}
