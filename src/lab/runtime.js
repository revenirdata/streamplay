// SPDX-License-Identifier: Apache-2.0
import { readFile, mkdir, writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
import { openLedger } from './ledger.js';
import { createFleet } from './fleet.js';
import { createQueueInspector, sqsSandbox, verifyQueueRecovery } from './queues.js';
import { runChecks } from './checks.js';
import { record } from '../scenario.js';
import { sourcePresets, validateProfile } from '../source-profile.js';

export async function loadLab(path, dataDir) {
  if (!path) return null;
  const sourceSha256 = createHash('sha256').update(await readFile(path)).digest('hex');
  const module = await import(pathToFileURL(path).href);
  return createLab(module.default, { dataDir, sourceSha256 });
}

export async function createLab(factory, { dataDir, sourceSha256 = 'injected-test-factory' }) {
  const inputs = [], outputs = [], logs = [];
  const shutdown = new AbortController();
  let activeChecks;
  let currentOutput, report = null, busy = null, configuration = {}, transport, ledger, inspector, queueApi, closed = false;
  const retain = (list, value) => {
    const serialized = JSON.stringify(value);
    if (serialized.length > 50000) value = { raw: serialized.slice(0, 50000), json: false, truncated: true, reason: 'Live record exceeded 50 KB; export an asserted run for bounded full capture.' };
    list.push(value);
    while (list.length > 500 || JSON.stringify(list).length > 2000000) list.shift();
  };
  const onLog = value => retain(logs, String(value).slice(0, 4000));
  const onOutput = output => { retain(outputs, output); currentOutput?.(output); };
  try {
    transport = await factory({ onOutput, onLog });
    if (!transport || typeof transport.publish !== 'function' || !transport.namespace) throw new Error('Lab module must provide publish and namespace.');
    ledger = await openLedger(join(dataDir, 'lab'));
    if (transport.queueEndpoint) {
      queueApi = sqsSandbox(transport.queueEndpoint);
      inspector = createQueueInspector(queueApi, transport.queueUrls ?? [], (message, url) => onOutput(record(message.Body, { transport: 'sqs', queue: url, messageId: message.MessageId, attributes: message.Attributes })));
    }
  } catch (error) { await transport?.close?.(); await ledger?.close(); queueApi?.close(); throw error; }
  const captureInput = (event, acknowledgement) => retain(inputs, record(JSON.stringify(event), { transport: transport.name, acknowledgement }));
  const profilePath = join(dataDir, 'lab', 'source-profile.json');
  let savedProfile;
  try { savedProfile = JSON.parse(await readFile(profilePath, 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') { await transport.close?.(); await ledger.close(); await inspector?.close(); queueApi?.close(); throw error; } }
  let fleet;
  try { fleet = createFleet({ publish: transport.publish, ledger, namespace: transport.namespace, onInput: captureInput, onLog, profile: savedProfile ?? transport.profile ?? sourcePresets.telemetry }); }
  catch (error) { await transport.close?.(); await ledger.close(); await inspector?.close(); queueApi?.close(); throw error; }
  fleet.add();
  async function saveReport(value) {
    const id = randomUUID(); report = { ...value, id, sourceSha256 };
    await mkdir(join(dataDir, 'lab-reports'), { recursive: true });
    await writeFile(join(dataDir, 'lab-reports', `${id}.json`), JSON.stringify(report, null, 2), { flag: 'wx' });
    return report;
  }
  const lab = {
    description: { name: transport.name, limit: 20, sourceSha256, queues: Boolean(inspector), configuration: Boolean(transport.configure), state: Boolean(transport.state), checks: (transport.checks ?? []).map(check => check.name) },
    topology: transport.topology,
    get busy() { return busy; },
    get active() { return fleet.active; },
    async snapshot() {
      return { status: closed ? 'closed' : busy ?? (fleet.active ? 'sending' : 'ready'), profile: fleet.profile, devices: fleet.snapshot(), inputs, outputs, logs, configuration,
        queues: inspector ? await inspector.snapshot() : null, report, sourceSha256, state: transport.state ? await transport.state() : { available: false, reason: 'This transport does not expose application state.' } };
    },
    async action(action, value = {}) {
      if (closed) throw new Error('Lab is closed.');
      if (busy) throw new Error('Wait for the current lab action.');
      busy = action;
      try {
        if (action === 'add') return { id: fleet.add(value) };
        if (action === 'profile') {
          if (fleet.active) throw new Error('Stop sources before changing their profile.');
          const profile = validateProfile(value);
          if (profile.accumulate && fleet.snapshot().some(source => source.rate < 0)) throw new Error('Set source values to zero or higher before selecting a meter profile.');
          const temporary = `${profilePath}.${randomUUID()}.tmp`;
          await writeFile(temporary, JSON.stringify(profile), { flag: 'wx' });
          await rename(temporary, profilePath);
          fleet.setProfile(profile); return fleet.profile;
        }
        if (action === 'configure-device') return fleet.configure(value);
        if (action === 'set-total') return await fleet.setTotal(value);
        if (action === 'send') return await fleet.send(value.id);
        if (action === 'start') return fleet.start(value.id, value.durationMs);
        if (action === 'stop') return await fleet.stop(value.id);
        if (action === 'episode') return fleet.episode(value);
        if (action === 'pause-consumer' && inspector) return await inspector.pause();
        if (action === 'resume-consumer' && inspector) return inspector.resume();
        if (action === 'clear') { inputs.length = 0; outputs.length = 0; logs.length = 0; return { message: 'View cleared. Device totals, application state and last report retained.' }; }
        if (action === 'configure-application' && transport.configure) {
          if (fleet.active) throw new Error('Stop all devices before changing application configuration.');
          configuration = await transport.configure(value); return configuration;
        }
        if (action === 'queue-check' && queueApi) return await saveReport(await verifyQueueRecovery(queueApi));
        if (action === 'checks') {
          if (fleet.active) throw new Error('Stop devices before running regression checks.');
          activeChecks = runChecks(transport.checks, { signal: shutdown.signal });
          try { return await saveReport(await activeChecks); } finally { activeChecks = null; }
        }
        throw new Error('Unknown or unsupported lab action.');
      } finally { busy = null; }
    },
    async scenarioAdapter({ onOutput: observe, scenario, signal }) {
      if (fleet.active || busy) throw new Error('Stop live devices before an asserted scenario to avoid mixing unrelated output.');
      if (inspector && (await inspector.snapshot()).paused) throw new Error('Resume the sandbox output inspector before running assertions.');
      currentOutput = observe;
      let prepared;
      try { prepared = await transport.prepareScenario?.(scenario, signal); }
      catch (error) { currentOutput = null; throw error; }
      return {
        metadata: { ...transport.metadata, engine: transport.name, adapterSourceSha256: sourceSha256, initialState: prepared ?? 'retained unless the trusted adapter resets it', isolation: 'Dedicated sandbox required; input acknowledgements do not prove output processing.' },
        async send(events, onSent) { for (const event of events) { signal?.throwIfAborted(); const acknowledgement = await transport.publish(event); captureInput(event, acknowledgement); onSent(record(JSON.stringify(event), { transport: transport.name, acknowledgement })); } },
        check: () => transport.check?.(),
        async close() { currentOutput = null; await transport.cleanupScenario?.(prepared); }
      };
    },
    async close() {
      closed = true;
      shutdown.abort(new Error('Workbench shutdown.'));
      await activeChecks?.catch(() => {});
      await fleet.close(); await inspector?.close();
      try { await transport.close?.(); } finally { queueApi?.close(); await ledger.close(); }
    }
  };
  return lab;
}
