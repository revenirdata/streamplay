// SPDX-License-Identifier: Apache-2.0
import { createHmac, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { deviceId, deterministicReading, renderTemplate, validateFleetProfile } from './profile.js';

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return Number(sorted[Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)].toFixed(2));
}

function metric(id) {
  return { id, connectionsAttempted: 0, connected: 0, connectionFailures: 0, reconnects: 0,
    publishesAttempted: 0, acknowledged: 0, publishFailures: 0, latencyMs: [], errors: [] };
}

function exposeMetric(value) {
  return { id: value.id, connectionsAttempted: value.connectionsAttempted, connected: value.connected,
    connectionFailures: value.connectionFailures, reconnects: value.reconnects, publishesAttempted: value.publishesAttempted,
    acknowledged: value.acknowledged, publishFailures: value.publishFailures,
    latencyMs: { p50: percentile(value.latencyMs, .5), p90: percentile(value.latencyMs, .9), p99: percentile(value.latencyMs, .99), max: value.latencyMs.length ? Number(Math.max(...value.latencyMs).toFixed(2)) : null },
    errors: value.errors };
}

async function targetOptions(target, runId, env, deviceId) {
  const variables = { deviceId, runId };
  const interpolate = value => value?.replace(/{{(deviceId|runId)}}/g, (_, key) => variables[key]);
  const clientId = interpolate(target.clientIdTemplate) ?? `streamplay-${runId}-${target.id}-${deviceId}`.slice(0, 120);
  const options = { clientId, clean: true, reconnectPeriod: 0, connectTimeout: target.timeoutMs, protocolVersion: target.protocolVersion };
  if (target.usernameEnv) options.username = env[target.usernameEnv];
  else if (target.usernameTemplate) options.username = interpolate(target.usernameTemplate);
  if (target.passwordEnv) options.password = env[target.passwordEnv];
  if (target.hmacSecretEnv) {
    const secret = env[target.hmacSecretEnv];
    if (!secret) throw new Error(`Environment variable ${target.hmacSecretEnv} is not set.`);
    options.password = createHmac('sha256', secret).update(deviceId).digest();
  }
  if (target.serverName) options.servername = target.serverName;
  if (target.alpn) options.ALPNProtocols = target.alpn;
  for (const [key, path] of [['ca', target.caFile], ['cert', target.certFile], ['key', target.keyFile]]) if (path) options[key] = await readFile(path);
  return { url: target.url ?? env[target.urlEnv], options };
}

async function mapLimit(items, limit, operation) {
  let next = 0;
  async function worker() {
    for (;;) {
      const index = next++;
      if (index >= items.length) return;
      await operation(items[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
}

export async function runFleetSimulation(input, { clientFactory, ledger = async () => {}, env = process.env, signal, now = () => Date.now(), sleep = delay, onProgress = () => {} } = {}) {
  const profile = validateFleetProfile(input);
  if (!clientFactory) throw new Error('An MQTT client factory is required.');
  const runId = profile.runId ?? randomUUID();
  const startedAt = new Date(now()).toISOString();
  const targetMetrics = new Map(profile.targets.map(target => [target.id, metric(target.id)]));
  const devices = Array.from({ length: profile.devices.count }, (_, index) => ({ index, id: deviceId(profile, index), clients: new Map() }));
  const checkAbort = () => { if (signal?.aborted) throw signal.reason ?? new Error('Simulation cancelled.'); };
  let logicalEvents = 0, duplicatePublishes = 0;

  async function connect(device, target, reconnect = false) {
    checkAbort();
    const metrics = targetMetrics.get(target.id); metrics.connectionsAttempted++;
    try {
      const resolved = await targetOptions(target, runId, env, device.id);
      if (!resolved.url) throw new Error(`Environment variable ${target.urlEnv} is not set.`);
      const client = await clientFactory({ ...resolved, target, deviceId: device.id });
      device.clients.set(target.id, client); metrics.connected++; if (reconnect) metrics.reconnects++;
    } catch (error) {
      metrics.connectionFailures++; if (metrics.errors.length < 20) metrics.errors.push({ phase: 'connect', deviceId: device.id, message: error.message });
      throw error;
    }
  }

  async function reconnect(device) {
    await Promise.all(profile.targets.map(async target => {
      const existing = device.clients.get(target.id);
      if (existing) await existing.close();
      await sleep(profile.faults.reconnectDelayMs, undefined, { signal });
      await connect(device, target, true);
    }));
  }

  async function publish(device, target, topic, payload, eventId, duplicate) {
    const metrics = targetMetrics.get(target.id); metrics.publishesAttempted++;
    const began = now();
    try {
      await device.clients.get(target.id).publish(topic, payload, { qos: target.qos });
      const latencyMs = now() - began; metrics.acknowledged++; metrics.latencyMs.push(latencyMs);
      await ledger({ runId, target: target.id, deviceId: device.id, eventId, duplicate, status: 'acknowledged', latencyMs, observedAt: new Date(now()).toISOString() });
    } catch (error) {
      metrics.publishFailures++; if (metrics.errors.length < 20) metrics.errors.push({ phase: 'publish', deviceId: device.id, eventId, message: error.message });
      await ledger({ runId, target: target.id, deviceId: device.id, eventId, duplicate, status: 'failed', error: error.message, observedAt: new Date(now()).toISOString() });
    }
  }

  try {
    await mapLimit(devices, profile.traffic.connectConcurrency, async device => {
      await Promise.all(profile.targets.map(target => connect(device, target)));
    });
    await Promise.all(devices.map(async device => {
      if (profile.traffic.rampUpMs) await sleep(Math.floor(profile.traffic.rampUpMs * device.index / Math.max(1, devices.length - 1)), undefined, { signal });
      for (let sequence = 1; sequence <= profile.traffic.messagesPerDevice; sequence++) {
        checkAbort();
        if (sequence > 1 && profile.traffic.intervalMs) {
          const jitter = profile.traffic.jitterMs ? Math.floor(deterministicReading({ ...profile, reading: { min: 0, max: profile.traffic.jitterMs, decimals: 0 } }, device.index, sequence)) : 0;
          await sleep(profile.traffic.intervalMs + jitter, undefined, { signal });
        }
        if (profile.faults.reconnectEveryDevice && (device.index + 1) % profile.faults.reconnectEveryDevice === 0 && sequence === profile.faults.reconnectAfterMessage) await reconnect(device);
        const eventId = `${runId}:${device.id}:${sequence}`;
        const variables = { deviceId: device.id, deviceIndex: device.index, sequence, eventId, timestamp: new Date(now()).toISOString(),
          reading: deterministicReading(profile, device.index, sequence),
          counter: (profile.counter.initial + device.index * profile.counter.perDeviceOffset + sequence * profile.counter.increment).toFixed(profile.counter.decimals),
          uptime: String(sequence * Math.max(1, Math.round(profile.traffic.intervalMs / 1000))) };
        const topic = renderTemplate(profile.topicTemplate, variables);
        const payload = JSON.stringify(renderTemplate(profile.payloadTemplate, variables));
        const logicalOrdinal = device.index * profile.traffic.messagesPerDevice + sequence;
        logicalEvents++;
        await Promise.all(profile.targets.map(target => publish(device, target, topic, payload, eventId, false)));
        if (profile.faults.duplicateEvery && logicalOrdinal % profile.faults.duplicateEvery === 0) {
          duplicatePublishes++;
          await Promise.all(profile.targets.map(target => publish(device, target, topic, payload, eventId, true)));
        }
        if (logicalEvents % 1000 === 0) onProgress({ logicalEvents, expected: profile.devices.count * profile.traffic.messagesPerDevice });
      }
    }));
  } finally {
    await Promise.allSettled(devices.flatMap(device => [...device.clients.values()].map(client => client.close())));
  }
  const targets = [...targetMetrics.values()].map(exposeMetric);
  const expectedPerTarget = logicalEvents + duplicatePublishes;
  const status = targets.every(target => target.acknowledged === expectedPerTarget && target.publishFailures === 0 && target.connectionFailures === 0) ? 'passed' : 'failed';
  return { version: 1, runId, name: profile.name, status, startedAt, finishedAt: new Date(now()).toISOString(),
    devices: profile.devices.count, logicalEvents, duplicatePublishes, expectedPublishesPerTarget: expectedPerTarget, targets };
}
