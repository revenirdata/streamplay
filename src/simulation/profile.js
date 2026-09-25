// SPDX-License-Identifier: Apache-2.0
const ordinaryName = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/;
const templateToken = /{{(deviceId|deviceIndex|sequence|eventId|timestamp|reading|counter|uptime)}}/g;

function integer(value, name, minimum, maximum) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) throw new Error(`${name} must be an integer from ${minimum} to ${maximum}.`);
  return value;
}

function finite(value, name, minimum, maximum) {
  if (!Number.isFinite(value) || value < minimum || value > maximum) throw new Error(`${name} must be a number from ${minimum} to ${maximum}.`);
  return value;
}

function environmentName(value, name) {
  if (value !== undefined && (typeof value !== 'string' || !/^[A-Z_][A-Z0-9_]*$/.test(value))) throw new Error(`${name} must name an environment variable.`);
  return value;
}

export function validateFleetProfile(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Simulation profile must be an object.');
  if (value.version !== 1) throw new Error('Simulation profile version must be 1.');
  if (typeof value.name !== 'string' || !value.name.trim() || value.name.length > 120) throw new Error('Simulation name must be 1-120 characters.');
  const devices = value.devices ?? {};
  const traffic = value.traffic ?? {};
  const faults = value.faults ?? {};
  const reading = value.reading ?? {};
  const counter = value.counter ?? {};
  const count = integer(devices.count, 'devices.count', 1, 10_000);
  const start = integer(devices.start ?? 1, 'devices.start', 0, 9_999_999);
  const width = integer(devices.width ?? 5, 'devices.width', 1, 12);
  const idPrefix = devices.idPrefix ?? 'device-';
  if (typeof idPrefix !== 'string' || idPrefix.length > 80) throw new Error('devices.idPrefix must be a string of at most 80 characters.');
  const messagesPerDevice = integer(traffic.messagesPerDevice, 'traffic.messagesPerDevice', 1, 10_000);
  if (count * messagesPerDevice > 5_000_000) throw new Error('A simulation may contain at most 5,000,000 logical events.');
  const intervalMs = integer(traffic.intervalMs, 'traffic.intervalMs', 0, 3_600_000);
  const jitterMs = integer(traffic.jitterMs ?? 0, 'traffic.jitterMs', 0, Math.max(intervalMs, 1));
  const rampUpMs = integer(traffic.rampUpMs ?? 0, 'traffic.rampUpMs', 0, 3_600_000);
  const connectConcurrency = integer(traffic.connectConcurrency ?? 50, 'traffic.connectConcurrency', 1, 500);
  const targetPayloadBytes = integer(traffic.targetPayloadBytes ?? 0, 'traffic.targetPayloadBytes', 0, 128_000);
  const duplicateEvery = integer(faults.duplicateEvery ?? 0, 'faults.duplicateEvery', 0, 1_000_000);
  const reconnectEveryDevice = integer(faults.reconnectEveryDevice ?? 0, 'faults.reconnectEveryDevice', 0, 10_000);
  const reconnectAfterMessage = integer(faults.reconnectAfterMessage ?? Math.ceil(messagesPerDevice / 2), 'faults.reconnectAfterMessage', 1, messagesPerDevice);
  const reconnectDelayMs = integer(faults.reconnectDelayMs ?? 250, 'faults.reconnectDelayMs', 0, 60_000);
  const min = finite(reading.min ?? 0, 'reading.min', -1_000_000_000, 1_000_000_000);
  const max = finite(reading.max ?? 100, 'reading.max', -1_000_000_000, 1_000_000_000);
  if (max < min) throw new Error('reading.max must be greater than or equal to reading.min.');
  const decimals = integer(reading.decimals ?? 2, 'reading.decimals', 0, 8);
  const counterInitial = finite(counter.initial ?? 0, 'counter.initial', 0, 1_000_000_000);
  const counterPerDeviceOffset = finite(counter.perDeviceOffset ?? 1, 'counter.perDeviceOffset', 0, 1_000_000_000);
  const counterIncrement = finite(counter.increment ?? 1, 'counter.increment', 0, 1_000_000_000);
  const counterDecimals = integer(counter.decimals ?? 6, 'counter.decimals', 0, 12);
  const seed = integer(value.seed ?? 1, 'seed', 0, 2_147_483_647);
  const runId = value.runId;
  if (runId !== undefined && (typeof runId !== 'string' || !ordinaryName.test(runId))) throw new Error('runId must be an ordinary identifier of at most 64 characters.');
  if (typeof value.topicTemplate !== 'string' || !value.topicTemplate.includes('{{deviceId}}') || value.topicTemplate.length > 512) throw new Error('topicTemplate must be a string containing {{deviceId}}.');
  if (!value.payloadTemplate || typeof value.payloadTemplate !== 'object' || Array.isArray(value.payloadTemplate)) throw new Error('payloadTemplate must be a JSON object.');
  const payloadText = JSON.stringify(value.payloadTemplate);
  if (payloadText.length > 128_000) throw new Error('payloadTemplate must fit within 128 KB.');
  if (!payloadText.includes('{{eventId}}')) throw new Error('payloadTemplate must include {{eventId}} so deliveries can be reconciled.');
  if (!Array.isArray(value.targets) || value.targets.length < 1 || value.targets.length > 4) throw new Error('Supply 1-4 MQTT targets.');
  const ids = new Set();
  const targets = value.targets.map((target, index) => {
    if (!target || typeof target !== 'object' || Array.isArray(target)) throw new Error(`targets[${index}] must be an object.`);
    if (typeof target.id !== 'string' || !ordinaryName.test(target.id) || ids.has(target.id)) throw new Error(`targets[${index}].id must be a unique ordinary identifier.`);
    ids.add(target.id);
    if ((typeof target.url !== 'string' || !/^mqtts?:\/\//.test(target.url)) && environmentName(target.urlEnv, `targets[${index}].urlEnv`) === undefined) throw new Error(`targets[${index}] needs an mqtt:// or mqtts:// url, or urlEnv.`);
    if (target.url !== undefined && target.url.length > 2048) throw new Error(`targets[${index}].url is too long.`);
    const qos = integer(target.qos ?? 1, `targets[${index}].qos`, 0, 2);
    const protocolVersion = integer(target.protocolVersion ?? 4, `targets[${index}].protocolVersion`, 4, 5);
    const timeoutMs = integer(target.timeoutMs ?? 15_000, `targets[${index}].timeoutMs`, 100, 120_000);
    for (const key of ['usernameEnv', 'passwordEnv', 'hmacSecretEnv']) environmentName(target[key], `targets[${index}].${key}`);
    if (target.passwordEnv && target.hmacSecretEnv) throw new Error(`targets[${index}] cannot use passwordEnv and hmacSecretEnv together.`);
    for (const key of ['caFile', 'certFile', 'keyFile', 'serverName', 'clientIdTemplate', 'usernameTemplate']) if (target[key] !== undefined && (typeof target[key] !== 'string' || !target[key])) throw new Error(`targets[${index}].${key} must be a non-empty string.`);
    if (target.alpn !== undefined && (!Array.isArray(target.alpn) || !target.alpn.length || target.alpn.some(value => typeof value !== 'string' || !value))) throw new Error(`targets[${index}].alpn must contain protocol names.`);
    return { id: target.id, ...(target.url === undefined ? {} : { url: target.url }), ...(target.urlEnv === undefined ? {} : { urlEnv: target.urlEnv }), qos, protocolVersion, timeoutMs,
      ...(target.alpn === undefined ? {} : { alpn: target.alpn }),
      ...Object.fromEntries(['usernameEnv', 'passwordEnv', 'hmacSecretEnv', 'caFile', 'certFile', 'keyFile', 'serverName', 'clientIdTemplate', 'usernameTemplate'].filter(key => target[key] !== undefined).map(key => [key, target[key]])) };
  });
  return structuredClone({ version: 1, name: value.name.trim(), ...(runId === undefined ? {} : { runId }), seed,
    devices: { count, start, width, idPrefix }, traffic: { messagesPerDevice, intervalMs, jitterMs, rampUpMs, connectConcurrency, targetPayloadBytes },
    faults: { duplicateEvery, reconnectEveryDevice, reconnectAfterMessage, reconnectDelayMs }, reading: { min, max, decimals },
    counter: { initial: counterInitial, perDeviceOffset: counterPerDeviceOffset, increment: counterIncrement, decimals: counterDecimals },
    topicTemplate: value.topicTemplate, payloadTemplate: value.payloadTemplate, targets });
}

export function renderTemplate(value, variables) {
  if (typeof value === 'string') {
    const exact = value.match(/^{{(deviceId|deviceIndex|sequence|eventId|timestamp|reading|counter|uptime)}}$/);
    if (exact) return variables[exact[1]];
    return value.replace(templateToken, (_, key) => String(variables[key]));
  }
  if (Array.isArray(value)) return value.map(item => renderTemplate(item, variables));
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, renderTemplate(item, variables)]));
  return value;
}

export function deviceId(profile, index) {
  return `${profile.devices.idPrefix}${String(profile.devices.start + index).padStart(profile.devices.width, '0')}`;
}

export function deterministicReading(profile, deviceIndex, sequence) {
  let value = (profile.seed ^ Math.imul(deviceIndex + 1, 0x45d9f3b) ^ Math.imul(sequence, 0x27d4eb2d)) >>> 0;
  value ^= value << 13; value ^= value >>> 17; value ^= value << 5;
  const unit = (value >>> 0) / 0xffffffff;
  return Number((profile.reading.min + unit * (profile.reading.max - profile.reading.min)).toFixed(profile.reading.decimals));
}
