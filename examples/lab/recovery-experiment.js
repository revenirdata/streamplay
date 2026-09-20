// SPDX-License-Identifier: Apache-2.0
import { connect, connectAsync } from 'mqtt';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { createDeliveryAudit } from '../../src/delivery-audit.js';

// A controlled real-transport experiment, not a simulation of any production broker.
export async function runRecoveryExperiment({ directory, persistent, onUpdate = () => {}, signal,
  broker = 'mqtt://127.0.0.1:18884' }) {
  const url = new URL(broker);
  if (url.hostname !== '127.0.0.1' || url.protocol !== 'mqtt:') throw new Error('Recovery experiments require an isolated loopback MQTT broker.');
  const audit = await createDeliveryAudit({ directory, stages: ['producer attempted', 'MQTT PUBACK', 'subscriber received'], lateAfterMs: 700 });
  const prefix = `streamplay-audit-${randomUUID()}`, topic = `${prefix}/events`, clientId = `${prefix}-reader`;
  let producer, reader, receiveFailure;
  const update = () => onUpdate(audit.snapshot());
  const check = () => { signal?.throwIfAborted(); if (receiveFailure) throw receiveFailure; };
  async function connectReader(clean) {
    const client = connect(broker, { clientId, clean, reconnectPeriod: 0, connectTimeout: 5000 });
    client.on('error', error => { receiveFailure = error; });
    client.on('message', (_topic, bytes) => {
      try { const value = JSON.parse(bytes.toString()); void audit.receipt('subscriber received', value.event_id, { topic }).then(update).catch(error => { receiveFailure = error; }); }
      catch (error) { receiveFailure = error; }
    });
    try { await once(client, 'connect', { signal: AbortSignal.timeout(6000) }); await client.subscribeAsync(topic, { qos: 1 }); return client; }
    catch (error) { await client.endAsync(true); throw error; }
  }
  async function publish(sequence, repeated = false) {
    check(); const id = `${audit.snapshot().runId}:${sequence}`, sourceId = sequence % 2 ? 'sensor-a' : 'sensor-b';
    const payload = { event_id: id, source_id: sourceId, sequence, value: sequence * 2 };
    if (!repeated) await audit.plan({ id, sourceId, sequence, payload });
    await audit.attempt(id); await audit.receipt('producer attempted', id);
    try { await producer.publishAsync(topic, JSON.stringify(payload), { qos: 1 }); await audit.receipt('MQTT PUBACK', id, { qos: 1, meaning: 'Broker accepted this publish; subscriber delivery is separate.' }); }
    catch (error) { await audit.failure(id, error.message); throw error; }
    update();
  }
  async function settle(expected, timeout = 4000) {
    const end = Date.now() + timeout;
    while (Date.now() < end) { check(); await audit.flush(); if (audit.snapshot().reconciliation.checkpoints[2].uniqueObserved >= expected) return; await delay(50, undefined, { signal }); }
    throw new Error(`Subscriber failed to reach the ${expected}-event positive control.`);
  }
  try {
    producer = await connectAsync(broker, { reconnectPeriod: 0, connectTimeout: 5000 });
    producer.on('error', error => { receiveFailure = error; });
    reader = await connectReader(!persistent);
    await publish(1); await publish(2); await settle(2);
    await reader.endAsync(); reader = null;
    await audit.marker(`Subscriber disconnected; MQTT session is ${persistent ? 'persistent (clean=false)' : 'ephemeral (clean=true)'}.`);
    for (const n of [3, 4, 5]) await publish(n);
    await delay(1100, undefined, { signal }); check();
    await audit.marker('Subscriber reconnecting; broker remained running throughout this experiment.');
    reader = await connectReader(!persistent);
    await publish(6); await publish(6, true);
    await settle(persistent ? 6 : 3);
    await delay(1000, undefined, { signal }); check(); await audit.flush();
    await audit.marker('Observation window ended after a received post-reconnection positive control.');
  } catch (error) { await audit.failure(null, error.message); throw error; }
  finally {
    // Discard only this experiment's uniquely named persistent session.
    try {
      await reader?.endAsync();
      if (persistent) { const cleanup = await connectAsync(broker, { clientId, clean: true, reconnectPeriod: 0, connectTimeout: 5000 }); await cleanup.endAsync(); }
    } catch (error) { await audit.failure(null, `Session cleanup: ${error.message}`); }
    finally { try { await producer?.endAsync(); } finally { await audit.close(); update(); } }
  }
  return audit.snapshot();
}
