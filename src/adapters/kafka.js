// SPDX-License-Identifier: Apache-2.0
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { record } from '../scenario.js';

export async function kafkaAdapter({ onOutput, onLog }, config) {
  if (!config?.brokers?.length || !config.inputTopic || !config.outputTopic) throw new Error('Kafka connection is not configured.');
  if (config.inputTopic === config.outputTopic) throw new Error('Input and output topics must differ.');
  const kafka = new Kafka({ clientId: 'streamplay', brokers: config.brokers, logLevel: logLevel.ERROR,
    connectionTimeout: 5000, requestTimeout: 10_000, retry: { retries: 2 },
    logCreator: () => ({ log }) => onLog(log.message) });
  const admin = kafka.admin();
  const producer = kafka.producer({ allowAutoTopicCreation: false, createPartitioner: Partitioners.DefaultPartitioner });
  const groupId = `streamplay-${randomUUID()}`;
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false, readUncommitted: false, maxWaitTimeInMs: 100,
    sessionTimeout: 10_000, retry: { retries: 2 } });
  let failure;
  let closed = false;
  const close = async () => {
    if (closed) return;
    closed = true;
    await Promise.allSettled([consumer.disconnect(), producer.disconnect()]);
    // Only this run's ephemeral group is removed; no topic, app state, or other group is changed.
    await admin.deleteGroups([groupId]).catch(() => {});
    await admin.disconnect();
  };
  try {
    await admin.connect();
    const topics = await admin.listTopics();
    if (![config.inputTopic, config.outputTopic].every(t => topics.includes(t))) throw new Error('Create the configured sandbox input and output topics before running.');
    const starts = await admin.fetchTopicOffsets(config.outputTopic);
    const minimum = new Map(starts.map(p => [p.partition, BigInt(p.offset)]));
    const seen = new Set();
    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({ topic: config.outputTopic, fromBeginning: true });
    // Subscribe and join before publishing. Seek to the recorded boundary on every assignment.
    let joinedResolve;
    const joined = new Promise(resolve => { joinedResolve = resolve; });
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      for (const partition of payload.memberAssignment[config.outputTopic] ?? []) {
        if (!minimum.has(partition)) { failure = new Error('Output partitions changed during the run. Retry against a stable sandbox.'); continue; }
        consumer.seek({ topic: config.outputTopic, partition, offset: minimum.get(partition).toString() });
      }
      joinedResolve();
    });
    consumer.on(consumer.events.CRASH, ({ payload }) => { failure = payload.error; joinedResolve(); });
    await consumer.run({ autoCommit: false, eachMessage: async ({ topic, partition, message }) => {
      if (closed || !minimum.has(partition) || BigInt(message.offset) < minimum.get(partition)) return;
      const identity = `${partition}:${message.offset}`;
      if (seen.has(identity)) return;
      seen.add(identity);
      onOutput(record(message.value?.toString('utf8') ?? 'null', { transport: 'kafka', topic, partition, offset: message.offset,
        key: message.key?.toString('utf8') ?? null, timestamp: message.timestamp,
        headers: Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, v]) => [k, Array.isArray(v) ? v.map(x => x.toString('base64')) : v?.toString('base64')])),
        headersEncoding: 'base64', tombstone: message.value === null }));
    } });
    let timer;
    try {
      await Promise.race([joined, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Kafka observer did not join within 15 seconds.')), 15_000); })]);
    } finally { clearTimeout(timer); }
    if (failure) throw failure;
    return {
      metadata: { engine: 'External application (Kafka boundaries)', brokers: config.brokers, inputTopic: config.inputTopic,
        outputTopic: config.outputTopic, outputStartOffsets: starts, groupId, state: 'external application state retained; no reset', isolation: 'dedicated sandbox topics required; output is not causally correlated' },
      async send(events, onSent) {
        for (const value of events) {
          if (failure) throw failure;
          const raw = JSON.stringify(value);
          const acks = await producer.send({ topic: config.inputTopic, acks: -1, messages: [{ value: raw }] });
          onSent(record(raw, { transport: 'kafka', topic: config.inputTopic, acknowledgements: acks }));
        }
      },
      check() { if (failure) throw failure; },
      close
    };
  } catch (error) { await close(); throw error; }
}
