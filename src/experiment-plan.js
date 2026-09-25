// SPDX-License-Identifier: Apache-2.0
const LIMITS = Object.freeze({ recordsPerShardSecond: 1_000, bytesPerShardSecond: 1_048_576 });

const defaults = Object.freeze({
  version: 1,
  name: 'Streaming smoke test',
  workload: { entities: 10, messagesPerSecondPerEntity: 1, durationSeconds: 60, payloadBytes: 1024, rampUpSeconds: 5, jitterPercent: 0, burstMultiplier: 1 },
  partitioning: { partitions: 1, keyCardinality: 10, hotKeyPercent: 10 },
  eventTime: { outOfOrderPercent: 0, maxLatenessMs: 0, clockSkewMs: 0, idlePartitionMs: 0 },
  faults: { duplicatePercent: 0, dropPercent: 0, malformedPercent: 0, reconnectsPerThousand: 0, consumerPauseSeconds: 0, sinkDelayMs: 0, processorRestarts: 0 },
  state: { deliverySemantics: 'at-least-once', checkpointIntervalSeconds: 60, retainedState: false },
  assertions: { maxLossPercent: 0, maxDuplicatePercent: 0, maxP99LatencyMs: 1000, maxBacklogRecords: 0, maxRecoverySeconds: 60 },
  capacity: { targetUtilizationPercent: 70 }
});

function object(value, name) {
  if (value === undefined) return {};
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${name} must be an object.`);
  return value;
}

function number(value, name, minimum, maximum, { integer = false } = {}) {
  if (!Number.isFinite(value) || value < minimum || value > maximum || (integer && !Number.isInteger(value))) {
    throw new Error(`${name} must be ${integer ? 'an integer' : 'a number'} from ${minimum} to ${maximum}.`);
  }
  return value;
}

function mergeSection(value, name) {
  return { ...defaults[name], ...object(value[name], name) };
}

export function validateExperimentPlan(value = {}) {
  object(value, 'Experiment plan');
  if (value.version !== undefined && value.version !== 1) throw new Error('Experiment plan version must be 1.');
  const name = value.name ?? defaults.name;
  if (typeof name !== 'string' || !name.trim() || name.length > 120) throw new Error('name must contain 1-120 characters.');
  const workload = mergeSection(value, 'workload');
  const partitioning = mergeSection(value, 'partitioning');
  const eventTime = mergeSection(value, 'eventTime');
  const faults = mergeSection(value, 'faults');
  const state = mergeSection(value, 'state');
  const assertions = mergeSection(value, 'assertions');
  const capacity = mergeSection(value, 'capacity');

  number(workload.entities, 'workload.entities', 1, 1_000_000, { integer: true });
  number(workload.messagesPerSecondPerEntity, 'workload.messagesPerSecondPerEntity', 0.000001, 100_000);
  number(workload.durationSeconds, 'workload.durationSeconds', 1, 86_400, { integer: true });
  number(workload.payloadBytes, 'workload.payloadBytes', 1, 1_048_576, { integer: true });
  number(workload.rampUpSeconds, 'workload.rampUpSeconds', 0, 86_400);
  number(workload.jitterPercent, 'workload.jitterPercent', 0, 100);
  number(workload.burstMultiplier, 'workload.burstMultiplier', 1, 1_000);
  number(partitioning.partitions, 'partitioning.partitions', 1, 100_000, { integer: true });
  number(partitioning.keyCardinality, 'partitioning.keyCardinality', 1, 1_000_000, { integer: true });
  number(partitioning.hotKeyPercent, 'partitioning.hotKeyPercent', 0, 100);
  for (const [key, max] of [['outOfOrderPercent', 100], ['maxLatenessMs', 86_400_000], ['clockSkewMs', 86_400_000], ['idlePartitionMs', 86_400_000]]) number(eventTime[key], `eventTime.${key}`, 0, max);
  for (const key of ['duplicatePercent', 'dropPercent', 'malformedPercent']) number(faults[key], `faults.${key}`, 0, 100);
  number(faults.reconnectsPerThousand, 'faults.reconnectsPerThousand', 0, 1_000);
  number(faults.consumerPauseSeconds, 'faults.consumerPauseSeconds', 0, 86_400);
  number(faults.sinkDelayMs, 'faults.sinkDelayMs', 0, 3_600_000);
  number(faults.processorRestarts, 'faults.processorRestarts', 0, 1_000, { integer: true });
  if (!['at-most-once', 'at-least-once', 'exactly-once'].includes(state.deliverySemantics)) throw new Error('state.deliverySemantics is invalid.');
  number(state.checkpointIntervalSeconds, 'state.checkpointIntervalSeconds', 0, 86_400);
  if (typeof state.retainedState !== 'boolean') throw new Error('state.retainedState must be true or false.');
  for (const key of ['maxLossPercent', 'maxDuplicatePercent']) number(assertions[key], `assertions.${key}`, 0, 100);
  for (const key of ['maxP99LatencyMs', 'maxBacklogRecords', 'maxRecoverySeconds']) number(assertions[key], `assertions.${key}`, 0, 1_000_000_000);
  number(capacity.targetUtilizationPercent, 'capacity.targetUtilizationPercent', 1, 100);
  return structuredClone({ version: 1, name: name.trim(), workload, partitioning, eventTime, faults, state, assertions, capacity });
}

export function estimateKinesisCapacity(value) {
  const plan = validateExperimentPlan(value);
  const { workload, partitioning, capacity } = plan;
  const averageRecordsPerSecond = workload.entities * workload.messagesPerSecondPerEntity;
  const peakRecordsPerSecond = averageRecordsPerSecond * workload.burstMultiplier;
  const averageBytesPerSecond = averageRecordsPerSecond * workload.payloadBytes;
  const peakBytesPerSecond = peakRecordsPerSecond * workload.payloadBytes;
  const utilization = capacity.targetUtilizationPercent / 100;
  const shardsForRecords = Math.ceil(peakRecordsPerSecond / (LIMITS.recordsPerShardSecond * utilization));
  const shardsForBytes = Math.ceil(peakBytesPerSecond / (LIMITS.bytesPerShardSecond * utilization));
  const recommendedShards = Math.max(1, shardsForRecords, shardsForBytes);
  const configuredShards = partitioning.partitions;
  const hottestKeyRecordsPerSecond = peakRecordsPerSecond * partitioning.hotKeyPercent / 100;
  const hottestKeyBytesPerSecond = peakBytesPerSecond * partitioning.hotKeyPercent / 100;
  const configuredRecordUtilizationPercent = peakRecordsPerSecond / (configuredShards * LIMITS.recordsPerShardSecond) * 100;
  const configuredByteUtilizationPercent = peakBytesPerSecond / (configuredShards * LIMITS.bytesPerShardSecond) * 100;
  const warnings = [];
  if (configuredShards < recommendedShards) warnings.push(`Configured shards are below the ${capacity.targetUtilizationPercent}% headroom target.`);
  if (hottestKeyRecordsPerSecond > LIMITS.recordsPerShardSecond || hottestKeyBytesPerSecond > LIMITS.bytesPerShardSecond) warnings.push('The hottest partition key can exceed one shard even when total capacity is sufficient.');
  if (partitioning.keyCardinality < configuredShards) warnings.push('There are fewer partition keys than shards, so traffic cannot spread across every shard.');
  if (workload.payloadBytes > LIMITS.bytesPerShardSecond) warnings.push('One record exceeds the one-mebibyte Kinesis record limit.');
  return {
    model: 'kinesis-provisioned-write',
    assumptions: { ...LIMITS, targetUtilizationPercent: capacity.targetUtilizationPercent, note: 'Planning estimate only. Validate with actual per-shard metrics and downstream lag.' },
    workload: {
      averageRecordsPerSecond, peakRecordsPerSecond, averageBytesPerSecond, peakBytesPerSecond,
      totalLogicalRecords: Math.ceil(averageRecordsPerSecond * workload.durationSeconds),
      totalPayloadBytes: Math.ceil(averageBytesPerSecond * workload.durationSeconds),
      hottestKeyRecordsPerSecond, hottestKeyBytesPerSecond
    },
    configured: { shards: configuredShards, recordUtilizationPercent: Number(configuredRecordUtilizationPercent.toFixed(2)), byteUtilizationPercent: Number(configuredByteUtilizationPercent.toFixed(2)) },
    recommendation: { shards: recommendedShards, constrainedBy: shardsForBytes >= shardsForRecords ? 'bytes' : 'records', shardsForRecords, shardsForBytes },
    warnings
  };
}

export function buildMqttFleetProfile(value, target = { id: 'local', url: 'mqtt://127.0.0.1:1883', qos: 1 }) {
  const plan = validateExperimentPlan(value);
  const messagesPerDevice = Math.ceil(plan.workload.messagesPerSecondPerEntity * plan.workload.durationSeconds);
  if (plan.workload.entities > 10_000) throw new Error('The MQTT fleet runner supports at most 10,000 devices in one run. Split larger experiments across load generators.');
  if (messagesPerDevice > 10_000 || messagesPerDevice * plan.workload.entities > 5_000_000) throw new Error('This plan exceeds one MQTT fleet run. Reduce the duration or rate, or split it into staged runs.');
  if (plan.workload.payloadBytes > 128_000) throw new Error('The MQTT fleet runner supports payloads up to 128 KB. Use a target-specific load adapter for larger records.');
  if (plan.workload.messagesPerSecondPerEntity < 1 / 3600) throw new Error('The MQTT fleet runner interval is limited to one hour. Use staged sends for lower-frequency experiments.');
  if (plan.workload.rampUpSeconds > 3600) throw new Error('The MQTT fleet runner ramp is limited to one hour. Use multiple stages for a longer rollout.');
  const duplicateEvery = plan.faults.duplicatePercent ? Math.max(1, Math.round(100 / plan.faults.duplicatePercent)) : 0;
  const reconnectEveryDevice = plan.faults.reconnectsPerThousand ? Math.max(1, Math.round(1000 / plan.faults.reconnectsPerThousand)) : 0;
  const unsupported = [];
  if (plan.workload.burstMultiplier > 1) unsupported.push('peak burst shape');
  if (plan.partitioning.hotKeyPercent > 0 || plan.partitioning.partitions > 1) unsupported.push('broker-to-stream partition placement');
  if (Object.values(plan.eventTime).some(Boolean)) unsupported.push('watermarks and late-event handling');
  if (plan.faults.dropPercent || plan.faults.malformedPercent) unsupported.push('dropped and malformed event injection');
  if (plan.faults.consumerPauseSeconds || plan.faults.sinkDelayMs || plan.faults.processorRestarts) unsupported.push('downstream failure injection');
  if (plan.state.retainedState || plan.state.deliverySemantics !== 'at-least-once') unsupported.push('processor state semantics');
  return {
    profile: {
      version: 1, name: plan.name, seed: 1,
      devices: { count: plan.workload.entities, idPrefix: 'streamplay-', start: 1, width: 6 },
      traffic: {
        messagesPerDevice,
        intervalMs: Math.max(0, Math.round(1000 / plan.workload.messagesPerSecondPerEntity)),
        jitterMs: Math.max(0, Math.round(1000 / plan.workload.messagesPerSecondPerEntity * plan.workload.jitterPercent / 100)),
        rampUpMs: Math.round(plan.workload.rampUpSeconds * 1000), connectConcurrency: Math.min(500, plan.workload.entities),
        targetPayloadBytes: plan.workload.payloadBytes
      },
      faults: { duplicateEvery, reconnectEveryDevice, reconnectAfterMessage: Math.max(1, Math.ceil(messagesPerDevice / 2)), reconnectDelayMs: 250 },
      reading: { min: 0, max: 100, decimals: 2 },
      counter: { initial: 0, perDeviceOffset: 1, increment: 1, decimals: 0 },
      topicTemplate: 'streamplay/devices/{{deviceId}}/telemetry',
      payloadTemplate: { event_id: '{{eventId}}', device_id: '{{deviceId}}', sequence: '{{sequence}}', emitted_at: '{{timestamp}}', reading: '{{reading}}' },
      targets: [target]
    },
    unsupported
  };
}

export const experimentPresets = Object.freeze({
  smoke: validateExperimentPlan({ name: 'Streaming smoke test' }),
  steady: validateExperimentPlan({ name: 'Steady fleet', workload: { entities: 100, messagesPerSecondPerEntity: 1, durationSeconds: 300, payloadBytes: 1024 }, partitioning: { partitions: 2, keyCardinality: 100, hotKeyPercent: 1 } }),
  burst: validateExperimentPlan({ name: 'Burst and backlog recovery', workload: { entities: 250, messagesPerSecondPerEntity: 2, durationSeconds: 300, payloadBytes: 2048, rampUpSeconds: 30, jitterPercent: 20, burstMultiplier: 5 }, partitioning: { partitions: 8, keyCardinality: 250, hotKeyPercent: 2 }, faults: { consumerPauseSeconds: 30, sinkDelayMs: 25 }, assertions: { maxBacklogRecords: 10_000, maxRecoverySeconds: 120 } }),
  skew: validateExperimentPlan({ name: 'Hot partition pressure', workload: { entities: 1000, messagesPerSecondPerEntity: 1, durationSeconds: 300, payloadBytes: 1024, burstMultiplier: 2 }, partitioning: { partitions: 8, keyCardinality: 1000, hotKeyPercent: 60 } }),
  recovery: validateExperimentPlan({ name: 'Failure and recovery', workload: { entities: 100, messagesPerSecondPerEntity: 2, durationSeconds: 600, payloadBytes: 1024 }, partitioning: { partitions: 4, keyCardinality: 100, hotKeyPercent: 2 }, faults: { duplicatePercent: 2, reconnectsPerThousand: 100, consumerPauseSeconds: 30, processorRestarts: 1 }, state: { retainedState: true, checkpointIntervalSeconds: 30 }, assertions: { maxDuplicatePercent: 0, maxRecoverySeconds: 120 } }),
  eventTime: validateExperimentPlan({ name: 'Late and out-of-order events', eventTime: { outOfOrderPercent: 10, maxLatenessMs: 30_000, clockSkewMs: 5_000, idlePartitionMs: 60_000 } })
});

export const experimentCoverage = Object.freeze([
  { category: 'Workload shape', status: 'ready', controls: 'entities, rate, duration, payload, ramp, jitter, bursts', evidence: 'generated event ledger and publish latency' },
  { category: 'Partition pressure', status: 'planning', controls: 'partition count, key cardinality, hot-key share', evidence: 'capacity estimate; target adapter supplies actual partition metrics' },
  { category: 'Event correctness', status: 'mixed', controls: 'duplicates, drops, malformed records', evidence: 'duplicates are executable; drops and malformed records require an adapter' },
  { category: 'Event time', status: 'adapter', controls: 'out-of-order share, lateness, clock skew, idle partitions', evidence: 'target adapter must expose watermarks and late-record outcomes' },
  { category: 'Failure and recovery', status: 'mixed', controls: 'reconnects, consumer pauses, slow sinks, processor restarts', evidence: 'MQTT reconnect and included recovery labs are executable; other faults depend on the target' },
  { category: 'State', status: 'adapter', controls: 'delivery semantics, checkpoints, retained state, rescaling', evidence: 'processor adapter must expose checkpoints, state, and restart behavior' },
  { category: 'Capacity', status: 'planning', controls: 'throughput, headroom, shard count, skew', evidence: 'planner estimate plus actual utilization, throttling, lag, and backpressure metrics' },
  { category: 'Assertions', status: 'ready', controls: 'loss, duplicates, exact output, latency, backlog, recovery', evidence: 'scenario assertions and boundary receipt reconciliation' }
]);
