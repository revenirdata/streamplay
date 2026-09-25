// SPDX-License-Identifier: Apache-2.0
import test from 'node:test';
import assert from 'node:assert/strict';
import { buildMqttFleetProfile, estimateKinesisCapacity, experimentCoverage, experimentPresets, validateExperimentPlan } from '../src/experiment-plan.js';

test('accepts a minimal plan and fills safe defaults', () => {
  const plan = validateExperimentPlan({ name: 'Minimal' });
  assert.equal(plan.workload.entities, 10);
  assert.equal(plan.faults.duplicatePercent, 0);
  assert.equal(plan.assertions.maxLossPercent, 0);
});

test('rejects invalid and unbounded experiment controls', () => {
  assert.throws(() => validateExperimentPlan({ workload: { entities: 0 } }), /workload.entities/);
  assert.throws(() => validateExperimentPlan({ partitioning: { hotKeyPercent: 101 } }), /hotKeyPercent/);
  assert.throws(() => validateExperimentPlan({ state: { deliverySemantics: 'magic' } }), /deliverySemantics/);
  assert.throws(() => validateExperimentPlan({ eventTime: [] }), /eventTime/);
});

test('calculates record and byte constrained Kinesis plans', () => {
  const records = estimateKinesisCapacity({ workload: { entities: 2_000, messagesPerSecondPerEntity: 1, payloadBytes: 100, durationSeconds: 60 }, partitioning: { partitions: 1, keyCardinality: 2_000, hotKeyPercent: 1 } });
  assert.equal(records.recommendation.constrainedBy, 'records');
  assert.equal(records.recommendation.shards, 3);
  assert.ok(records.warnings.some(value => value.includes('below')));
  const bytes = estimateKinesisCapacity({ workload: { entities: 100, messagesPerSecondPerEntity: 1, payloadBytes: 20_000, durationSeconds: 60 }, partitioning: { partitions: 3, keyCardinality: 100, hotKeyPercent: 1 } });
  assert.equal(bytes.recommendation.constrainedBy, 'bytes');
  assert.equal(bytes.recommendation.shards, 3);
});

test('flags a hot key that aggregate capacity cannot solve', () => {
  const result = estimateKinesisCapacity({ workload: { entities: 1_000, messagesPerSecondPerEntity: 2, payloadBytes: 1_000, burstMultiplier: 2 }, partitioning: { partitions: 8, keyCardinality: 1_000, hotKeyPercent: 60 } });
  assert.ok(result.warnings.some(value => value.includes('hottest partition key')));
});

test('ships useful presets and labels every coverage area honestly', () => {
  assert.deepEqual(Object.keys(experimentPresets), ['smoke', 'steady', 'burst', 'skew', 'recovery', 'eventTime']);
  assert.equal(experimentCoverage.length, 8);
  assert.ok(experimentCoverage.every(item => item.status && item.evidence));
});

test('turns executable experiment controls into a bounded MQTT fleet profile', () => {
  const { profile, unsupported } = buildMqttFleetProfile({ name: 'Fleet', workload: { entities: 20, messagesPerSecondPerEntity: 2, durationSeconds: 10, payloadBytes: 2048, jitterPercent: 10 }, faults: { duplicatePercent: 10, reconnectsPerThousand: 100 } });
  assert.equal(profile.devices.count, 20);
  assert.equal(profile.traffic.messagesPerDevice, 20);
  assert.equal(profile.traffic.targetPayloadBytes, 2048);
  assert.equal(profile.faults.duplicateEvery, 10);
  assert.equal(profile.faults.reconnectEveryDevice, 10);
  assert.ok(unsupported.includes('broker-to-stream partition placement'));
  assert.throws(() => buildMqttFleetProfile({ workload: { entities: 10_001 } }), /10,000 devices/);
});
