# Streaming test model

StreamPlay treats a streaming test as more than an input fixture and an output comparison. A useful experiment declares the workload, routing pressure, time behavior, failures, state assumptions, capacity expectations, and pass/fail boundaries. The versioned experiment JSON is the source of truth for that design.

The browser exposes the controls most engineers change first: entity count, message rate, duration, payload size, burst multiplier, partitions or shards, hottest-key share, and target utilization. Presets fill a complete valid plan. The advanced JSON remains available for everything else, so a basic smoke test does not require a long form.

```json
{
  "version": 1,
  "name": "Burst and recovery",
  "workload": {
    "entities": 250,
    "messagesPerSecondPerEntity": 2,
    "durationSeconds": 300,
    "payloadBytes": 2048,
    "burstMultiplier": 5
  },
  "partitioning": {
    "partitions": 8,
    "keyCardinality": 250,
    "hotKeyPercent": 2
  },
  "faults": {
    "consumerPauseSeconds": 30,
    "sinkDelayMs": 25
  },
  "assertions": {
    "maxBacklogRecords": 10000,
    "maxRecoverySeconds": 120
  }
}
```

Run the planner without the browser:

```sh
npm run streamplay -- plan examples/experiments/streaming-smoke.json
```

The result includes average and peak records and bytes, a conservative Kinesis provisioned-shard estimate, configured utilization, hottest-key pressure, and warnings. It is a design calculation. A real capacity result still needs actual per-shard utilization, throttling, consumer lag, processor backpressure, checkpoint, and recovery evidence from the target environment.

## Control surface

| Area | Controls | Required evidence |
| --- | --- | --- |
| Workload | entities, rate, duration, payload size, ramp, jitter, bursts | generated-event ledger and boundary acknowledgements |
| Partitioning | partition or shard count, key cardinality, skew and hot keys | actual per-partition utilization and throttling |
| Correctness | duplicates, drops, malformed records, schema variants | exact input/output IDs, rejects and failure-store records |
| Event time | out-of-order share, lateness, clock skew, idle partitions | watermarks, late-record outcomes and window results |
| Failure | disconnects, pauses, slow sinks, broker or worker restarts | fault timestamps, backlog growth and recovery boundary |
| State | delivery semantics, checkpoints, retained state and rescaling | checkpoint status, restored state and output reconciliation |
| Capacity | throughput, headroom, saturation and backlog | service metrics plus end-to-end latency and recovery time |
| Assertions | loss, duplicates, ordering, output, latency and recovery | explicit expected versus actual result |

The workbench labels every area as **ready**, **planning**, **adapter**, or **mixed**. A control appearing in an experiment does not imply that every target can execute or observe it. Target adapters provide engine and service-specific actions and evidence.

## From plan to execution

The browser can export an MQTT fleet profile from an experiment. That conversion executes device count, steady rate, duration, payload size, ramp, jitter, duplicate injection, and reconnect injection. It also lists controls that remain unsupported by the broker-level run. This prevents a successful MQTT acknowledgement test from being presented as proof of Kinesis persistence, event-time correctness, processor recovery, or final delivery.

For complete evidence, use the same stable event IDs at each observable boundary:

1. generated intent;
2. producer or broker acknowledgement;
3. stream persistence;
4. processor input and output;
5. output queue or sink;
6. dead-letter or failure store.

Missing evidence means unknown at that boundary. It does not automatically mean permanent loss.
