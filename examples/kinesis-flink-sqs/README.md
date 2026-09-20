# Local Kinesis → Flink → SQS

A public synthetic example with two independently bound transports and real Apache Flink 1.20.2 processing. LocalStack 4.14.0 emulates Kinesis and SQS. No AWS account or private application checkout is required.

## Start

From the repository root, with Node 22+, Docker and Compose v2:

```sh
npm ci
npm run example:kinesis-flink
```

PowerShell:

```powershell
$env:STREAMPLAY_ADAPTER_MODULE='examples/adapters/kinesis-sqs.js'
npm start
```

Bash:

```sh
STREAMPLAY_ADAPTER_MODULE=examples/adapters/kinesis-sqs.js npm start
```

Import `examples/scenarios/telemetry.kinesis-sqs.json`. The first event yields `received`. Flink's keyed processing-time timer yields `timeout` after two seconds without another event. The next input arrives after a 4.5-second pause, yielding `recovered`, then a second timeout. The expectation contains four records. Semantic assertions select `device_id` and `status`; raw JSON retains generated event IDs and timestamps.

Input: Kinesis `streamplay-telemetry`. Output: SQS `streamplay-events.fifo`, on loopback port 4568. The actual application is [Telemetry.java](../jvm/src/main/java/com/revenir/streamplay/Telemetry.java); the workbench binding is [kinesis-sqs.js](../adapters/kinesis-sqs.js). This is a synthetic inactivity rule.

## Ownership and state

- Use this dedicated stack only. The binding rejects other stream/queue names; custom applications can provide their own trusted bindings.
- The adapter takes an exclusive `.streamplay/locks/kinesis-sqs.lock` before polling. A second observer in the same checkout is rejected. External SQS consumers cannot be detected reliably and must not be attached.
- The output queue must be empty before capture. Existing records are not silently drained to manufacture a clean test.
- Each output is deleted only after the workbench accepts it into memory. This is destructive observation, **not crash-safe storage**. If a process crashes, inspect ownership before removing a stale lock; never remove a live observer's lock.
- Kinesis sequence/shard receipts mean the transport accepted input. The timer/recovery outputs provide the positive processing check.
- State is retained while the job runs. Use fresh device IDs between independent cases, or `npm run example:kinesis-flink:down` and start again. That removes this Compose project's resources, not saved workbench runs or unrelated projects.

The example source reads one shard from LATEST after startup. It does not implement production checkpointed Kinesis offsets, shard discovery/rebalancing or worker recovery. Do not infer real AWS compatibility or production delivery guarantees from this local example. Use the separate [Kafka/Flink recovery experiment](../../docs/FLINK-RECOVERY.md) for its documented checkpoint tests.

## Integration evidence

```sh
npm run test:kinesis-flink
```

CI executes the real job, verifies live output before completion, checks semantic results and raw transport metadata, rejects a second observer, and verifies that a stopped application cannot pass an empty-output assertion. Evidence and container logs are written to `.streamplay/evidence/kinesis-flink/results.json`; the stack is shut down afterward.
