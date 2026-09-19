# Live source profiles and operational checks

Run `npm run lab` for the small JavaScript sensor example. It needs no Docker, cloud account
or AI service. The UI identifies it as JavaScript, not Flink. To start the existing real
Kafka/Flink example and the workbench together, use `npm run lab -- --example kafka-flink`.
Stop with Ctrl+C; the managed example is shut down. Each managed launch uses its own Compose
project and removes only that project. Its fixed example ports must be available.

## Configure events without building a device simulator

1. Choose numeric telemetry, temperature, order events or accumulating meter.
2. Preview the JSON and apply the profile while sources are stopped.
3. Add sources, select a source, set its value/cadence, and start a bounded session.

For your own payload, put one real-shaped JSON object in the input-events array, enter the
source identity path and field to vary, and choose configured value or seeded random range.
StreamPlay preserves the remaining fields. Use synthetic data and dedicated test identities.
The advanced profile supports multiple generated fields, constants, cycles and sequences.
`eventIdPath` creates a fresh identifier; without it, any event ID in the template stays unchanged.
Random values are repeatable for the same seed/source/index; timestamps and generated event IDs
are fresh. Export a scenario's actual captured inputs for exact payload replay.

An accumulated total is optional and only makes sense for a rate with defined units. Meter
mode treats the configured value as units/minute. Its raw-rate conversion factor converts
raw units to that rate. A single manual event has zero elapsed duration; a timed session
advances the total using measured elapsed time. The final interval is clipped to the episode
duration. Stored totals are namespaced by transport and source ID. Use a separate source ID
when changing the meaning/unit of a cumulative measurement.

The journal reserves totals before network publication. This prevents uncertain sends from
rewinding a later reading, but does not prove the reserved quantity was delivered. Only one
workbench may own a data directory. After a crash, inspect the PID in `meter-ledger.lock` and
confirm that process is no longer running before removing the stale lock. Do not delete the journal.

## Select a real transport

Connections and trusted executable modules are selected in the starting shell, never imported
from browser JSON. Examples below are PowerShell; use equivalent environment exports on Unix.

```powershell
$env:STREAMPLAY_MQTT_URL='mqtt://127.0.0.1:18884'
$env:STREAMPLAY_MQTT_INPUT_TOPIC='sandbox/events/in'
$env:STREAMPLAY_MQTT_OUTPUT_TOPIC='sandbox/events/out'
npm run lab -- --module examples/lab/mqtt.js
```

Start your own MQTT application and isolated broker first. Optional username/password come
from `STREAMPLAY_MQTT_USERNAME` and `STREAMPLAY_MQTT_PASSWORD`. The module handles JSON over
MQTT; it does not speak a proprietary hardware protocol. Broker PUBACK is input acceptance,
not downstream processing. Use expected outputs to test your application.

```powershell
$env:STREAMPLAY_LOCALSTACK_ENDPOINT='http://127.0.0.1:4567'
$env:STREAMPLAY_KINESIS_STREAM='sandbox-input'
$env:STREAMPLAY_SQS_QUEUES='["http://127.0.0.1:4567/queue/us-east-1/000000000000/sandbox-output.fifo"]'
npm run lab -- --module examples/lab/kinesis-sqs.js
```

Use actual queue URLs returned by your loopback LocalStack. The stream, queues and application
must already exist. This module cannot connect to production AWS. The output inspector starts
paused. Resuming it consumes and deletes messages; it must be resumed before asserted runs.
Use dedicated output queues, not another application's queue. The recovery check creates and
removes its own unique queues and never purges configured output queues.

## Trusted application hooks

A lab module exports an async factory receiving `{ onOutput, onLog }`. Return `name`,
`namespace`, `publish(event)`, optional `close`, `check`, `metadata`, `topology`, `profile`,
`configure(value)`, `state()`, `prepareScenario(scenario, signal)` and `cleanupScenario(prepared)`.
Configuration and state are application-defined. The included JavaScript example demonstrates
them; generic Kafka/MQTT cannot inspect arbitrary processor internals automatically.

`checks` is an optional list of `{ name, command, args, cwd, timeoutMs, successPattern }`.
Only startup code sets these definitions. Use a command that exits nonzero for failed tests and
a success regex that matches a positive executed-test count. Reports retain bounded command
output, so avoid commands that print secrets. Missing checks show **not configured**.

## Validation

`npm test` checks generation, persistent totals, interference prevention, failure handling,
pause semantics and early-output detection. Browser tests cover profiles, source controls,
graphs and exports. `node scripts/lab-integration.js mqtt` uses isolated Mosquitto at 18884.
`node scripts/lab-integration.js queues` uses LocalStack at 4567 with path-style queue URLs.
The latter verifies Kinesis and SQS separately; it does not claim a Flink transform connected them.
CI runs both plus real Kafka/Flink integration, including live source profiles and calculated
output totals for two source identities. Evidence is saved under `.streamplay/evidence`.
