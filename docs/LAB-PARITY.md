# Reusable local-lab capabilities

StreamPlay supports application events and IoT telemetry. It does not implement a particular
company's device protocol, authentication scheme, business rules or infrastructure.
This inventory covers the reusable capabilities of the reference local Flink lab.

| Capability | StreamPlay implementation | Evidence / boundary |
| --- | --- | --- |
| Start a workbench with one command | `npm run lab`; `npm run lab -- --example kafka-flink` owns example startup/shutdown | Default is an explicitly labeled JavaScript example; real Kafka/Flink example is separate |
| Add independent simulated sources | Live sources: one-click add, select, configure value/cadence, send, start/stop; 20-source cap | Fleet unit tests and browser test; finite sessions, not a load-capacity benchmark |
| Support devices other than meters | Numeric telemetry, temperature, order and meter presets; paste a payload and map fields | Nested fields, seeded ranges, cycles, sequences, constants; profile unit tests |
| Preserve raw JSON | Inputs/outputs/transport evidence and exports | Live retention is bounded and marks oversized records; saved scenarios retain bounded full capture |
| Show a clickable architecture | Source identities, named streams/topics, processor and output nodes; search/zoom, raw records, logs/config/state tabs | Existing graph browser tests plus live-source graph test; connections are architecture, not proven event causality |
| Independent offline/online behavior | Stop/resume a selected source while other sources continue | Real MQTT integration and browser workflow; offline means no telemetry, not zero-valued telemetry |
| Timed/target-quantity episode | Meter preset: zero baseline, active interval or target quantity, zero stop phase | Real MQTT subscriber evidence; elapsed-time accounting unit test with delayed publishes |
| Raw units and accumulated readings | Explicit rate conversion, starting total, durable journal; reserve before sending | Restart test, exclusive writer lock, no backward totals. A failed send may leave a gap; total is input accounting, not physical truth |
| MQTT input path | Generic MQTT module, explicit startup connection/authentication, QoS 1, timeouts, cancellation | Real broker/subscriber test; no private device authentication or protocol copied |
| Kafka and local Kinesis/SQS paths | Kafka module; loopback-only LocalStack Kinesis/SQS module | Kafka/Flink CI exercises live source profiles and calculated totals; local Kinesis write independently read and SQS output independently consumed |
| Named scenarios and threshold boundaries | Event sequence builder; below/exactly-at/above preset; silence vs zero | Pure builder checks and real application assertions |
| Missing, duplicate and early output detection | Exact multiset assertions; optional per-phase expected outputs | Test deliberately produces an early alert with the correct overall count and still fails |
| Progress, cancellation, repeat/import/export | Live capture, phase names, episode status, cancellation, source-plan import/export, scenario/run history | Browser and runner checks; import never executes commands or restores checkpoints |
| Application configuration and state | Trusted module `configure` and `state` hooks; visible unsupported state | Demonstrated by JS example; arbitrary Flink state requires explicit instrumentation |
| Existing application controls | Typed forms, dynamic source selection, grouped settings, background-operation locks and cancellation | Browser and HTTP contract tests; bindings retain domain validation and actual runtime ownership |
| Application reports and export | Separate reports/queues views and full application snapshot export | Binding supplies captured evidence; a form or report panel alone does not prove engine execution |
| Fresh scenario state and cleanup | Trusted `prepareScenario` / `cleanupScenario` lifecycle hooks | Example resets state; cleanup failures fail a run. Application-specific temporary rules belong in its adapter |
| Queue health | Approximate visible/in-flight/delayed counts, retention, visibility, redrive, missing-DLQ/backlog warnings | Local SQS; oldest age explicitly unknown without CloudWatch; not called Flink backpressure |
| Pause/resume output inspector | Await current poll, pause further reads, resume and drain | Unit test and real local SQS integration; inspector consumes/deletes test messages |
| Queue recovery experiment | Unique disposable FIFO queues, failed processing, retries, DLQ preservation, same-group recovery, drain, cleanup | Six real local SQS assertions; report persists with cleanup evidence |
| Regression checks from UI | Startup-defined commands, no shell, bounded time/output, required success evidence, JSON report | Positive/zero-evidence/failure/timeout tests; no configured tests is not a pass |
| Run a complete scenario suite | Select saved scenarios in the UI or `npm run run:suite -- suite.json`; adapter-specific suites share the same results view | Sequential execution, continue after assertion failure, stop on transport/cleanup error, immutable run/suite artifacts |
| Expected/actual table and per-test inspection | Generic suites, adapter suites, individual adapter scenarios and regression checks | Inspect assertions, input JSON, configuration, output JSON, logs and full report; selection persists across polling; export per case or full suite |
| Cancel a suite without implying success | Active case cleans up; remaining cases stay NOT RUN; unasserted cases stay OBSERVED | Runner, HTTP and browser tests; CLI exits nonzero unless every case passes |
| Clear without destroying state | Clears displayed capture while preserving totals, application state and report | Unit/browser tests; no queue purge |

## Application-specific adapters

See [scenario suites and result inspection](SCENARIO-SUITES.md) for the suite format,
terminal workflow and adapter report contract.

The generic runner does not bundle a private application's rules or tests. An adapter can
run that application's existing synthesized-infrastructure and delivery-handler tests through
`checks`, inspect its state, configure temporary rules, and clean them up. The startup module
selects the relevant checks. A command exiting zero is insufficient: `successPattern`
must match evidence that a positive number of relevant checks actually passed.

Application-specific rule tests and notification-handler tests run through the binding's
fixed controls. The public interface supports that integration without requiring those rules
to be built into StreamPlay. See [application controls](APPLICATION-CONTROLS.md) for the contract.

## Work still outside parity

Neither the reference lab nor this transfer proves a production broker survives an AWS
replacement/reconnection storm without losing data. That needs identifiable records,
injected failures, recovery/reconciliation and an isolated deployment test. Broker restart
resilience, production EFO/IAM behavior, real notification delivery and a measured Flink
backpressure/load experiment remain separate work; no passing badge here claims them.

Profiles currently apply to the live source group. For simultaneous differently shaped source
groups, use separate workbenches with separate data directories or a custom trusted adapter.
The generator does not emulate physical hardware, radios, arbitrary binary protocols or sensor accuracy.
