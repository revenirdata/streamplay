# Prototype validation and remaining evidence

StreamPlay is an early prototype, not an MVP or a validated developer product. Passing bundled examples proves a narrow implementation path. It does not establish easy integration, repeatability across engine states, or demand from other engineers.

## Automated coverage

| Area | Evidence | What it does not establish |
| --- | --- | --- |
| Real process execution | Spawned process, readiness, raw output, source digest, exit errors | Flink or Kafka Streams semantics |
| Kafka/Flink | Actual Docker services, repeated runs, offset boundaries, duplicate counts, negative assertions | Arbitrary user applications, all versions, event-time correctness |
| Assertions | Late duplicates, missing output, invalid JSON, tombstones, selected/missing fields | Eventual completeness or automatic diagnosis |
| Lifecycle | Staged sends, cancellation, partial evidence, adapter/cleanup errors | Forced cancellation of arbitrary plugins |
| Storage | Immutable artifact publication; write failures preserve evidence in the response | Crash-safe capture journals or multi-process run isolation |
| Browser | Edit/run/inspect/save/reload/compare, live records, reconnect/cancel, mobile sizing | Independent user acceptance or a full accessibility audit |
| Application controls | Trusted action dispatch, action/run exclusion, draft configuration preservation, text-only rendering | Sandboxing trusted binding code or canceling arbitrary lifecycle operations |

## Real application pilot

An experimental local adapter can attach to an independently maintained application without moving its business logic into StreamPlay. The first private pilot uses actual Flink with a local Kinesis input, Postgres configuration, and SQS output. Its application-specific code, fixtures, and run bundles stay outside this public repository. This is local-emulator validation, not real AWS validation or a public reproducible compatibility claim.

The pilot checks fresh versus retained keyed state, timer resets with paced inputs, suppressed output alongside a positive control, malformed input alongside a positive control, and application failure. A negative result is retained as evidence, not rewritten into a passing run. Summary findings must distinguish application behavior, transport behavior, and workbench defects.

The private pilot also exercises the replacement developer workflow through StreamPlay's UI: start and real end-to-end readiness, configuration editing, live telemetry/output, operator state inspection, logging controls, clear-view state preservation, reload, and stop/restart. A same-alert rule-change experiment captures pre-change state, applies a new threshold, and exposes an early alert from retained state; a fresh-key positive control checks the full duration. These are application-specific local results, not a claim about all Flink programs. The binding shares one queue observer between the live panel and saved scenarios.

## Gaps that block an MVP claim

- First-class Kinesis and Kafka Streams integrations remain incomplete.
- Configuring a private local adapter still requires substantial engineering work.
- Application lifecycle/configuration/diagnostic controls require a trusted binding; dependency bootstrapping and general state-reset semantics are not implemented.
- Topic/queue isolation and known initial application state are still operator responsibilities.
- Kafka boundary observation alone does not prove application health. An empty-output assertion may match while an unmonitored app is stopped; a real binding needs readiness and a positive control.
- Capture is held in memory until completion; destructive queue acknowledgments are not crash-safe.
- Active/history APIs are intended for small local sessions, not long-running high-volume use.
- No verified macOS setup, engine compatibility matrix, or independent usability acceptance yet.
- No evidence of repeat adoption, contribution demand, or reasons to expect GitHub stars.

## Adoption test

An engineer who did not build StreamPlay should be able to connect an existing application, reproduce a known behavior, inspect relevant records/logs/state assumptions, and rerun after a code or configuration change. Record the setup changes, friction, missing controls, and whether they voluntarily use it again. Compare against their existing workflow. Until that happens, adoption remains a hypothesis.
