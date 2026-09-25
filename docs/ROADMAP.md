# Roadmap to the first release

The repository is public so contributors can participate while the product develops. There is no stable release yet.

## Initial development slice

- [x] Real process execution and a local browser workbench.
- [x] Kafka producer/observer with offset boundaries and record metadata.
- [x] Containerized Kafka/Flink SQL example and CI integration.
- [x] Local immutable run snapshots, saved scenarios, duplicate-sensitive comparisons, optional assertions.
- [x] The same scenario execution contract in the browser and CLI.

## Before the first release

The [interactive stream graph](PIPELINE-GRAPH.md), live-source controls and [scenario suites](SCENARIO-SUITES.md) are implemented. Next: the deeper capabilities in [the product vision](VISION.md)—portable application projects, verified execution conditions, evidence differences/timeline, and portable CI regression cases. The graph does not imply automatic discovery or internal event tracing.

- [x] Kafka Streams JVM example with retained state, process restart and explicit isolated reset checks.
- [ ] Kinesis transport: the isolated AWS profile/identity and bounded create/write/read/cleanup smoke test is implemented; general shard discovery, configurable starting positions, partial write retries, throttling evidence and application processing remain.
- [x] Public synthetic local Kinesis → actual Flink → SQS pilot with independent input/output bindings, staged timeout/recovery, live raw records and explicit observer ownership.
- [ ] Flink/Kinesis example with explicit credentials, resource ownership, cleanup, and cost prerequisites.
- [x] Trusted application controls for readiness, configuration, state, logs and lifecycle; bundled examples have scoped launch/reset commands. Portable arbitrary-application setup remains incomplete.
- [x] Send pacing, NDJSON import with original-line errors, and configurable event templates.
- [ ] Input keys and broader transport metadata controls.
- [x] Headless MQTT fleet simulation with stable event IDs, multi-target publishing, bounded reconnect/duplicate injection, receipt ledgers, and acknowledgement latency.
- [x] Versioned experiment plans with simple presets, optional workload/time/fault/state/assertion controls, an honest capability map, and Kinesis capacity/hot-key estimates.
- [x] Unified `streamplay` CLI for the workbench, scenarios, suites, experiment planning, fleet simulation, labs, and bundled examples.
- [x] Cooperative cancellation with partial evidence, live inspection, and browser reconnection. Adapter setup and external I/O still need their own bounds.
- [ ] Stronger isolation: fresh topic bindings plus verified app reset, with late outputs and concurrent producers tested.
- [ ] Scenario/run compatibility policy and bounded, paginated history storage.
- [ ] Verified setup on Linux, macOS, and Windows; test version matrix rather than generic compatibility claims.
- [ ] At least three engineers try this on their own applications; address integration friction before announcing a release.

## Good first contribution areas

- More NDJSON fixtures covering real-world schema and encoding edge cases.
- Keyboard navigation for the record inspector and accessible JSON expansion.
- More small synthetic scenarios: malformed messages, duplicates, missing outputs, and reordered records.
- JSON field differences for two explicitly selected records, preserving the raw values.

## Later, driven by use

Schema registries, Avro/Protobuf, engine-specific event-time controls, saved baselines, richer CI reporting, additional transports. These are not current support claims.
