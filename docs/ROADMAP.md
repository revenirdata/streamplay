# Roadmap to the first release

The repository is public so contributors can participate while the product develops. There is no stable release yet.

## Initial development slice

- [x] Real process execution and a local browser workbench.
- [x] Kafka producer/observer with offset boundaries and record metadata.
- [x] Containerized Kafka/Flink SQL example and CI integration.
- [x] Local immutable run snapshots, saved scenarios, duplicate-sensitive comparisons, optional assertions.
- [x] The same scenario execution contract in the browser and CLI.

## Before the first release

- [ ] Kafka Streams example using an actual JVM application; inspect startup, errors, and explicit state reset.
- [ ] Kinesis transport: shard discovery, starting positions, partial write retries, throttling, and isolated real-AWS verification.
- [ ] Flink/Kinesis example with explicit credentials, resource ownership, cleanup, and cost prerequisites.
- [ ] Application lifecycle: trusted local launch/reset configuration, readiness, status, and engine logs.
- [ ] Input keys and transport metadata, send pacing, NDJSON import, and event templates.
- [ ] Cancel active runs and distinguish cancellation from observation completion.
- [ ] Stronger isolation: fresh topic bindings plus verified app reset, with late outputs and concurrent producers tested.
- [ ] Scenario/run compatibility policy and bounded, paginated history storage.
- [ ] Verified setup on Linux, macOS, and Windows; test version matrix rather than generic compatibility claims.
- [ ] At least three engineers try this on their own applications; address integration friction before announcing a release.

## Good first contribution areas

- NDJSON fixture import with clear invalid-line errors and round-trip export tests.
- Keyboard navigation for the record inspector and accessible JSON expansion.
- More small synthetic scenarios: malformed messages, duplicates, missing outputs, and reordered records.
- JSON field differences for two explicitly selected records, preserving the raw values.

## Later, driven by use

Schema registries, Avro/Protobuf, engine-specific event-time controls, saved baselines, richer CI reporting, additional transports. These are not current support claims.
