# Architecture and correctness boundaries

The UI edits a versioned scenario. Both the HTTP server and CLI call `runScenario`. An adapter prepares output observation, accepts input events, supplies metadata, reports errors, and closes resources. The runner captures evidence and writes an immutable local snapshot.

```text
Browser / CLI
     |
scenario -> runner -> adapter -> existing application
                |        |              |
                |        +--- outputs --+
                v
       .streamplay/runs/<id>.json
```

`src/scenario.js` defines validation and assertions. `src/runner.js` owns execution state. `src/adapters/` owns transport/process details. `src/store.js` owns local artifacts. `public/` is a small native browser interface with no frontend framework or build step. The only production npm dependency is KafkaJS.

## Current adapters

**Process:** fixed trusted example path, separate Node process, NDJSON stdin/stdout and stderr capture. The source SHA-256 is recorded. No arbitrary command execution is accepted over HTTP. Failure or a process still running at the deadline produces an error. This adapter is a quick way to exercise the workbench, not a substitute for Flink/Kafka Streams semantics.

**Kafka:** requires existing sandbox topics. Snapshot output end offsets, join a new group, seek to those boundaries, publish JSON inputs with broker acknowledgments, observe the full interval, then disconnect and delete only that observer group. Kafka consumer re-delivery of the same partition/offset is deduplicated; distinct offsets with identical payloads remain duplicates. Transactional reads exclude uncommitted records. Connection/authentication support is intentionally limited to local plaintext in this slice.

Offset filtering excludes old records, but does not identify causal outputs. Partition changes, external producers, queued work, event-time behavior, and retained application state are limitations. No reset is implicit. The example's Docker teardown is explicit and scoped to its own Compose project.

## Result model

- `observed`: the declared window ended without an assertion.
- `passed`: supplied expected records matched the observed multiset.
- `failed`: missing or extra records within the window.
- `error`: setup, send, capture, resource limit, or cleanup failure. Partial evidence is retained when possible.

Record order is ignored for assertions; object key order is ignored; array order and duplicate counts are preserved. No automatic time/ID normalization. Error results do not become passing empty-output assertions. A passed check does not prove eventual stream completeness or a known initial application state.

## Deliberate next steps

Separate engine lifecycle adapters from transport adapters, and allow input and output bindings to use different transports. A Kinesis-input application may emit to SQS; supporting Kinesis input alone would not make that application inspectable. SQS observation is a consuming operation, unlike a Kafka observer with an independent consumer group, so queue ownership, acknowledgment, and competing consumers need an explicit contract.

Kinesis needs its own shard/sequence-number logic. Kafka Streams needs its own launch/reset example even though the boundary transport is Kafka. Move comparison semantics into a shared browser/server module as comparison features expand. Add cancellation, pagination, and atomic artifact publication before long-running sessions are treated as stable.
