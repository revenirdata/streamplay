# Kafka Streams: retained state and explicit reset

This example runs a real Java 17 / Kafka Streams 3.9.1 application. [Orders.java](../jvm/src/main/java/com/revenir/streamplay/Orders.java) filters nonpositive quantities, identifies duplicates by `(order_id, event_id)`, and accumulates each order's total in a persistent state store. A second delivery of the same event emits nothing. Different events for the same order increment the total.

## Start

From the repository root, with Node 22+, Docker and Compose v2:

```sh
npm ci
npm run example:kafka-streams
```

PowerShell:

```powershell
$env:STREAMPLAY_KAFKA_BROKERS='localhost:19094'
npm start
```

Bash:

```sh
STREAMPLAY_KAFKA_BROKERS=localhost:19094 npm start
```

Import `examples/scenarios/orders.kafka-streams.json` in the workbench. On fresh state, two deliveries of `evt-1` produce one output: `total_cents: 6000, unique_events: 1`.

## Rerun and reset deliberately

Rerunning that fixture without resetting emits **zero** new records: the application remembers `evt-1`. The original one-output expectation therefore fails. Change the expectation to `[]` to test duplicate suppression, or reset before testing fresh-state behavior.

- Application ID: `streamplay-orders-v1`.
- Topics: `streamplay-orders-in`, `streamplay-orders-out`; internal repartition/changelog topics belong to that application ID.
- Local RocksDB state: `/state`, in the Compose project's named state volume.
- `npm run example:kafka-streams:stop` stops containers and retains state and broker data.
- `npm run example:kafka-streams:reset` removes only the `streamplay-kafka-streams` Compose containers, network and volumes. That removes this dedicated broker's topics/offsets/changelogs and application state. It preserves `.streamplay/` saved workbench evidence and unrelated Docker projects.
- Run `npm run example:kafka-streams` after reset. Readiness waits for the current JVM to enter RUNNING, not an earlier log line.

Do not share this example broker with other applications. Its reset deliberately destroys all data in its dedicated broker volume. Deduplication state has no TTL and is intended for bounded fixtures, not production retention policy.

## Integration checks

```sh
npm run test:kafka-streams
```

The test starts from a clean example, checks duplicate suppression, retained-state rerun, JVM restart, a new event for an existing order, and a fresh run after reset. It saves actual records and logs to `.streamplay/evidence/kafka-streams/results.json`, then removes the example stack. CI uploads that evidence. Kafka Streams uses `exactly_once_v2`; arbitrary applications attached to StreamPlay need not have the same semantics.
