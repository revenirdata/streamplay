# Where did the events go?

A stream returning to normal throughput does not tell you whether a gap recovered.
StreamPlay can now reconcile **individual event IDs** across explicit checkpoints and
preserve the receipts in an append-only, fsynced journal.

For each checkpoint the workbench shows unique events observed, IDs still unaccounted for,
delayed IDs, and repeated observations. Expand a checkpoint to inspect the exact IDs or
export the evidence. Equal totals cannot hide a missing event offset by a duplicate.

## Run the real MQTT recovery demonstration

Start a dedicated local Mosquitto instance using the existing integration fixture:

```powershell
docker run -d --name streamplay-recovery-demo -p 127.0.0.1:18884:1883 -v "${PWD}/examples/adapters/mosquitto-test.conf:/mosquitto/config/mosquitto.conf:ro" eclipse-mosquitto:2.0.22
node scripts/recovery-demo.js
```

Open the printed localhost URL. Select a subscriber recovery policy and click **Run recovery
experiment**. The same six uniquely identified events are used for both policies:

1. Receive two events as a positive control.
2. Disconnect the subscriber and publish three more events; the broker acknowledges them.
3. Reconnect and publish the sixth event twice, to expose a repeated observation.
4. Reconcile exact IDs after the observation window and save the journal.

With an **ephemeral session**, six unique events are acknowledged by the broker, three
are observed by this subscriber and three remain unaccounted for. With a **persistent
session**, all six are observed, including three delayed events recovered after reconnect.
Both runs expose the repeated sixth event. No counts are fabricated or preloaded.

This interrupts a subscriber while Mosquitto stays running. It does **not** reproduce
Fargate task replacement, a particular production broker, broker persistence across restart,
or a load/reconnection storm. Persistent MQTT subscriptions are not automatically a remedy
for an internal broker-to-Kinesis write path.

Ctrl+C stops the workbench. Remove this demonstration's container when finished:

```powershell
docker rm -f streamplay-recovery-demo
```

`node scripts/recovery-integration.js` asserts both real MQTT experiments in CI and writes
`.streamplay/evidence/delivery/gaps.json` and `recovered.json`. Import either into any
StreamPlay workbench, or use the live demo for a recording.

## Instrument an application

```js
import { createDeliveryAudit } from './src/delivery-audit.js';
const audit = await createDeliveryAudit({
  directory: '.streamplay/delivery',
  stages: ['producer attempted', 'broker accepted', 'Kinesis observed'],
  lateAfterMs: 5000,
});
await audit.plan({ id, sourceId, sequence, payload }); // persisted before publication
await audit.attempt(id);
await audit.receipt('producer attempted', id);
const acknowledgement = await publish(payload);
await audit.receipt('broker accepted', id, acknowledgement);
// An independent downstream observer supplies the matching ID:
await audit.receipt('Kinesis observed', id, { shardId, sequenceNumber });
await audit.close();
const evidence = audit.snapshot();
```

The application's adapter must carry or unambiguously derive the same stable event ID at
each checkpoint. Record confirmed acknowledgements separately from attempted sends. A
timeout may mean unknown acceptance; do not relabel it as a definitive rejection. Use
`failure()` for observer/transport errors and `marker()` for disruptions and recovery.
Surface `deliveryAudit` in an application snapshot to render the checkpoint view.

This is intended for bounded experiments: 10,000 unique events, 100,000 journal entries,
16 KB per entry, and 8 MB total journal entries. Payload hashes identify the planned payload without putting full
telemetry or credentials into receipts. Caller-supplied receipt metadata must be sanitized.
Timestamps are taken by the local observer; they measure observation delay, including
reader lag, not necessarily time spent inside the application.

`readDeliveryAudit()` reconstructs a saved journal. An unclosed journal stays **observing**;
a truncated final line is rejected, and an empty or failed audit cannot be marked accounted
for. Shutdown and cancellation retain the available evidence. This does not guarantee
disk survival or resume/retransmit producers after process failure.

**Unaccounted is not proven permanently lost.** The result is scoped to captured checkpoints
and the observation window. Observer outages, missing correlation, retention expiry and
late delivery must remain visible. Repeated observations can be queue redelivery rather
than duplicated business effects. A telemetry-to-alert transformation is many-to-few;
alert counts are not a substitute for receipts for every telemetry record.
