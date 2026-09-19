# Inspect records through a real Flink worker restart

From the repository root, run `npm run lab:recovery`. Docker and Node are required.
The launcher starts a dedicated local Kafka/Flink stack and prints the workbench URL
(default http://127.0.0.1:4333). Ports 19092 and 18081 must be free: stop the normal
example stack first. Click **Run worker recovery experiment**.

Click the input topic to inspect the actual JSON read from Kafka, including event ID,
source ID, amount, partition and offset. Click Flink and select **State** to inspect
checkpoint and restore evidence. Click the output topic to see Flink's running totals
as raw JSON. The full topic names are in the inspector and node tooltips.

**Pause view** freezes the graph and its inspector while collection continues. Expanded
transport details remain open during updates. **Export application evidence** saves
the captured input/output records, publication acknowledgements, checkpoints, checks
and experiment timeline. Source controls and cancellation remain live while the graph
is paused.

## What the experiment proves

The SQL groups events by source, counts them and adds their amounts. It is in
`examples/kafka-flink/recovery.sql`; no result is simulated by the workbench.

| Step | Source A | Source B |
| --- | --- | --- |
| Before interruption | 3 + 4 = **7**, two events | **5**, one event |
| Accepted by Kafka while the worker is down | another **6** | another **8** |
| Sent after restart | another **2** | none |
| Required final result | **15**, four events | **13**, two events |

After observing the baseline, the experiment waits for a completed checkpoint, kills
only its dedicated TaskManager, publishes while it is down, and restarts it. Success
requires an actual restore reported by Flink, at least the baseline checkpoint ID,
the expected totals for both independent keys, all six input IDs, and a post-restart
positive control. A running job alone cannot pass the test.

Output records are **updates to an aggregate**, not receipts for individual inputs.
Several output updates for the same source are expected. Input observation is from
an independent Kafka reader; it is not a per-event trace inside Flink.

The fixture uses Flink 1.20.2 and Kafka 3.9.1 with bounded local memory. The JobManager
and Kafka remain running. Checkpoints use JobManager storage, so this experiment does
not establish recovery from JobManager or whole-cluster loss. It does not certify
another application's state, AWS Managed Flink, Kinesis/EFO or exactly-once external
side effects. See [Flink checkpoint storage documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/checkpoints/)
and [upsert Kafka semantics](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/upsert-kafka/).

## Repeat and stop

Each run uses fresh topics, source event IDs and a job. JSON reports and the executed
SQL are retained under `.streamplay/flink-recovery-demo/`. A failed or cancelled run
preserves available evidence and cancels its job. Ctrl+C stops the workbench and removes
only the dedicated `streamplay-recovery` Compose stack and its temporary data.

`npm run test:recovery` runs the same experiment without the UI and stops the stack.
`node scripts/flink-recovery-browser.js` exercises real browser inspection, export and
mobile layout. CI uploads the reports and screenshots. Normal unit/browser tests also
verify that polling preserves expanded metadata and that pausing freezes only the view.
