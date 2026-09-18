# Experimental local application adapters

This is a development interface for trying StreamPlay against an existing application. It is not a stable plugin API or a claim of first-class support for that application's transports or engine version.

The operator selects a trusted JavaScript module through `STREAMPLAY_ADAPTER_MODULE` when starting the server or CLI. The browser cannot select executable files. Restart StreamPlay after changing the module. Store application-specific bindings and fixtures in that application's private workspace when appropriate, and set `STREAMPLAY_DATA_DIR` there too.

```js
// adapter.mjs -- ordinary local code with the same access as the running user
export default async function ({ scenario, onOutput, onLog, signal }) {
  // Connect to the app's isolated test resources. Establish observation before returning.
  // On setup failure, release resources here before throwing.
  return {
    metadata: { engine: 'Your application', state: 'Describe the actual reset/retention behavior' },
    async send(events, onSent) {
      // Publish each exact event, await acknowledgment, then call onSent(record).
    },
    async finishInput() { /* optional: signal finite input completion */ },
    check() { /* throw if capture or application health failed */ },
    async close() { /* stop observation; await and release resources */ }
  };
}
```

The record contract is `{raw, value, json, observedAt, ...transportMetadata}`. `raw` preserves the payload, `value` contains decoded JSON, and `json` is false when decoding failed. Set `tombstone: true` for a deleted Kafka value. `onOutput(record)` returns false once capture is closed or a capture limit is reached. A consuming adapter must not acknowledge a record rejected by capture. An accepted in-memory record is **not** a durable acknowledgment guarantee: this prototype writes the run at completion.

`check()` is synchronous and must surface background consumer errors and any application health it claims to monitor. Bound all external I/O. Cancellation is cooperative: use `signal` for sends, and ensure `close()` releases background consumers. Cancellation during setup waits for that adapter's bounded setup to return; it does not forcibly terminate arbitrary plugin code.

For SQS or another destructive consumer, use a queue dedicated to the run/application and declare deletion semantics. Two independent queue consumers do not provide the isolation of two Kafka consumer groups. Persist the application version, launch configuration assumptions, and state/reset behavior in metadata without credentials.

## Staged inputs and selected-field assertions

```json
{
  "version": 1,
  "name": "Timeout and recovery",
  "adapter": "local",
  "events": [{"device":"synthetic-1"}, {"device":"synthetic-1"}],
  "scheduleMs": [0, 8000],
  "observeMs": 2000,
  "matchFields": ["status"],
  "expected": [{"status":"offline"}, {"status":"online"}]
}
```

Each delay is relative to completion of the previous send (the first is before the first event). Observation remains active between sends. The final observation window starts after the final acknowledgment. Timing is wall-clock pacing, not control of an engine's event-time clock or watermarks.

`matchFields` uses explicit dot-separated paths and preserves the original raw outputs. Missing fields fail. Record order is ignored, but duplicate counts remain significant. The assertion above cannot prove that another offline event will not arrive after its final two-second window.

The process example uses an IPC readiness handshake before accepting inputs, so launching Node on a busy machine does not consume the assertion window. Its adapter always uses a fresh process; an external application may retain state.
