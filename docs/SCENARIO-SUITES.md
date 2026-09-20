# Development lab coverage

StreamPlay is a visual test workbench around application code. Test definitions and
expected results remain JSON or code; the UI runs them and exposes the evidence.
Application-specific behavior comes from trusted startup adapters.

## Features and where they live

| Capability | StreamPlay support |
| --- | --- |
| One action for a scenario suite | Select saved scenarios and **Run selected suite**; application adapters can expose their own **Run scenario suite** |
| Expected versus actual per check | Shared results table for generic suites, application suites, single application scenarios and regression checks |
| Inspect an individual case | Assertions, input records, configuration, output records, logs and full JSON; selection survives background polling |
| Reproducible execution without the UI | `npm run run:suite -- suite.json`; uses the same scenario runner as the server |
| Cancellation and partial evidence | Current adapter closes before completion; later cases remain NOT RUN; no-expectation cases remain OBSERVED |
| Durable evidence | Each generic run and suite is an immutable artifact; JSON exports retain all cases; adapter reports use the adapter's archive |
| Independent sources | Generic source profiles, one-click source creation, individual start/stop/rate/cadence controls and bounded sessions |
| Non-water payloads | JSON templates and configurable identity/value fields, seeded random values, cycles, sequences and optional totals |
| Timed and target-quantity episodes | Meter profiles; explicit units, baseline, zero-flow and silence phases; persisted cumulative totals |
| Threshold and reconnect scenarios | Generic sequence builder plus application-specific rules and assertions supplied by the adapter |
| Real input/configuration/state/output/log inspection | Application controls and stream graph; raw records remain visible |
| MQTT transport | Trusted MQTT adapters with authentication configuration at startup; acknowledgements are distinguished from application output |
| Queue backlog, pause/resume and recovery | Local sandbox queue inspector and recovery checks; adapter-specific observations remain labeled |
| Operator recovery and regression checks | Trusted adapter checks; the separate Kafka/Flink example exercises worker restart and checkpoint recovery |
| Scenario import, export, rerun and comparison | Existing editor and immutable run history; suite export includes the selected scenario definitions |

## Run a generic suite

```sh
npm run run:suite -- examples/suites/orders.process.json
```

This example runs the real Node.js example transform in a fresh process for each
case. It does not run Flink. Exit status is zero only when every case passes.
Artifacts are written under the configured data directory's `runs/` and `suites/`.

In the browser, save scenarios from the editor, select them in **Scenario suite**,
then run them. **Export suite plan** creates the same JSON accepted by the CLI.
Suites accept 1–20 scenarios and validate every plan before starting. An assertion
failure allows later cases to run; transport/cleanup errors or cancellation stop
the suite. A successful publish alone is not a passing test.

Each scenario chooses its adapter. The process example starts fresh; Kafka retains
external application state unless your application resets it. Local adapters must
implement isolation and cleanup for their own systems. Inspect environment metadata.
The runner never rebuilds or deploys application code.

## Application integration

The application snapshot's `reports.suite` contains a suite status, phase and cases.
Each case has an ID, name, status, assertions and a full report. Assertions carry
`expected`, `actual`, `passed` and either `name` or `alertType`. JSON values are
rendered as text, not executable markup. The inspector recognizes `inputs`,
`configs`/`scenario`/`plan`, `outputs`/`alerts`, and `logs`, while retaining every
other field under **full report**. Cleanup errors remain visible even when output
assertions passed. Optional `reports.scenario` and `reports.checks` use the same UI.

These capabilities are generic. Device rule types, application state dumps and
infrastructure regression commands belong in the integration adapter. Suite success
only describes the configured tests and observation intervals; it does not certify
production infrastructure or notification-provider delivery.

