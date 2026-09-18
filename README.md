# StreamPlay

**A local workbench for streaming development.**

[![CI](https://github.com/revenirdata/streamplay/actions/workflows/ci.yml/badge.svg)](https://github.com/revenirdata/streamplay/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Open source · In development · Not a release yet**

Created by **[Carl Salazar (@kc-salazar)](https://github.com/kc-salazar)**. An open-source project by **[Revenir](https://www.revenirdata.com/streamplay)**.

Changing a streaming transformation often means juggling an editor, a producer, a consumer, logs, and yesterday's fixtures. StreamPlay brings the **edit → run → inspect → repeat** loop into one local workspace. Keep your application code in your editor. Send JSON events through it, inspect actual records, and retain the useful runs.

Start by exploring. Add expected outputs once you understand the behavior.

![StreamPlay capturing actual JSON output from its included Node.js process example](docs/images/workbench.png)

## Try the working development slice

Requires **Node.js 22+**. No account or cloud service needed.

```sh
git clone https://github.com/revenirdata/streamplay.git
cd streamplay
npm ci
npm start
```

Open **http://127.0.0.1:4317** and click **Run scenario**. Three sample events go through a real, separate Node.js process: two duplicate orders produce two outputs; a zero-quantity order produces none.

Edit [`examples/process/transform.js`](examples/process/transform.js), rerun, and compare the saved outputs. This quickstart exercises the workbench with a **Node.js example**, not Flink or a simulated streaming engine.

## Run Kafka → Flink → Kafka

With Docker Engine/Desktop and Compose v2 running, allocate roughly 4 GB RAM and sufficient disk for the images:

```sh
npm run example:up
npm start
```

Select **Kafka → your application → Kafka** in the workbench and run the same events. The packaged example uses **Apache Kafka 3.9.1**, **Apache Flink 1.20.2**, and **Flink Kafka SQL connector 3.3.0-1.20**. These are pinned example versions, not a claim to cover every version.

The transformation is ordinary [Flink SQL](examples/kafka-flink/orders.sql), executed by Flink. View the job at **http://127.0.0.1:18081**. To edit and resubmit it from a clean sandbox:

```sh
npm run example:down
# Edit examples/kafka-flink/orders.sql in your editor.
npm run example:up
```

`example:down` removes this example's Compose containers and volumes, including its Kafka data and Flink state. It does not delete saved workbench runs. Re-running `example:up` while the job is already running keeps that job; it does not apply SQL edits.

Run the same scenario without the browser:

```sh
npm run run:scenario -- examples/scenarios/orders.kafka.json
```

Exit code `1` means a mismatch or execution error. A scenario without `expected` captures observations; it does not assert correctness. [CI](https://github.com/revenirdata/streamplay/actions/workflows/ci.yml) starts the actual Kafka/Flink containers and checks two consecutive runs.

## What works in this first slice

- Edit JSON event arrays; execute the included process example or send to configured Kafka topics.
- Inspect raw input/output, Kafka offsets and partitions, producer acknowledgments, and process/adapter errors.
- Save local run snapshots and scenarios; export scenario JSON to Git.
- Compare outputs between runs with duplicate-sensitive, order-independent matching.
- Optionally assert exact output records over a declared observation window; run those scenarios in a terminal or CI.

Local data stays under `.streamplay/`, excluded from Git. No telemetry. Runs can include sensitive event data; use appropriate development fixtures.

## What a run means

Kafka capture records output offsets **before** publishing, joins a fresh observation consumer group, and reads committed records at or beyond that boundary. Previously captured offsets are excluded. Outputs arriving during setup or from another producer may still appear: **offset boundaries are not causal correlation**. Use dedicated sandbox topics and one application; do not point this prototype at production.

The full observation interval runs after the final input acknowledgment. `observed` means capture completed without assertions; `passed` means expected records matched during that interval; `failed` means they did not; `error` means execution/capture failed. A finite observation window cannot prove that no more outputs will arrive.

A new run does **not** reset Kafka topics, Flink state, or the external application. The process example starts fresh each time. Initial-state assumptions are recorded in run metadata. Capture is capped at 10,000 output records or 5 MB; overflow is an error, never a successful truncated run.

## Bring your application

Start your app separately and bind it to isolated Kafka input/output topics. Configure the workbench through environment variables; credentials and shell commands are not accepted from the browser.

| Variable | Default |
| --- | --- |
| `STREAMPLAY_KAFKA_BROKERS` | `localhost:19092` |
| `STREAMPLAY_INPUT_TOPIC` | `streamplay-orders-in` |
| `STREAMPLAY_OUTPUT_TOPIC` | `streamplay-orders-out` |
| `STREAMPLAY_PORT` | `4317` |
| `STREAMPLAY_DATA_DIR` | `.streamplay` |

The initial Kafka adapter supports local plaintext brokers and JSON values. SASL/TLS configuration, record keys on input, pacing, schema registries, automatic app launch, and engine log collection are future work. UTF-8 raw values and tombstone metadata are retained on output; binary decoding is not implemented.

## First-release direction

The first release targets these three paths:

| Path | Current state |
| --- | --- |
| Kafka → Flink → Kafka | Initial runnable example and integration CI |
| Kafka → Kafka Streams → Kafka | Planned; native example, lifecycle, and reset verification still needed |
| Kinesis → Flink → Kinesis | Planned; separate transport and AWS validation still needed |

Kafka and Kinesis move events. Flink and Kafka Streams process them. Their semantics and APIs differ; StreamPlay shares the workbench around them rather than pretending they are interchangeable. This public development repository precedes the first release; there is no claim that all three integrations are ready.

See the [roadmap](docs/ROADMAP.md) and [architecture](docs/ARCHITECTURE.md).

## Contribute

Start with [CONTRIBUTING.md](CONTRIBUTING.md). Useful early contributions include reproducible streaming scenarios, raw-data inspection improvements, and adapter correctness tests. The goal is a tool engineers return to while changing real transformations.

```sh
npm ci
npm run check
npm test
npx playwright install chromium
npx playwright test
```

**Creator and lead maintainer:** [Carl Salazar (@kc-salazar)](https://github.com/kc-salazar). **Project home:** [Revenir](https://github.com/revenirdata). [Maintainer model](MAINTAINERS.md) · [Apache-2.0 license](LICENSE).
