# Inspect a streaming pipeline

The dark stream graph puts named topics, streams, processors and outputs alongside captured JSON. Select a node to inspect its records, transport metadata or application context. Search by name, zoom or fit the graph, and follow curved connections through branches and merges. Sources can expand into individual device identities from captured inputs.

## Three evidence views

- **Scenario topology** shows the architecture before execution. It has no captured traffic.
- **Live application** polls a configured local application's bounded capture window. A stopped or unavailable application is not shown as live.
- **Selected run** shows that run's captured inputs, outputs, logs and configuration. New runs save their topology with the evidence, so later configuration changes do not relabel history. Older runs use a visibly marked inferred topology.

Solid connections mark observed input/output boundaries; dashed connections have no internal transfer observations. Brief pulses indicate new boundary captures only. Counts are captured record counts, not throughput or proof of downstream consumption. A graph edge is an architectural relationship, not a per-event causal trace. No generated explanation or AI service is involved.

## Declare your architecture

Default graphs cover the process example and the configured Kafka input/output topics. An operator can supply a topology JSON file at startup:

PowerShell:

```powershell
$env:STREAMPLAY_TOPOLOGY_FILE = './examples/kafka-flink/topology.json'
npm start
```

Bash:

```bash
STREAMPLAY_TOPOLOGY_FILE=./examples/kafka-flink/topology.json npm start
```

The same environment variable works with `npm run run:scenario -- <scenario.json>`. A trusted application launcher can instead pass `topology` to `workbench(config, adapters)`. Topology is operator configuration, not accepted from a browser-supplied scenario. Edit the example if you override the actual topic names.

Contract: `version: 1`, an `adapter` (`process`, `kafka` or `local`), `nodes`, and `edges`. Each node has a unique `id`, `kind`, `label`, optional `detail` and `observe` (`inputs`, `outputs` or `none`). Kinds are `source`, `topic`, `stream`, `processor`, `queue`, `sink` and `configuration`. Edges have `from`, `to` and `observe`. The declared adapter must match the selected execution adapter; otherwise its default graph is shown.

Only the adapter's single input/output capture boundaries are available today. Multiple nodes marked `inputs` describe that same captured input collection; they do not discover or measure independent topics. The same applies to `outputs`. Leave additional uninstrumented stages at `none`. Per-resource observation bindings and automatic discovery are future work.

An input source may include `identityPath`, such as `device.id`. The graph displays the first six captured identities and groups remaining or missing identities in another node. This is a captured-fixture view, not device inventory discovery. Declarations are limited to 32 nodes and 64 edges and must be acyclic. The inspector shows the latest 25 records at a selected boundary; saved artifacts retain the runner's full bounded capture.

## Scope

This is an architecture and evidence navigator for local experiments. It does not edit pipelines, auto-discover a cluster, expose arbitrary Flink operator state, or trace individual records through transformations. The application binding determines which configuration, logs and state can be inspected. Saved runs never borrow current application state.

Interaction reference: [Confluent Stream Lineage](https://www.confluent.io/blog/visualize-apache-kafka-data-easily-with-stream-lineage/) demonstrates named topics and navigable producer/consumer relationships. StreamPlay's graph is an original implementation integrated with local experimental evidence.
