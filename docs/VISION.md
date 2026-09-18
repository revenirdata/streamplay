# StreamPlay: product vision and scope

## Purpose
StreamPlay is a local experimentation and regression workbench for streaming applications. Engineers run their existing code against controlled inputs, inspect the actual evidence, compare behavior after a change, and preserve useful experiments as runnable tests.

The core workflow is: connect an application → establish known execution conditions → edit/send events → inspect inputs, configuration, logs and outputs → change code/configuration → compare → save a regression case.

## Who and which work
For engineers building and maintaining streaming transformations, stateful rules and event-driven integrations. Their code stays in their normal editor, language and repository. Raw JSON and transport metadata remain visible. No required AI service, cloud account, paid backend, telemetry or rewrite into a visual language.

Use cases:
1. Change a filter/enrichment/aggregation and compare exact output records.
2. Exercise a timeout, recovery, duplicate or late event with explicit timing/state assumptions.
3. Verify a CDC/configuration update and inspect the app's exposed state.
4. Reproduce a reported discrepancy, share the evidence, then run the resulting case in CI.

The private application pilot establishes narrow local workflow feasibility. It is not broad engine support, production validation, independent usability acceptance or demand evidence. Client-specific source, fixtures and business rules stay private.

## Why this scope is substantial
The hard work is integration with existing applications, trustworthy state/isolation semantics, evidence provenance and portable execution. A UI alone does not solve these. The intended advantage is a connected development workflow over real applications, with transparent correctness boundaries. It is a hypothesis to validate, not a claim that no competing tools exist.

## Delivery order
The immediate first slice is an interactive **stream graph**: named devices/sources → streams/topics → processing applications → outputs, with curved connections and selectable evidence. It makes an experiment navigable, while preserving the difference between declared architecture and actual boundary observations. Dark mode is the default. This precedes the four deeper capabilities below; it does not replace them.

1. Connect an existing app through a versioned, portable project definition shared by UI and CLI. First support configured local Kafka topics and an optional trusted binding module. Remove machine-specific launchers.
2. Repeat experiments with explicit fresh/retained/unknown state, readiness, reset receipts, observation boundaries and bounded failure behavior. Freshness is verified only where the adapter can establish it. No universal deterministic replay promise.
3. Inspect deterministic evidence: expected/actual differences, changes between runs, a local observation timeline, source/configuration fingerprints and execution conditions. Do not infer causal links or fabricate unavailable state.
4. Export/import versioned regression bundles and run them headlessly with compatibility checks, explicit bindings, meaningful exit codes and CI examples. Imported data never silently authorizes executable code or resource deletion.

Each item has its own acceptance test. A complete bundle depends on the preceding contracts. Existing basic capture/comparison/CLI features are foundations, not completion of these capabilities.

## Scope boundaries
Committed engine direction remains Flink and Kafka Streams; transports remain Kafka and Kinesis, with SQS supported through the private pilot's binding. These are distinct APIs and semantics, not interchangeable connector labels. Complete one public Kafka/Flink workflow first, then expand the support matrix with genuine execution evidence. A process example is a quickstart/test fixture, not streaming-engine validation.

In scope: project setup, a graph for navigating architecture and evidence, lifecycle/readiness, fixtures and pacing, explicit state handling, capture/inspection/comparison, regression artifacts, local UI and CLI, adapter contracts, reproducible examples and integration tests.

Out of scope for this phase: production monitoring/operations, a hosted streaming platform, a transformation language, low-code pipeline authoring, automatic universal root-cause analysis, a required AI backend, billing, broad connector catalogs, production traffic capture and community promotion.

The license remains Apache-2.0. No licensing, ownership or commercial-policy changes are implied by this plan.

## Release and value gates
- A clean checkout follows documented setup without editing StreamPlay source or absolute developer paths.
- An engineer can connect their own supported app and complete edit → run → inspect → rerun.
- Unsupported health, state, ordering and timing assumptions are visible.
- Failure, cancellation, partial capture, duplicate outputs and dirty state have meaningful tests.
- A saved regression case is runnable in the supported CI environment and exposes a deliberately introduced regression.
- Human usability and subsequent reuse are measured separately from automated passes. No star/contributor guarantee and no promotion until the maintainer wants it.

## Contributor design
Keep public roadmap/specs, executable examples, an adapter conformance suite, small ownership boundaries, documented compatibility and useful error reports. Offer bounded contributions such as a synthetic scenario or failing transport test. Maintain a clear review policy and acknowledge contributions. Do not demand contributors reproduce private client infrastructure. Public docs remain the accessible source of truth; Linear is maintainer planning.

## Lessons from established tools
- MLflow: persist run inputs/configuration and artifacts so comparisons have provenance; borrow run discipline, not an ML/AI dependency. https://www.mlflow.org/docs/latest/ml/tracking
- Testcontainers: test real dependencies and explicit lifecycle/readiness. https://testcontainers.com/guides/getting-started-with-testcontainers-for-nodejs/
- Kafka Streams already has TopologyTestDriver; complement engine-native tests instead of replacing their semantics. https://kafka.apache.org/42/javadoc/org/apache/kafka/streams/TopologyTestDriver.html
- Nussknacker already offers scenario testing and result inspection; testing UI alone is not unique. Differentiate through existing-code integration and portable evidence. https://docs.nussknacker.io/scenariosAuthoring/testingAndDebugging/
- Open Source Guides: document contribution paths and respond clearly to contributors. These practices support participation; they do not guarantee popularity. https://opensource.guide/building-community/
