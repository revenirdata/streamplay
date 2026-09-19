# Validate the pipeline beyond the transformation

StreamPlay records inputs, outputs, timing, assertions, logs and local application observations.
It does not infer delivery success from an input acknowledgement or a running process.

| Failure | Experiment | Required evidence |
| --- | --- | --- |
| Consumer missing or stopped | Pause a dedicated sandbox consumer, send bounded traffic, resume | Visible/in-flight/delayed backlog and actual successful processing/deletions before and after |
| Poison message | Make a sandbox handler fail repeatedly | Receive counts, configured retry bound, original payload in DLQ, subsequent message delivery |
| Permanent vs transient delivery failure | Exercise the real handler with provider responses stubbed | Permanent preservation once; transient retry; preservation failure never acknowledged as success |
| Cumulative metrics misinterpreted | Test the synthesized infrastructure | Correct metric name/dimensions; counter increase versus standing value; missing-data behavior |
| Backpressure | Bound input rate, slow processing, compare baseline and recovery | Input rate, processing rate, source lag, task backpressure and recovery time; queue depth alone is insufficient |
| Duplicate routing | Run both real routing implementations with shared fixtures | Exactly the intended producer/consumer acts for each ownership setting |
| Deployment drift | Compare deployed resource definitions and published metrics to source | Deployment revision, live metric presence, alarm actions and consumer bindings |

Record the application revision, initial state, observation interval and cleanup result with each
experiment. Distinguish unknown from zero. Queue counts may be approximate; time since first
observing a backlog is not the age of the oldest message. Inspectors that consume and delete
messages can mask a missing downstream consumer; run that test with the inspector paused.

Only application-specific adapters can provide the relevant observations. StreamPlay currently
does not ship a universal queue monitor, CloudWatch validator, DLQ runner or Flink backpressure
collector. Import/exported scenarios preserve event plans, not deployed infrastructure or checkpoints.
Keep infrastructure tests and provider failure fixtures with their owning applications and run
them alongside StreamPlay scenarios in CI. Do not send real customer notifications in a test.
