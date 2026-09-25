# Isolated AWS transport pilot

StreamPlay can verify that a named AWS profile resolves to the intended account, then run a small real-service smoke test against resources that the pilot creates and owns.

```bash
splay aws doctor --profile personal --region us-east-1
splay aws smoke --profile personal --region us-east-1 \
  --confirm-account 123456789012 --events 5 --max-cost-usd 1
```

`doctor` calls STS `GetCallerIdentity` and makes no mutations. `smoke` requires the returned 12-digit account ID, then creates one uniquely named one-shard Kinesis stream and one standard SQS queue. It sends and independently reads synthetic records through each service, saves JSON evidence under `.streamplay/aws/`, and requests deletion in a `finally` block.

The SDK uses its standard credential provider chain. No access keys are accepted through the browser or stored in StreamPlay files. Prefer a named profile with short-lived IAM Identity Center credentials. Set the profile and Region explicitly so a shell default cannot silently select another account.

## What it proves

- the selected AWS identity and Region are usable;
- the caller can create, write, read and delete the isolated Kinesis resource;
- the caller can create, send, receive and delete the isolated SQS resource;
- exact synthetic event IDs returned from each transport;
- cleanup requests and failures are retained in the evidence file.

It does not connect Kinesis to SQS, run Flink, certify an application, or measure production capacity. A real application pilot still needs an isolated application deployment that consumes the owned stream and writes to the owned output.

## Cost and cleanup boundary

The command permits one Kinesis shard, one queue and at most 100 events. `--max-cost-usd` may be no greater than `$1`; it records the operator's cost guardrail but cannot enforce AWS billing. AWS pricing varies by Region, and billing data is delayed. At the current published US-East example price of `$0.015` per provisioned shard-hour, the intended short run costs cents when deletion succeeds. Always inspect the evidence `cleanup` object. If cleanup reports a failure, delete the exact resource names shown in `resources` before retrying.
