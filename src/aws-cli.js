#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
import { buildAwsPilotPlan, discoverAwsIdentity, runAwsPilot, validateAwsPilotOptions } from './aws-pilot.js';

function flags(args) {
  const result = {};
  for (let index = 0; index < args.length; index++) {
    const token = args[index];
    if (!token.startsWith('--')) throw new Error(`Unexpected argument ${token}.`);
    const key = token.slice(2);
    const value = args[index + 1];
    if (!value || value.startsWith('--')) throw new Error(`${token} requires a value.`);
    if (result[key] !== undefined) throw new Error(`${token} was supplied more than once.`);
    result[key] = value; index++;
  }
  return result;
}

function help() {
  console.log(`StreamPlay AWS pilot

Usage:
  splay aws doctor --profile <name> --region <region>
  splay aws smoke --profile <name> --region <region> --confirm-account <12 digits> [--events 5] [--max-cost-usd 1]

doctor performs a read-only STS identity check. smoke creates one uniquely named,
one-shard Kinesis stream and one SQS queue, round-trips synthetic records, writes
evidence under .streamplay/aws, and requests deletion in all outcomes. It does
not connect the two services or prove application/Flink processing.`);
}

const [command = 'help', ...args] = process.argv.slice(2);
if (['help', '--help', '-h'].includes(command)) help();
else {
  try {
    const input = flags(args);
    const options = validateAwsPilotOptions({ profile: input.profile ?? process.env.AWS_PROFILE, region: input.region ?? process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION, events: input.events, maxCostUsd: input['max-cost-usd'], outputDirectory: input['output-directory'] });
    process.env.AWS_PROFILE = options.profile;
    process.env.AWS_REGION = options.region;
    const identity = await discoverAwsIdentity(options);
    if (command === 'doctor') {
      console.log(JSON.stringify({ status: 'authenticated', profile: options.profile, region: options.region, ...identity, mutation: 'none' }, null, 2));
    } else if (command === 'smoke') {
      if (input['confirm-account'] !== identity.account) throw new Error(`Refusing to create resources. Rerun with --confirm-account ${identity.account} after verifying this is the intended account.`);
      const plan = buildAwsPilotPlan({ ...options, account: identity.account });
      console.log(JSON.stringify({ plan, identity, warning: plan.costStatement }, null, 2));
      const evidence = await runAwsPilot(plan, { outputDirectory: options.outputDirectory, onProgress: message => console.error(`[aws] ${message}`) });
      console.log(JSON.stringify(evidence, null, 2));
      if (evidence.status !== 'passed') process.exitCode = 1;
    } else throw new Error(`Unknown AWS command "${command}". Run splay aws help.`);
  } catch (error) {
    console.error(error.message); process.exitCode = 1;
  }
}
