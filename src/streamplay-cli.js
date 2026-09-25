#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = new URL('../', import.meta.url);
const commands = Object.freeze({
  start: { file: 'src/server.js', usage: 'streamplay start', description: 'Open the local workbench.' },
  lab: { file: 'scripts/lab.js', usage: 'streamplay lab [adapter]', description: 'Launch the workbench with live sources and a local lab.' },
  run: { file: 'src/cli.js', usage: 'streamplay run <scenario.json>', description: 'Run one saved scenario.' },
  suite: { file: 'src/suite-cli.js', usage: 'streamplay suite <suite.json>', description: 'Run a saved regression suite.' },
  plan: { file: 'src/plan-cli.js', usage: 'streamplay plan <experiment.json>', description: 'Validate an experiment and calculate capacity.' },
  simulate: { file: 'src/simulate-cli.js', usage: 'streamplay simulate <fleet.json> [--allow-remote]', description: 'Run a bounded MQTT fleet simulation.' },
  aws: { file: 'src/aws-cli.js', usage: 'streamplay aws <doctor|smoke>', description: 'Verify an AWS profile or run an isolated transport smoke test.' }
});

function help() {
  console.log(`StreamPlay — code-first streaming systems test environment

Usage:
  splay <command> [arguments]
  sp <command> [arguments]
  streamplay <command> [arguments]

Commands:
${Object.entries(commands).map(([name, command]) => `  ${name.padEnd(10)} ${command.description}`).join('\n')}
  example    Start, stop, reset, or test a bundled streaming example.
  help       Show this command reference.

Examples:
  splay start
  splay run examples/scenarios/orders.process.json
  splay suite examples/suites/orders.process.json
  splay plan examples/experiments/streaming-smoke.json
  splay simulate examples/simulations/mqtt-fleet.local.json
  splay aws doctor --profile personal --region us-east-1
  splay example kafka-flink up

The JSON files are the source of truth. The browser edits and inspects the same
scenario and experiment artifacts used by the CLI.`);
}

function resolveExample(args) {
  const [name, action = 'up', ...rest] = args;
  const examples = {
    'kafka-flink': 'scripts/example.js',
    'kafka-streams': 'scripts/kafka-streams.js',
    'kinesis-flink': 'scripts/kinesis-flink.js'
  };
  if (!examples[name]) throw new Error('Usage: streamplay example <kafka-flink|kafka-streams|kinesis-flink> <up|down|reset>');
  return { file: examples[name], args: [action, ...rest] };
}

const [name = 'help', ...args] = process.argv.slice(2);
if (name === 'help' || name === '--help' || name === '-h') help();
else {
  try {
    const command = name === 'example' ? resolveExample(args) : commands[name] && { file: commands[name].file, args };
    if (!command) throw new Error(`Unknown command "${name}". Run splay help.`);
    const child = spawn(process.execPath, [fileURLToPath(new URL(command.file, root)), ...command.args], { stdio: 'inherit', cwd: process.cwd() });
    const result = await new Promise(resolve => { child.once('error', error => resolve({ error })); child.once('exit', (code, signal) => resolve({ code, signal })); });
    if (result.error) throw result.error;
    if (result.signal) process.kill(process.pid, result.signal);
    else process.exitCode = result.code ?? 1;
  } catch (error) {
    console.error(error.message); process.exitCode = 1;
  }
}
