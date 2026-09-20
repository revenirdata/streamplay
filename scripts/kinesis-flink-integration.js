// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { readFile,mkdir,writeFile } from 'node:fs/promises';
import { up,compose } from './kinesis-flink.js';
import adapter from '../examples/adapters/kinesis-sqs.js';
import { runScenario } from '../src/runner.js';
import { createStore } from '../src/store.js';
const evidence={status:'running',checks:[]};
try {
  await compose('down','--volumes','--remove-orphans');await up();
  const scenario=JSON.parse(await readFile('examples/scenarios/telemetry.kinesis-sqs.json','utf8'));
  let live=false;
  const run=await runScenario(scenario,{adapters:{local:adapter},store:createStore('.streamplay/kinesis-flink'),onUpdate:({run})=>{if(run.status==='running'&&run.outputs.length)live=true;}});
  evidence.run=run;assert.equal(run.status,'passed',run.error??JSON.stringify(run.assertion));assert.ok(live,'Outputs visible before completion');
  assert.ok(run.inputs.every(r=>r.sequenceNumber&&r.shardId));assert.ok(run.outputs.every(r=>r.messageId&&r.value.event_id&&r.value.occurred_at));
  evidence.checks.push('Real Flink timer emits timeout and recovery through independently configured Kinesis/SQS','Live raw JSON, generated IDs/timestamps, semantic assertions and transport receipts retained');
  const observer=await adapter({onOutput:()=>true,onLog:()=>{}});
  try{await assert.rejects(()=>adapter({onOutput:()=>true,onLog:()=>{}}),/already owns/);}finally{await observer.close();}
  evidence.checks.push('Second workbench observer rejected before consuming queue messages');
  await compose('stop','application');
  const failed=await runScenario({...scenario,events:[{device_id:'stopped',value:0}],scheduleMs:undefined,expected:[],observeMs:100},{adapters:{local:adapter}});
  assert.equal(failed.status,'error');evidence.stoppedApplication=failed;evidence.checks.push('Stopped application fails readiness instead of passing an empty-output assertion');
  evidence.status='passed';console.log('PASS actual Kinesis → Flink → SQS: staged timers, live capture, observer ownership, readiness failure.');
}catch(e){evidence.status='failed';evidence.error=e.stack;process.exitCode=1;console.error(e.stack);}
finally{
  try{evidence.logs=await compose('logs','--no-color');await compose('down','--volumes','--remove-orphans');}catch(e){evidence.cleanupError=e.message;process.exitCode=1;}
  await mkdir('.streamplay/evidence/kinesis-flink',{recursive:true});await writeFile('.streamplay/evidence/kinesis-flink/results.json',JSON.stringify(evidence,null,2));
}
