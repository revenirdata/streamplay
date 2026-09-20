// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { up, compose, ready } from './kafka-streams.js';
import { runScenario } from '../src/runner.js';
import { createStore } from '../src/store.js';
const kafka = { brokers:['localhost:19094'], inputTopic:'streamplay-orders-in', outputTopic:'streamplay-orders-out' };
const event = { event_id:'evt-1', order_id:'order-101', quantity:2, unit_price_cents:3000 };
const expected = [{order_id:'order-101',total_cents:6000,unique_events:1}];
const store=createStore('.streamplay/kafka-streams');
const evidence={engine:'Kafka Streams 3.9.1 / JVM 17',runs:[],status:'running'};
async function run(name, events, outputs) {
  const result=await runScenario({version:1,name,adapter:'kafka',events,expected:outputs,observeMs:6000},{kafka,store});
  evidence.runs.push(result); assert.equal(result.status,'passed',result.error??JSON.stringify(result.assertion));
  assert.ok(result.inputs.every(r=>r.transport==='kafka')); assert.ok(result.outputs.every(r=>r.offset!==undefined));
}
try {
  await compose('down','--volumes','--remove-orphans'); await up();
  await run('Duplicate fixture, fresh state',[event,event,{...event,quantity:0}],expected);
  await run('Same fixture, retained state',[event,event],[]);
  await compose('restart','application'); await ready();
  await run('Same fixture after JVM restart',[event],[]);
  await run('Different event for same order',[{...event,event_id:'evt-2',quantity:1}],[{order_id:'order-101',total_cents:9000,unique_events:2}]);
  await compose('down','--volumes','--remove-orphans'); await up();
  await run('Same fixture after explicit reset',[event,event],expected);
  evidence.status='passed'; console.log('PASS actual Kafka Streams: duplicate suppression, retained state, JVM restart, accumulation and explicit reset.');
} catch(error) { evidence.status='failed'; evidence.error=error.stack; process.exitCode=1; }
finally {
  try { evidence.logs=await compose('logs','--no-color'); await compose('down','--volumes','--remove-orphans'); }
  catch(error) { evidence.cleanupError=error.message; process.exitCode=1; }
  await mkdir('.streamplay/evidence/kafka-streams',{recursive:true}); await writeFile('.streamplay/evidence/kafka-streams/results.json',JSON.stringify(evidence,null,2));
  if(evidence.error)console.error(evidence.error);
}
