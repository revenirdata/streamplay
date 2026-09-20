// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { createConnection } from 'node:net';
import { connectAsync } from 'mqtt';
import { randomUUID } from 'node:crypto';
import mqttAdapter from '../examples/adapters/mqtt.js';
import { buildExperiment } from '../src/experiment.js';
import { runScenario } from '../src/runner.js';

// The integration is intentionally locked to a dedicated loopback test broker.
const broker='mqtt://127.0.0.1:18884',prefix=`streamplay-test-${randomUUID()}`;
const input=prefix+'/in',output=prefix+'/out';
// Container creation returns before its listener is ready. Bound readiness independently.
await new Promise((resolve, reject) => {
  const deadline = Date.now() + 15000;
  function attempt() {
    const socket = createConnection({ host: '127.0.0.1', port: 18884 });
    socket.setTimeout(1000, () => socket.destroy(new Error('Broker readiness timed out')));
    socket.once('connect', () => { socket.destroy(); resolve(); });
    socket.once('error', error => {
      if (Date.now() >= deadline) reject(error);
      else setTimeout(attempt, 150);
    });
  }
  attempt();
});
const app=await connectAsync(broker,{reconnectPeriod:0,connectTimeout:5000});
app.on('message',(topic,raw)=>{const event=JSON.parse(raw.toString());if(event.value>0)void app.publishAsync(output,JSON.stringify({device_id:event.device_id,doubled:event.value*2}),{qos:1});});
await app.subscribeAsync(input,{qos:1});
await app.publishAsync(output,JSON.stringify({stale:true}),{qos:1,retain:true});
const scenario=buildExperiment({name:'MQTT multi-device phases',adapter:'local',observeMs:500,template:{device_id:'template',value:0},deviceIds:['sensor-a','sensor-b'],
  phases:[{name:'active',kind:'emit',durationMs:200,intervalMs:100,value:2},{name:'silence',kind:'silence',durationMs:100},{name:'zero',kind:'emit',durationMs:100,intervalMs:100,value:0}],
  expected:[{device_id:'sensor-a',doubled:4},{device_id:'sensor-b',doubled:4},{device_id:'sensor-a',doubled:4},{device_id:'sensor-b',doubled:4}]});
const options={adapters:{local:callbacks=>mqttAdapter(callbacks,{STREAMPLAY_MQTT_URL:broker,STREAMPLAY_MQTT_INPUT_TOPIC:input,STREAMPLAY_MQTT_OUTPUT_TOPIC:output})}};
try {
  for(let i=0;i<2;i++){
    const run=await runScenario(scenario,options);
    assert.equal(run.status,'passed',run.error ?? JSON.stringify(run.assertion));
    assert.equal(run.inputs.length,6);assert.equal(run.outputs.length,4);
    assert.ok(run.inputs.every(r=>r.acknowledgement.includes('PUBACK')));
    assert.ok(run.logs.some(l=>l.includes('retained')));
  }
  const mismatch=await runScenario({...scenario,expected:[]},options);
  assert.equal(mismatch.status,'failed');
  console.log('PASS: two MQTT application runs, two identities, silence/zero phases, retained-message exclusion, PUBACK evidence and deliberate assertion failure.');
} finally {await app.publishAsync(output,'',{qos:1,retain:true});await app.endAsync();}
