// SPDX-License-Identifier: Apache-2.0
// Generic JSON MQTT boundaries. No device-specific authentication or payload conventions.
import { connectAsync } from 'mqtt';
import { randomUUID } from 'node:crypto';
import { record } from '../../src/scenario.js';

export default async function mqttAdapter({ onOutput, onLog, signal }, env = process.env) {
  const broker = env.STREAMPLAY_MQTT_URL;
  if (!broker) throw new Error('Set STREAMPLAY_MQTT_URL to an isolated test broker.');
  const url = new URL(broker);
  if (!['mqtt:', 'mqtts:', 'ws:', 'wss:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error('Use an MQTT URL without embedded credentials, query or fragment.');
  const input = env.STREAMPLAY_MQTT_INPUT_TOPIC, output = env.STREAMPLAY_MQTT_OUTPUT_TOPIC;
  if (![input,output].every(t=>typeof t==='string' && t.length>0 && t.length<=250 && !/[+#\u0000]/.test(t)) || input===output) throw new Error('Set distinct exact input/output topics without wildcards.');
  let client, failure, closed=false;
  async function bounded(operation, label) {
    let timer;
    try { return await Promise.race([operation,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(`${label} timed out`)),15000);})]); }
    finally {clearTimeout(timer);}
  }
  const check = () => {if(signal?.aborted)throw signal.reason ?? new Error('Cancelled');if(failure)throw failure;};
  const close = async () => {
    if(closed || !client)return;
    closed=true;
    try {await bounded(client.endAsync(Boolean(signal?.aborted || failure)),'MQTT close');}
    catch(error){client.end(true);throw error;}
  };
  try {
    client=await bounded(connectAsync(url.href,{clientId:`streamplay-${randomUUID()}`,clean:true,reconnectPeriod:0,connectTimeout:10000,
      username:env.STREAMPLAY_MQTT_USERNAME,password:env.STREAMPLAY_MQTT_PASSWORD}), 'MQTT connect');
    client.on('error',error=>{failure=error;});
    client.on('close',()=>{if(!closed)failure=new Error('MQTT disconnected during capture');});
    client.on('message',(topic,payload,packet)=>{
      if(closed || topic!==output)return;
      if(packet.retain){onLog('Ignored retained output predating this subscription.');return;}
      onOutput(record(payload.toString('utf8'),{transport:'mqtt',topic,qos:packet.qos,duplicate:packet.dup,retained:packet.retain}));
    });
    await bounded(client.subscribeAsync(output,{qos:1}),'MQTT subscribe');
    check();
    return {
      metadata:{engine:'External application (MQTT boundaries)',broker:url.href,inputTopic:input,outputTopic:output,
        state:'external application state retained; no reset',isolation:'dedicated sandbox topics required; output is not causally correlated'},
      async send(events,onSent){
        for(const value of events){check();const raw=JSON.stringify(value);await bounded(client.publishAsync(input,raw,{qos:1,retain:false}),'MQTT publish');
          onSent(record(raw,{transport:'mqtt',topic:input,qos:1,acknowledgement:'broker PUBACK; application processing not implied'}));}
      },check,close,
    };
  } catch(error){await close();throw error;}
}
