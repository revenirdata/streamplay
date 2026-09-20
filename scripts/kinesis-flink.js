// SPDX-License-Identifier: Apache-2.0
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { KinesisClient, CreateStreamCommand, DescribeStreamSummaryCommand } from '@aws-sdk/client-kinesis';
import { SQSClient, CreateQueueCommand } from '@aws-sdk/client-sqs';
const exec=promisify(execFile);
export const compose=async (...args)=>(await exec('docker',['compose','-p','streamplay-kinesis-flink','-f','examples/kinesis-flink-sqs/compose.yaml',...args],{windowsHide:true,timeout:600000,maxBuffer:8000000})).stdout;
export const connection={endpoint:'http://127.0.0.1:4568',region:'us-east-1',credentials:{accessKeyId:'test',secretAccessKey:'test'},maxAttempts:2};
export async function ready() {
  for(let i=0;i<120;i++) {
    const running=JSON.parse(await compose('ps','--format','json','application'));
    if(running.State!=='running')throw new Error('Flink application stopped: '+await compose('logs','--tail','40','application'));
    const {stdout:started}=await exec('docker',['inspect','--format','{{.State.StartedAt}}',running.ID],{windowsHide:true});
    if((await compose('logs','--since',started.trim(),'application')).includes('STREAMPLAY_READY'))return;
    await delay(1000);
  }
  throw new Error('Flink source readiness timed out');
}
export async function up() {
  await compose('build','application'); await compose('up','-d','--wait','localstack');
  const k=new KinesisClient(connection), q=new SQSClient(connection);
  try {
    try {await k.send(new CreateStreamCommand({StreamName:'streamplay-telemetry',ShardCount:1}));}catch(e){if(e.name!=='ResourceInUseException')throw e;}
    for(let i=0;i<60;i++){if((await k.send(new DescribeStreamSummaryCommand({StreamName:'streamplay-telemetry'}))).StreamDescriptionSummary.StreamStatus==='ACTIVE')break;await delay(500);}
    await q.send(new CreateQueueCommand({QueueName:'streamplay-events.fifo',Attributes:{FifoQueue:'true',ContentBasedDeduplication:'false'}}));
  }finally{k.destroy();q.destroy();}
  await compose('up','-d','application');await ready();
  console.log('Local Kinesis → actual Flink → SQS ready. Set STREAMPLAY_ADAPTER_MODULE=examples/adapters/kinesis-sqs.js and run npm start.');
}
if(process.argv[1]&&pathToFileURL(process.argv[1]).href===import.meta.url){
  try{if(process.argv[2]==='up')await up();else if(process.argv[2]==='down')await compose('down','--volumes','--remove-orphans');else throw new Error('Usage: node scripts/kinesis-flink.js up|down');}
  catch(e){console.error(e.message);process.exitCode=1;}
}
