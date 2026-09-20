// SPDX-License-Identifier: Apache-2.0
// Dedicated synthetic local pilot only. The application is supplied independently.
import { open, mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { KinesisClient, PutRecordCommand, DescribeStreamSummaryCommand } from '@aws-sdk/client-kinesis';
import { SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand, ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs';
import { record } from '../../src/scenario.js';
import { compose, ready, connection } from '../../scripts/kinesis-flink.js';

export default async function create({onOutput,onLog,signal}, env=process.env) {
  const stream=env.STREAMPLAY_KINESIS_STREAM??'streamplay-telemetry';
  const queueName=env.STREAMPLAY_SQS_QUEUE??'streamplay-events.fifo';
  // Source/queue bindings are independent, but this public pilot is intentionally scoped.
  if(stream!=='streamplay-telemetry'||queueName!=='streamplay-events.fifo')throw new Error('This pilot owns only streamplay-telemetry and streamplay-events.fifo. Supply your own trusted binding for other resources.');
  const dir=resolve('.streamplay/locks');await mkdir(dir,{recursive:true});
  let lock;try{lock=await open(resolve(dir,'kinesis-sqs.lock'),'wx');}catch(e){if(e.code==='EEXIST')throw new Error('An observer already owns this pilot queue. Stop it before running another capture.');throw e;}
  const k=new KinesisClient(connection),q=new SQSClient(connection);
  let closed=false,failure,loop,queueUrl;
  const check=()=>{signal?.throwIfAborted();if(failure)throw failure;};
  const close=async()=>{closed=true;await loop; k.destroy();q.destroy();await lock.close(); const {unlink}=await import('node:fs/promises');await unlink(resolve(dir,'kinesis-sqs.lock'));};
  try {
    await ready();
    await k.send(new DescribeStreamSummaryCommand({StreamName:stream}));
    const discovered=(await q.send(new GetQueueUrlCommand({QueueName:queueName}))).QueueUrl;
    queueUrl=connection.endpoint+new URL(discovered).pathname;
    const a=(await q.send(new GetQueueAttributesCommand({QueueUrl:queueUrl,AttributeNames:['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible']}))).Attributes;
    if(Number(a.ApproximateNumberOfMessages)||Number(a.ApproximateNumberOfMessagesNotVisible))throw new Error('Pilot output queue is not empty. Reset the isolated example before capturing; old messages will not be silently drained.');
    onLog((await compose('logs','--tail','8','application')).trim());
    loop=(async()=>{while(!closed){try{
      const result=await q.send(new ReceiveMessageCommand({QueueUrl:queueUrl,MaxNumberOfMessages:10,WaitTimeSeconds:1,VisibilityTimeout:10}),{abortSignal:AbortSignal.timeout(5000)});
      for(const message of result.Messages??[]){
        if(onOutput(record(message.Body,{transport:'sqs',queueUrl,messageId:message.MessageId,acknowledgement:'delete after accepted in-memory capture'}))===false)throw new Error('Output capture refused; message left unacknowledged');
        await q.send(new DeleteMessageCommand({QueueUrl:queueUrl,ReceiptHandle:message.ReceiptHandle}),{abortSignal:AbortSignal.timeout(5000)});
      }
    }catch(e){failure=e;return;}}})();
    return {metadata:{engine:'Apache Flink 1.20.2 synthetic telemetry job',input:{transport:'kinesis',stream,endpoint:connection.endpoint},output:{transport:'sqs',queueUrl},state:'retained until isolated job restart; use unique device IDs per scenario',readiness:'running container and source iterator ready; timeout/recovery outputs are positive controls',ownership:'one workbench observer under repository lock; external consumers are forbidden and cannot be detected reliably',acknowledgement:'capture then delete; in-memory capture is not crash-safe',timeoutMs:2000},
      async send(events,onSent){for(const value of events){check();if(!value||typeof value.device_id!=='string'||!value.device_id)throw new Error('Telemetry needs a device_id string');const raw=JSON.stringify(value);const result=await k.send(new PutRecordCommand({StreamName:stream,PartitionKey:value.device_id,Data:Buffer.from(raw)}),{abortSignal:AbortSignal.timeout(5000)});onSent(record(raw,{transport:'kinesis',stream,sequenceNumber:result.SequenceNumber,shardId:result.ShardId,acknowledgement:'Kinesis accepted input; not an engine receipt'}));}},
      check,close};
  }catch(e){await close();throw e;}
}
