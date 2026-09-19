// SPDX-License-Identifier: Apache-2.0
import { KinesisClient, PutRecordCommand } from '@aws-sdk/client-kinesis';
import { localEndpoint } from '../../src/lab/queues.js';

export default async function create() {
  const endpoint = localEndpoint(process.env.STREAMPLAY_LOCALSTACK_ENDPOINT);
  const stream = process.env.STREAMPLAY_KINESIS_STREAM;
  const queues = JSON.parse(process.env.STREAMPLAY_SQS_QUEUES ?? '[]');
  if (!stream || !Array.isArray(queues) || !queues.length || queues.length > 8 || queues.some(url => new URL(url).origin !== endpoint)) throw new Error('Configure a sandbox input stream and 1–8 SQS queue URLs on the local endpoint.');
  const client = new KinesisClient({ endpoint, region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' }, maxAttempts: 2 });
  return {
    name: 'Local Kinesis / SQS application', namespace: `${endpoint}/${stream}`, queueEndpoint: endpoint, queueUrls: queues,
    metadata: { inputStream: stream, outputQueues: queues, endpoint, engine: 'External application; configure and start it separately' },
    async publish(event) {
      const response = await client.send(new PutRecordCommand({ StreamName: stream, PartitionKey: String(event.device_id ?? event.event_id ?? 'streamplay'), Data: Buffer.from(JSON.stringify(event)) }), { abortSignal: AbortSignal.timeout(10000) });
      return { meaning: 'Kinesis accepted input; downstream processing not implied', sequenceNumber: response.SequenceNumber, shardId: response.ShardId };
    },
    close: () => client.destroy(),
    topology: { version: 1, adapter: 'local', nodes: [
      { id: 'devices', kind: 'source', label: 'Emulated devices', observe: 'inputs', identityPath: 'device_id' },
      { id: 'input', kind: 'stream', label: stream, observe: 'inputs' },
      { id: 'app', kind: 'processor', label: 'Your stream processor', observe: 'none' },
      { id: 'output', kind: 'queue', label: 'Sandbox SQS outputs', observe: 'outputs' }
    ], edges: [{ from: 'devices', to: 'input', observe: 'inputs' }, { from: 'input', to: 'app' }, { from: 'app', to: 'output', observe: 'outputs' }] }
  };
}
