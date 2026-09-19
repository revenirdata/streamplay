// SPDX-License-Identifier: Apache-2.0
import { SQSClient, GetQueueAttributesCommand, ReceiveMessageCommand, DeleteMessageCommand,
  CreateQueueCommand, DeleteQueueCommand, SetQueueAttributesCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';

export function localEndpoint(value) {
  const url = new URL(value);
  if (url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) || url.username || url.password || url.search || url.hash || url.pathname !== '/') throw new Error('Operational queue tools require an explicit loopback LocalStack endpoint.');
  return url.origin;
}
export function sqsSandbox(endpoint) {
  endpoint = localEndpoint(endpoint);
  const client = new SQSClient({ endpoint, region: 'us-east-1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' }, maxAttempts: 2 });
  const send = command => client.send(command, { abortSignal: AbortSignal.timeout(10000) });
  const scoped = url => { const parsed = new URL(url); if (parsed.origin !== endpoint) throw new Error('Queue URL must use the configured local endpoint.'); return url; };
  return {
    endpoint,
    attributes: async url => (await send(new GetQueueAttributesCommand({ QueueUrl: scoped(url), AttributeNames: ['All'] }))).Attributes,
    receive: async (url, visibility = 2) => (await send(new ReceiveMessageCommand({ QueueUrl: scoped(url), MaxNumberOfMessages: 1, WaitTimeSeconds: 0, VisibilityTimeout: visibility, MessageSystemAttributeNames: ['All'] }))).Messages ?? [],
    remove: (url, receipt) => send(new DeleteMessageCommand({ QueueUrl: scoped(url), ReceiptHandle: receipt })),
    create: async name => (await send(new CreateQueueCommand({ QueueName: name, Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1' } }))).QueueUrl,
    destroy: url => send(new DeleteQueueCommand({ QueueUrl: scoped(url) })),
    redrive: (url, arn) => send(new SetQueueAttributesCommand({ QueueUrl: scoped(url), Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: '2' }) } })),
    publish: (url, body, group = 'test') => send(new SendMessageCommand({ QueueUrl: scoped(url), MessageBody: JSON.stringify(body), MessageGroupId: group, MessageDeduplicationId: randomUUID() })),
    close: () => client.destroy()
  };
}

export function createQueueInspector(api, urls, onOutput) {
  let paused = true, polling = false, running = null, lastError;
  const previous = new Map(), deleted = new Map();
  for (const url of urls) { if (new URL(url).origin !== api.endpoint) throw new Error('Queue URL must use the local endpoint.'); }
  async function snapshot() {
    const queues = [];
    for (const url of urls) {
      const attrs = await api.attributes(url);
      const read = key => attrs[key] === undefined ? null : Number(attrs[key]);
      const visible = read('ApproximateNumberOfMessages');
      const old = previous.get(url), now = Date.now(), removed = deleted.get(url) ?? 0;
      const pendingSince = visible > 0 ? old?.pendingSince ?? now : null;
      const warnings = [];
      if (!attrs.RedrivePolicy) warnings.push('No dead-letter queue configured.');
      if (old?.visible !== null && visible > old?.visible) warnings.push('Visible backlog increased since the previous observation.');
      if (pendingSince && now - pendingSince >= 10000 && removed === old?.deleted) warnings.push('Backlog observed for at least 10 seconds with no inspector deletions. Other consumers are not measured.');
      previous.set(url, { visible, pendingSince, deleted: removed });
      queues.push({ url, visible, inFlight: read('ApproximateNumberOfMessagesNotVisible'), delayed: read('ApproximateNumberOfMessagesDelayed'), approximate: true,
        oldestMessageAgeSeconds: null, oldestAgeReason: 'Not supplied by GetQueueAttributes; CloudWatch observation required.', retentionSeconds: read('MessageRetentionPeriod'),
        visibilityTimeoutSeconds: read('VisibilityTimeout'), redrivePolicy: attrs.RedrivePolicy ? JSON.parse(attrs.RedrivePolicy) : null, inspectorDeleted: removed, warnings });
    }
    return { paused, polling, queues, error: lastError ?? null, meaning: 'Queue backlog is not a Flink backpressure measurement. Resuming this inspector consumes and deletes sandbox messages.' };
  }
  async function poll() {
    if (paused || polling) return;
    polling = true;
    running = (async () => {
      for (const url of urls) {
        if (paused) break;
        for (const message of await api.receive(url)) {
          await onOutput(message, url);
          await api.remove(url, message.ReceiptHandle);
          deleted.set(url, (deleted.get(url) ?? 0) + 1);
        }
      }
      lastError = null;
    })();
    try { await running; } catch (error) { lastError = error.message; paused = true; }
    finally { polling = false; running = null; }
  }
  const timer = setInterval(() => void poll(), 250); timer.unref();
  return { snapshot, resume() { paused = false; }, async pause() { paused = true; await running?.catch(() => {}); }, async close() { clearInterval(timer); await this.pause(); } };
}

// Unique disposable queues, never a production/output queue. A failure is retained in the report.
export async function verifyQueueRecovery(api) {
  const report = { id: randomUUID(), startedAt: new Date().toISOString(), status: 'running', checks: [], scope: 'Real local SQS API behavior; no real notification provider and no Flink throughput claim.' };
  let main, dead;
  const check = (name, pass, evidence) => { report.checks.push({ name, passed: Boolean(pass), evidence }); if (!pass) throw new Error(name); };
  const until = async fn => { const deadline = Date.now() + 15000; while (Date.now() < deadline) { const value = await fn(); if (value) return value; await delay(200); } throw new Error('Queue condition did not converge within 15 seconds.'); };
  try {
    main = await api.create(`sp-check-${report.id}.fifo`); dead = await api.create(`sp-dead-${report.id}.fifo`);
    check('Detect missing DLQ', !(await api.attributes(main)).RedrivePolicy, { queue: main });
    await api.redrive(main, (await api.attributes(dead)).QueueArn);
    await api.publish(main, { id: 'poison', run: report.id });
    const backlog = await until(async () => Number((await api.attributes(main)).ApproximateNumberOfMessages) > 0);
    check('Backlog persists without a consumer', backlog, { consumer: 'absent' });
    const first = await until(async () => (await api.receive(main, 1))[0]);
    const second = await until(async () => (await api.receive(main, 1))[0]);
    check('Failed processing is redelivered', second.MessageId === first.MessageId && Number(second.Attributes.ApproximateReceiveCount) >= 2, { first: first.Attributes, second: second.Attributes });
    const preserved = await until(async () => { await api.receive(main, 1); return (await api.receive(dead, 1))[0]; });
    check('Poison payload reaches DLQ', preserved.Body === first.Body, { body: preserved.Body, attributes: preserved.Attributes });
    await api.publish(main, { id: 'healthy', run: report.id });
    const healthy = await until(async () => (await api.receive(main, 1))[0]);
    check('Same FIFO group resumes after poison message', JSON.parse(healthy.Body).id === 'healthy', { body: healthy.Body });
    await api.remove(main, healthy.ReceiptHandle);
    await until(async () => { const a = await api.attributes(main); return Number(a.ApproximateNumberOfMessages) === 0 && Number(a.ApproximateNumberOfMessagesNotVisible) === 0; });
    check('Successful processing drains queue', true, await api.attributes(main)); report.status = 'passed';
  } catch (error) { report.status = 'failed'; report.error = error.message; }
  finally {
    report.cleanup = [];
    for (const url of [main, dead].filter(Boolean)) {
      try { await api.destroy(url); report.cleanup.push({ url, deleted: true }); }
      catch (error) { report.cleanup.push({ url, deleted: false, error: error.message }); report.status = 'failed'; }
    }
    report.finishedAt = new Date().toISOString();
  }
  return report;
}
