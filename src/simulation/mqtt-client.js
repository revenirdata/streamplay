// SPDX-License-Identifier: Apache-2.0
import mqtt from 'mqtt';

export function assertTargetAllowed(value, { allowRemote = false, allowProduction = false } = {}) {
  const url = new URL(value);
  const local = ['127.0.0.1', 'localhost', '::1'].includes(url.hostname);
  if (!local && !allowRemote) throw new Error(`Refusing remote MQTT target ${url.hostname}; pass --allow-remote only for an isolated endpoint.`);
  if (/(^|[.-])prod(uction)?([.-]|$)/i.test(url.hostname) && !allowProduction) throw new Error(`Refusing production-like MQTT target ${url.hostname}; this simulator is for isolated environments.`);
  return url.href;
}

export function mqttClientFactory({ url, options }) {
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(url, options);
    const failed = error => { client.removeAllListeners(); client.end(true); reject(error); };
    client.once('error', failed);
    client.once('connect', () => {
      client.removeListener('error', failed);
      client.on('error', () => {});
      resolve({
        publish(topic, payload, publishOptions) { return client.publishAsync(topic, payload, publishOptions); },
        close() { return client.endAsync(); }
      });
    });
  });
}
