// SPDX-License-Identifier: Apache-2.0
import { resolve } from 'node:path';

export function configuration(env = process.env) {
  return {
    port: Number(env.STREAMPLAY_PORT ?? 4317),
    dataDir: resolve(env.STREAMPLAY_DATA_DIR ?? '.streamplay'),
    adapterModule: env.STREAMPLAY_ADAPTER_MODULE ? resolve(env.STREAMPLAY_ADAPTER_MODULE) : null,
    topologyFile: env.STREAMPLAY_TOPOLOGY_FILE ? resolve(env.STREAMPLAY_TOPOLOGY_FILE) : null,
    labModule: env.STREAMPLAY_LAB_MODULE ? resolve(env.STREAMPLAY_LAB_MODULE) : null,
    kafka: {
      brokers: (env.STREAMPLAY_KAFKA_BROKERS ?? 'localhost:19092').split(',').map(s => s.trim()),
      inputTopic: env.STREAMPLAY_INPUT_TOPIC ?? 'streamplay-orders-in',
      outputTopic: env.STREAMPLAY_OUTPUT_TOPIC ?? 'streamplay-orders-out'
    }
  };
}
