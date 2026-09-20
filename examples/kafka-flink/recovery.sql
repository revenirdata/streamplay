-- SPDX-License-Identifier: Apache-2.0
-- Tokens are replaced only with locally generated UUID-derived identifiers.
SET 'pipeline.name' = '__JOB__';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '1 s';
SET 'execution.checkpointing.timeout' = '30 s';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '1 s';

CREATE TABLE incoming (
  event_id STRING,
  source_id STRING,
  amount BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = '__INPUT__',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '__JOB__',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);
CREATE TABLE totals (
  source_id STRING,
  event_count BIGINT,
  total BIGINT,
  PRIMARY KEY (source_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = '__OUTPUT__',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json', 'value.format' = 'json'
);
INSERT INTO totals
SELECT source_id, COUNT(*), SUM(amount) FROM incoming GROUP BY source_id;
