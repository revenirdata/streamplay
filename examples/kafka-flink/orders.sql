-- SPDX-License-Identifier: Apache-2.0
SET 'pipeline.name' = 'streamplay-orders';
SET 'parallelism.default' = '1';

CREATE TABLE orders_in (
  order_id STRING,
  quantity INT,
  unit_price_cents INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'streamplay-orders-in',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'streamplay-flink-example',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE orders_out (
  order_id STRING,
  total_cents INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'streamplay-orders-out',
  'properties.bootstrap.servers' = 'kafka:9092',
  'sink.delivery-guarantee' = 'at-least-once',
  'format' = 'json'
);

-- Duplicate inputs intentionally remain duplicate outputs.
INSERT INTO orders_out
SELECT order_id, quantity * unit_price_cents
FROM orders_in
WHERE quantity > 0;
