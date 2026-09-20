// SPDX-License-Identifier: Apache-2.0
package com.revenir.streamplay;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;
import java.time.Duration;
import java.util.Properties;

/** Small stateful sandbox, not a general order-accounting implementation. */
public final class Orders {
  static final ObjectMapper JSON = new ObjectMapper();
  public static void main(String[] args) {
    Properties p = new Properties();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamplay-orders-v1");
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    p.put(StreamsConfig.STATE_DIR_CONFIG, "/state");
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    StreamsBuilder b = new StreamsBuilder();
    b.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("orders"), Serdes.String(), Serdes.String()));
    b.<String,String>stream("streamplay-orders-in", Consumed.with(Serdes.String(), Serdes.String()))
      .selectKey((key, value) -> { try { return JSON.readTree(value).path("order_id").asText(); } catch(Exception e) { throw new IllegalArgumentException(e); } })
      .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
      .transformValues(() -> new ValueTransformerWithKey<String,String,String>() {
        KeyValueStore<String,String> state;
        public void init(ProcessorContext context) { state = context.getStateStore("orders"); }
        public String transform(String key, String value) {
          try {
            var event = JSON.readTree(value);
            long quantity = event.path("quantity").asLong();
            if (quantity <= 0) return null;
            String id = "seen:" + JSON.writeValueAsString(new String[]{key, event.path("event_id").asText(value)});
            if (state.get(id) != null) return null;
            String totalKey = "total:" + key, countKey = "count:" + key;
            long total = (state.get(totalKey) == null ? 0 : Long.parseLong(state.get(totalKey))) + Math.multiplyExact(quantity, event.path("unit_price_cents").asLong());
            long count = (state.get(countKey) == null ? 0 : Long.parseLong(state.get(countKey))) + 1;
            state.put(id, "seen"); state.put(totalKey, Long.toString(total)); state.put(countKey, Long.toString(count));
            return JSON.createObjectNode().put("order_id", key).put("total_cents", total).put("unique_events", count).toString();
          } catch(Exception e) { throw new IllegalArgumentException("Invalid order event", e); }
        }
        public void close() {}
      }, "orders").filter((key, value) -> value != null).to("streamplay-orders-out", Produced.with(Serdes.String(), Serdes.String()));
    KafkaStreams streams = new KafkaStreams(b.build(), p);
    streams.setStateListener((next, old) -> { if (next == KafkaStreams.State.RUNNING) System.out.println("STREAMPLAY_READY Kafka Streams 3.9.1 application.id=streamplay-orders-v1 state=/state"); });
    Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(10))));
    streams.start();
  }
}
