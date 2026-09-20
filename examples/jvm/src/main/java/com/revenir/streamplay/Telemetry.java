// SPDX-License-Identifier: Apache-2.0
package com.revenir.streamplay;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;

/** Synthetic single-shard local fixture; intentionally not a production Kinesis connector. */
public final class Telemetry {
  static final ObjectMapper JSON = new ObjectMapper();
  static final String STREAM = "streamplay-telemetry";
  static StaticCredentialsProvider credentials() { return StaticCredentialsProvider.create(AwsBasicCredentials.create("test","test")); }
  public static void main(String[] args) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.addSource(new Input()).name("local-kinesis")
      .keyBy(value -> { try { return JSON.readTree(value).get("device_id").asText(); } catch(Exception e) { throw new IllegalArgumentException(e); } })
      .process(new Timeout()).name("per-source-timeout")
      .addSink(new Output()).name("local-sqs");
    env.execute("streamplay-synthetic-telemetry");
  }
  public static class Input extends RichParallelSourceFunction<String> {
    private volatile boolean active = true;
    public void run(SourceContext<String> context) throws Exception {
      try (var client=KinesisClient.builder().endpointOverride(URI.create("http://localstack:4566")).region(Region.US_EAST_1).credentialsProvider(credentials()).build()) {
        String shard=client.describeStream(r->r.streamName(STREAM)).streamDescription().shards().get(0).shardId();
        String iterator=client.getShardIterator(r->r.streamName(STREAM).shardId(shard).shardIteratorType(ShardIteratorType.LATEST)).shardIterator();
        System.out.println("STREAMPLAY_READY Flink 1.20.2 source=LATEST timeoutMs=2000 state=fresh-on-job-start");
        while(active) {
          var records=client.getRecords(GetRecordsRequest.builder().shardIterator(iterator).limit(100).build());
          iterator=records.nextShardIterator();
          for(var record:records.records()) synchronized(context.getCheckpointLock()) { context.collect(record.data().asUtf8String()); }
          Thread.sleep(100);
        }
      }
    }
    public void cancel() { active=false; }
  }
  public static class Timeout extends KeyedProcessFunction<String,String,String> {
    private transient ValueState<Long> timer;
    private transient ValueState<Boolean> offline;
    public void open(Configuration config) {
      timer=getRuntimeContext().getState(new ValueStateDescriptor<>("timer",Long.class));
      offline=getRuntimeContext().getState(new ValueStateDescriptor<>("offline",Boolean.class));
    }
    private String event(String device, String status) {
      return JSON.createObjectNode().put("device_id",device).put("status",status).put("event_id",UUID.randomUUID().toString()).put("occurred_at",Instant.now().toString()).toString();
    }
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
      if(timer.value()==null) out.collect(event(ctx.getCurrentKey(),"received"));
      else if(Boolean.TRUE.equals(offline.value())) out.collect(event(ctx.getCurrentKey(),"recovered"));
      else ctx.timerService().deleteProcessingTimeTimer(timer.value());
      offline.update(false);
      long deadline=ctx.timerService().currentProcessingTime()+2000;
      timer.update(deadline); ctx.timerService().registerProcessingTimeTimer(deadline);
    }
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
      if(timer.value()!=null && timestamp==timer.value() && !Boolean.TRUE.equals(offline.value())) {
        offline.update(true); out.collect(event(ctx.getCurrentKey(),"timeout"));
      }
    }
  }
  public static class Output extends RichSinkFunction<String> {
    private transient SqsClient client;
    private transient String queueUrl;
    public void open(Configuration config) {
      client=SqsClient.builder().endpointOverride(URI.create("http://localstack:4566")).region(Region.US_EAST_1).credentialsProvider(credentials()).build();
      String discovered=client.getQueueUrl(r->r.queueName("streamplay-events.fifo")).queueUrl();
      queueUrl="http://localstack:4566"+URI.create(discovered).getPath();
    }
    public void invoke(String value, Context context) throws Exception {
      var event=JSON.readTree(value);
      client.sendMessage(r->r.queueUrl(queueUrl).messageBody(value).messageGroupId(event.get("device_id").asText()).messageDeduplicationId(event.get("event_id").asText()));
    }
    public void close() { if(client!=null)client.close(); }
  }
}
