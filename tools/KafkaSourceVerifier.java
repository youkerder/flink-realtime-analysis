package com.example.realtime.verify;

import com.example.realtime.UserBehavior;
import com.example.realtime.UserBehaviorDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 校验作业真实的 KafkaSource + UserBehaviorDeserializer 链路（不落库，只统计）。
 *
 * 目的：验证生产端 JSON 字段名与解析层约定一致、脏数据过滤规则生效、
 * 消息能被完整消费（对比 topic 位点总数与算子收到条数）。
 */
public class KafkaSourceVerifier {

    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "127.0.0.1:9092";
        String topic = args.length > 1 ? args[1] : "user_behavior";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<UserBehavior> source = KafkaSource.<UserBehavior>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("verify-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setDeserializer(new UserBehaviorDeserializer())
                .build();

        WatermarkStrategy<UserBehavior> wm = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getTimestamp() * 1000L)
                .withIdleness(java.time.Duration.ofSeconds(10));

        DataStream<UserBehavior> events = env.fromSource(source, wm, "kafka-source");

        events.addSink(new CountingSink()).setParallelism(1);
        env.execute("kafka-source-verifier");
    }

    /** 统计落到算子里的记录数与时间范围，作业结束时打印。 */
    public static class CountingSink extends RichSinkFunction<UserBehavior> {
        private long count;
        private long minTs = Long.MAX_VALUE;
        private long maxTs = Long.MIN_VALUE;
        private long pv, cart, fav, buy, badBehavior;

        @Override
        public void open(Configuration parameters) {
            count = 0;
        }

        @Override
        public void invoke(UserBehavior e, Context context) {
            count++;
            minTs = Math.min(minTs, e.getTimestamp());
            maxTs = Math.max(maxTs, e.getTimestamp());
            switch (e.getBehavior()) {
                case "pv": pv++; break;
                case "cart": cart++; break;
                case "fav": fav++; break;
                case "buy": buy++; break;
                default: badBehavior++; break;
            }
        }

        @Override
        public void close() {
            System.out.println("EMITTED_RECORDS=" + count);
            System.out.println("TS_RANGE=" + minTs + ".." + maxTs);
            System.out.println("BEHAVIORS=pv=" + pv + ",cart=" + cart + ",fav=" + fav + ",buy=" + buy
                    + ",bad=" + badBehavior);
        }
    }
}
