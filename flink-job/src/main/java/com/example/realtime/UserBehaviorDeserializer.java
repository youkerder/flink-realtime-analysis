package com.example.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Kafka 消息 -> UserBehavior。
 * 手动解析 snake_case 字段；行为类型非法或时间戳缺失的消息跳过不输出，
 * 保证脏数据不会打挂作业。
 */
public class UserBehaviorDeserializer implements KafkaRecordDeserializationSchema<UserBehavior> {
    private static final long serialVersionUID = 1L;
    // 数据集官方有效时间范围：2017-11-25 00:00 ~ 2017-12-04 00:00（UTC+8），
    // 原始数据中混有 1902/2037 年等脏时间戳记录，超出范围的直接丢弃
    private static final long TS_MIN = 1511539200L;
    private static final long TS_MAX = 1512336000L;
    private transient ObjectMapper mapper;

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public void open(InitializationContext context) {
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UserBehavior> out) {
        try {
            JsonNode n = mapper().readTree(new String(record.value(), StandardCharsets.UTF_8));
            String behavior = n.path("behavior").asText("");
            long ts = n.path("timestamp").asLong(0);
            if (!behavior.matches("pv|cart|fav|buy") || ts < TS_MIN || ts > TS_MAX) {
                return;
            }
            out.collect(new UserBehavior(
                    n.path("user_id").asLong(),
                    n.path("item_id").asLong(),
                    n.path("category_id").asLong(),
                    behavior,
                    ts));
        } catch (Exception e) {
            // 解析失败的消息直接丢弃，避免坏数据导致作业反复重启
        }
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
