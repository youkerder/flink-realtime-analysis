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
            if (!behavior.matches("pv|cart|fav|buy") || ts <= 0) {
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
