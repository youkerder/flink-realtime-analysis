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
 * Kafka 消息 → {@link UserBehavior} 的反序列化器，同时承担"脏数据过滤"的职责。
 *
 * <h3>它做三件事</h3>
 * <ol>
 *   <li>把消息体字节按 UTF-8 解码，再用 Jackson 解析成 JSON 树；</li>
 *   <li>校验行为类型是否合法、时间戳是否落在数据集有效范围内；</li>
 *   <li>校验不通过或解析失败的记录<b>静默丢弃</b>（不 collect、不抛异常）。</li>
 * </ol>
 *
 * <h3>为什么是"丢弃"而不是"抛异常"</h3>
 * 这是本项目做过一次改动的设计决策，值得记一下。
 *
 * <p>最初的写法是遇到脏数据抛异常。实测发现这样不行：流作业抛异常会触发
 * {@code fixedDelayRestart} 重启策略（见 {@link RealtimeAnalysisJob}），
 * 而脏数据是持续存在于数据流里的——作业重启后继续消费，很快又碰到下一条脏数据、
 * 再次抛异常、再次重启。反复重试超过次数上限后作业直接彻底失败、再也起不来。
 *
 * <p>所以改成当前这种"丢弃脏数据、保全作业连续性"的做法。代价是脏数据没有留痕，
 * 事后无法追溯丢了哪些。生产环境的常规做法是把脏数据旁路输出到一个独立的
 * dead-letter topic 里，这次没有做。
 *
 * <h3>线程安全</h3>
 * {@code ObjectMapper} 声明为 {@code transient}，因为它是不可序列化的运行时对象，
 * 不能随算子一起被序列化分发到 TaskManager。用惰性初始化（{@link #mapper()}）保证
 * 每个算子实例各持有自己的一份，避免多线程共享。
 */
public class UserBehaviorDeserializer implements KafkaRecordDeserializationSchema<UserBehavior> {
    private static final long serialVersionUID = 1L;

    /**
     * 数据集官方有效时间范围：2017-11-25 00:00 ~ 2017-12-04 00:00（UTC+8），
     * 换算成秒级 UNIX 时间戳就是下面这两个值。
     *
     * <p>为什么要卡这个范围：全量扫描原始数据后发现混入了一批时间戳明显偏离
     * 2017 年 11-12 月的记录（例如 1902 年、2037 年，还有时间戳为 0 或负数的），
     * 属于数据采集环节的异常，不是真实业务事件。这些记录会直接污染事件时间窗口，
     * 所以必须在入口处拦掉。
     */
    private static final long TS_MIN = 1511539200L;
    private static final long TS_MAX = 1512336000L;

    /** 运行时对象，不参与序列化；用惰性初始化见 {@link #mapper()}。 */
    private transient ObjectMapper mapper;

    /**
     * 惰性获取 ObjectMapper。
     * ObjectMapper 构造有一定开销且是线程安全的，复用一份即可。
     */
    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public void open(InitializationContext context) {
        // 本反序列化器不需要在 open 阶段做初始化（ObjectMapper 走惰性初始化），留空实现
    }

    /**
     * 单条 Kafka 消息的反序列化 + 过滤逻辑。
     *
     * <p>过滤采用"双重校验"：行为类型必须是 pv/cart/fav/buy 四类之一，
     * 且时间戳必须落在官方有效范围内。两者任一不满足就丢弃。
     *
     * <p>整段逻辑包在 try-catch 里，是为了兜住字段缺失、类型不匹配、
     * JSON 格式损坏等一切解析异常——catch 块里什么都不做，等价于丢弃。
     *
     * @param record Kafka 记录（这里只用到 value）
     * @param out    合法记录的输出收集器；不调用 collect 就等于丢弃
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UserBehavior> out) {
        try {
            JsonNode n = mapper().readTree(new String(record.value(), StandardCharsets.UTF_8));

            // 用 path() 而不是 get()：字段缺失时 path() 返回 MissingNode，
            // asText("")/asLong(0) 会给出默认值而不会返回 null，省掉一层判空。
            String behavior = n.path("behavior").asText("");
            long ts = n.path("timestamp").asLong(0);

            if (!behavior.matches("pv|cart|fav|buy") || ts < TS_MIN || ts > TS_MAX) {
                // 行为类型非法，或时间戳越界 —— 丢弃这条记录
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

    /**
     * 告诉 Flink 这个反序列化器产出什么类型，
     * 便于 Flink 在构建执行图时做类型推断（避免退化成无类型的通用序列化）。
     */
    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
