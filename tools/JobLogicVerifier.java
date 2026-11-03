package com.example.realtime.verify;

import com.example.realtime.RealtimeAnalysisJob;
import com.example.realtime.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.Duration;

/**
 * 本地逻辑校验器（不属于交付链路，仅用于验证作业算子在全量数据集上的正确性）。
 *
 * 对比真实作业的差异只有两处：
 *   1. Source 由 Kafka 换成 CSV 文本（解析规则与 UserBehaviorDeserializer 完全一致）；
 *   2. Sink 由 ClickHouse JDBC 换成 CSV 文件。
 * 中间的时间戳提取、Watermark、窗口、聚合、TopN 算子全部直接复用 RealtimeAnalysisJob 的内部类，
 * 因此这里跑出的数字就是作业在 Kafka 上会算出的数字（前提是同一条数据被消费一次）。
 */
public class JobLogicVerifier {

    private static final long TS_MIN = 1511539200L;
    private static final long TS_MAX = 1512336000L;

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String outPrefix = args[1];
        int parallelism = args.length > 2 ? Integer.parseInt(args[2]) : 4;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().disableClosureCleaner();

        DataStream<String> lines = env.readTextFile(input).setParallelism(1);

        DataStream<UserBehavior> events = lines
                .map(line -> {
                    String[] p = line.split(",");
                    if (p.length < 5) {
                        return null;
                    }
                    try {
                        return new UserBehavior(Long.parseLong(p[0].trim()), Long.parseLong(p[1].trim()),
                                Long.parseLong(p[2].trim()), p[3].trim(), Long.parseLong(p[4].trim()));
                    } catch (NumberFormatException e) {
                        return null;
                    }
                })
                .filter(e -> e != null)
                // 与 UserBehaviorDeserializer 的过滤口径保持一致
                .filter(e -> ("pv".equals(e.getBehavior()) || "cart".equals(e.getBehavior())
                        || "fav".equals(e.getBehavior()) || "buy".equals(e.getBehavior()))
                        && e.getTimestamp() >= TS_MIN && e.getTimestamp() <= TS_MAX);

        WatermarkStrategy<UserBehavior> watermarks = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getTimestamp() * 1000L)
                .withIdleness(Duration.ofSeconds(10));

        DataStream<UserBehavior> withTs = events.assignTimestampsAndWatermarks(watermarks);

        SingleOutputStreamOperator<RealtimeAnalysisJob.WindowResult> metrics = withTs
                .windowAll(org.apache.flink.streaming.api.windowing.assigners
                        .TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new RealtimeAnalysisJob.MetricsWindowProcess())
                .name("metrics-1min");

        metrics.map(r -> r.windowStart + "," + r.windowEnd + "," + r.pv + "," + r.uv + ","
                        + r.cart + "," + r.fav + "," + r.buy + "," + r.stage1 + "," + r.stage2 + "," + r.stage3)
                .addSink(new LineSink(outPrefix + "_metrics.csv"))
                .setParallelism(1);

        SingleOutputStreamOperator<RealtimeAnalysisJob.ItemCnt> counts = withTs
                .keyBy(e -> e.getItemId())
                .window(org.apache.flink.streaming.api.windowing.assigners
                        .TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new RealtimeAnalysisJob.CountAggregate(), new RealtimeAnalysisJob.CountWindowProcess())
                .name("item-count-1min");

        counts.keyBy(c -> c.windowStart)
                .process(new RealtimeAnalysisJob.TopNProcess(100))
                .name("topn-per-minute")
                .map(r -> r.windowStart + "," + r.itemId + "," + r.categoryId + "," + r.cnt)
                .addSink(new LineSink(outPrefix + "_topn.csv"))
                .setParallelism(1);

        env.execute("job-logic-verifier");
    }

    /** 单并行度写文件，便于后续逐窗口对账。 */
    public static class LineSink extends RichSinkFunction<String> {
        private final String path;
        private transient BufferedWriter writer;

        public LineSink(String path) {
            this.path = path;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            writer = new BufferedWriter(new FileWriter(path, false), 1 << 20);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            writer.write(value);
            writer.newLine();
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }
}
