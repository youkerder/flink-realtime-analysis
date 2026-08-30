package com.example.realtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 电商用户行为实时分析作业。
 *
 * 数据流：Kafka(user_behavior) -> 事件时间 1 分钟滚动窗口 -> ClickHouse 三张结果表
 *   1) rt.metrics_1min   每分钟 PV / UV / 四类行为计数
 *   2) rt.funnel_1min    每分钟 浏览 -> 加购/收藏 -> 购买 三级转化漏斗
 *   3) rt.item_topn_1min 每分钟商品热度 Top 100（TopN 比指标晚一个窗口闭合）
 *
 * 启动参数：--kafka kafka:9092 --clickhouse jdbc:clickhouse://clickhouse:8123/rt
 */
public class RealtimeAnalysisJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String kafkaBootstrap = params.get("kafka", "kafka:9092");
        String clickhouseUrl = params.get("clickhouse", "jdbc:clickhouse://clickhouse:8123/rt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        KafkaSource<UserBehavior> source = KafkaSource.<UserBehavior>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("user_behavior")
                .setGroupId("realtime-analysis")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new UserBehaviorDeserializer())
                .build();

        // 事件时间语义：允许 5 秒乱序，时间戳取事件秒级时间戳 * 1000；
        // withIdleness 防止分区数 < Source 并行度时空闲子任务卡住全局 watermark
        WatermarkStrategy<UserBehavior> watermarks = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getTimestamp() * 1000L)
                .withIdleness(Duration.ofSeconds(10));

        DataStream<UserBehavior> events = env.fromSource(source, watermarks, "kafka-source");

        // ---------- 分支一：每分钟指标 + 漏斗（遍历窗口内记录现算，窗口内数据量小，无需增量状态） ----------
        SingleOutputStreamOperator<WindowResult> metrics = events
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new MetricsWindowProcess())
                .name("metrics-1min");

        metrics.addSink(JdbcSink.sink(
                "INSERT INTO rt.metrics_1min (window_start, window_end, pv, uv, cart, fav, buy) VALUES (?,?,?,?,?,?,?)",
                (ps, r) -> {
                    ps.setTimestamp(1, Timestamp.from(Instant.ofEpochMilli(r.windowStart)));
                    ps.setTimestamp(2, Timestamp.from(Instant.ofEpochMilli(r.windowEnd)));
                    ps.setLong(3, r.pv);
                    ps.setLong(4, r.uv);
                    ps.setLong(5, r.cart);
                    ps.setLong(6, r.fav);
                    ps.setLong(7, r.buy);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(500)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickhouseUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()))
                .name("clickhouse-metrics");

        metrics.flatMap(new FunnelSplitter())
                .addSink(JdbcSink.sink(
                        "INSERT INTO rt.funnel_1min (window_start, stage, users) VALUES (?,?,?)",
                        (ps, r) -> {
                            ps.setTimestamp(1, Timestamp.from(Instant.ofEpochMilli(r.windowStart)));
                            ps.setString(2, r.stage);
                            ps.setLong(3, r.users);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(500)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(clickhouseUrl)
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()))
                .name("clickhouse-funnel");

        // ---------- 分支二：每分钟商品热度 TopN ----------
        SingleOutputStreamOperator<ItemCnt> counts = events
                .keyBy(e -> e.getItemId())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountAggregate(), new CountWindowProcess())
                .name("item-count-1min");

        SingleOutputStreamOperator<TopNRow> topn = counts
                .keyBy(c -> c.windowStart)
                .process(new TopNProcess(100))
                .name("topn-per-minute");

        topn.addSink(JdbcSink.sink(
                "INSERT INTO rt.item_topn_1min (window_start, item_id, category_id, cnt) VALUES (?,?,?,?)",
                (ps, r) -> {
                    ps.setTimestamp(1, Timestamp.from(Instant.ofEpochMilli(r.windowStart)));
                    ps.setLong(2, r.itemId);
                    ps.setLong(3, r.categoryId);
                    ps.setLong(4, r.cnt);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(500)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickhouseUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()))
                .name("clickhouse-topn");

        env.execute("realtime-user-behavior-analysis");
    }

    // ------------------------------------------------------------------
    // 每分钟指标 + 转化漏斗：直接遍历窗口内全部记录计算
    // ------------------------------------------------------------------
    public static class MetricsWindowProcess extends ProcessAllWindowFunction<UserBehavior, WindowResult, TimeWindow> {
        @Override
        public void process(Context ctx, Iterable<UserBehavior> records, Collector<WindowResult> out) {
            long pv = 0, cart = 0, fav = 0, buy = 0;
            Set<Long> users = new HashSet<>();
            Set<Long> pvUsers = new HashSet<>();
            Set<Long> cartFavUsers = new HashSet<>();
            Set<Long> buyUsers = new HashSet<>();
            for (UserBehavior e : records) {
                users.add(e.getUserId());
                switch (e.getBehavior()) {
                    case "pv":
                        pv++;
                        pvUsers.add(e.getUserId());
                        break;
                    case "cart":
                        cart++;
                        cartFavUsers.add(e.getUserId());
                        break;
                    case "fav":
                        fav++;
                        cartFavUsers.add(e.getUserId());
                        break;
                    case "buy":
                        buy++;
                        buyUsers.add(e.getUserId());
                        break;
                    default:
                        break;
                }
            }
            // 漏斗：第二层 = 浏览 且 (加购或收藏) 的用户数；第三层 = 浏览 且 购买 的用户数
            long stage2 = cartFavUsers.stream().filter(pvUsers::contains).count();
            long stage3 = buyUsers.stream().filter(pvUsers::contains).count();
            out.collect(new WindowResult(
                    ctx.window().getStart(), ctx.window().getEnd(),
                    pv, users.size(), cart, fav, buy,
                    pvUsers.size(), stage2, stage3));
        }
    }

    // ------------------------------------------------------------------
    // 商品计数（窗口关闭时产出 windowStart/End + 类目信息）
    // ------------------------------------------------------------------
    public static class CountAcc {
        public long cnt;
        public long categoryId;
    }

    public static class CountAggregate implements AggregateFunction<UserBehavior, CountAcc, CountAcc> {
        @Override
        public CountAcc createAccumulator() {
            return new CountAcc();
        }

        @Override
        public CountAcc add(UserBehavior e, CountAcc acc) {
            acc.cnt++;
            acc.categoryId = e.getCategoryId();
            return acc;
        }

        @Override
        public CountAcc getResult(CountAcc acc) {
            return acc;
        }

        @Override
        public CountAcc merge(CountAcc a, CountAcc b) {
            a.cnt += b.cnt;
            a.categoryId = b.categoryId;
            return a;
        }
    }

    public static class CountWindowProcess extends ProcessWindowFunction<CountAcc, ItemCnt, Long, TimeWindow> {
        @Override
        public void process(Long itemId, Context ctx, Iterable<CountAcc> input, Collector<ItemCnt> out) {
            CountAcc acc = input.iterator().next();
            out.collect(new ItemCnt(ctx.window().getStart(), ctx.window().getEnd(),
                    itemId, acc.categoryId, acc.cnt));
        }
    }

    // ------------------------------------------------------------------
    // TopN：按窗口收集计数，事件时间定时器触发时排序取前 N
    // ------------------------------------------------------------------
    public static class TopNProcess extends KeyedProcessFunction<Long, ItemCnt, TopNRow> {
        private final int topN;
        private transient MapState<Long, Long> cntState;  // itemId -> count
        private transient MapState<Long, Long> catState;  // itemId -> categoryId

        public TopNProcess(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            cntState = getRuntimeContext().getMapState(new MapStateDescriptor<>("itemCnt", Long.class, Long.class));
            catState = getRuntimeContext().getMapState(new MapStateDescriptor<>("itemCat", Long.class, Long.class));
        }

        @Override
        public void processElement(ItemCnt v, Context ctx, Collector<TopNRow> out) throws Exception {
            cntState.put(v.itemId, v.cnt);
            catState.put(v.itemId, v.categoryId);
            // 窗口结束（watermark 越过 windowEnd）时触发排序输出
            ctx.timerService().registerEventTimeTimer(v.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopNRow> out) throws Exception {
            List<long[]> all = new ArrayList<>();
            for (Map.Entry<Long, Long> e : cntState.entries()) {
                all.add(new long[]{e.getKey(), e.getValue()});
            }
            all.sort((a, b) -> Long.compare(b[1], a[1]));
            int n = Math.min(topN, all.size());
            for (int i = 0; i < n; i++) {
                long itemId = all.get(i)[0];
                out.collect(new TopNRow(ctx.getCurrentKey(), itemId, catState.get(itemId), all.get(i)[1]));
            }
            cntState.clear();
            catState.clear();
        }
    }

    // ------------------------------------------------------------------
    // 结果行类型（public 字段，Kryo 序列化；生产环境建议补 getter/setter 走 POJO 序列化）
    // ------------------------------------------------------------------
    public static class WindowResult {
        public long windowStart, windowEnd;
        public long pv, uv, cart, fav, buy;
        public long stage1, stage2, stage3;

        public WindowResult(long windowStart, long windowEnd, long pv, long uv,
                            long cart, long fav, long buy, long stage1, long stage2, long stage3) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.pv = pv;
            this.uv = uv;
            this.cart = cart;
            this.fav = fav;
            this.buy = buy;
            this.stage1 = stage1;
            this.stage2 = stage2;
            this.stage3 = stage3;
        }
    }

    public static class ItemCnt {
        public long windowStart, windowEnd, itemId, categoryId, cnt;

        public ItemCnt(long windowStart, long windowEnd, long itemId, long categoryId, long cnt) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.cnt = cnt;
        }
    }

    public static class TopNRow {
        public long windowStart, itemId, categoryId, cnt;

        public TopNRow(long windowStart, long itemId, long categoryId, long cnt) {
            this.windowStart = windowStart;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.cnt = cnt;
        }
    }

    public static class FunnelRow {
        public long windowStart;
        public String stage;
        public long users;

        public FunnelRow(long windowStart, String stage, long users) {
            this.windowStart = windowStart;
            this.stage = stage;
            this.users = users;
        }
    }

    public static class FunnelSplitter implements org.apache.flink.api.common.functions.FlatMapFunction<WindowResult, FunnelRow> {
        @Override
        public void flatMap(WindowResult r, Collector<FunnelRow> out) {
            out.collect(new FunnelRow(r.windowStart, "pv", r.stage1));
            out.collect(new FunnelRow(r.windowStart, "cart_fav", r.stage2));
            out.collect(new FunnelRow(r.windowStart, "buy", r.stage3));
        }
    }
}
