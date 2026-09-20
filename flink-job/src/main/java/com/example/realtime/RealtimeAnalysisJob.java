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
 * 电商用户行为实时分析作业（主入口）。
 *
 * <h3>整体数据流</h3>
 * <pre>
 *   Kafka(user_behavior)
 *        │  UserBehaviorDeserializer 解析 JSON、过滤脏数据
 *        ▼
 *   事件时间 1 分钟滚动窗口（TumblingEventTimeWindows）
 *        │
 *        ├─ 分支一：windowAll 全局窗口 → 每分钟 PV / UV / 四类行为计数 + 三级转化漏斗
 *        └─ 分支二：keyBy(itemId) 窗口 → 每分钟商品热度 Top 100
 *        │
 *        ▼
 *   ClickHouse 三张结果表（JDBC Sink 批量写入）
 * </pre>
 *
 * <h3>三张结果表</h3>
 * <ol>
 *   <li>{@code rt.metrics_1min}   每分钟 PV / UV / cart / fav / buy</li>
 *   <li>{@code rt.funnel_1min}    每分钟 浏览 → 加购/收藏 → 购买 三级转化漏斗</li>
 *   <li>{@code rt.item_topn_1min} 每分钟商品热度 Top 100</li>
 * </ol>
 *
 * <h3>启动参数</h3>
 * <pre>
 *   flink run -c com.example.realtime.RealtimeAnalysisJob realtime-analysis-1.0.jar \
 *        --kafka kafka:9092 --topic user_behavior \
 *        --clickhouse "jdbc:clickhouse://clickhouse:8123/rt"
 * </pre>
 *
 * <h3>已知取舍</h3>
 * <ul>
 *   <li>UV 用 {@link HashSet} 在窗口内精确去重，内存随单窗口用户数线性增长；
 *       数据量再上一个量级需换 HyperLogLog（约 0.8% 误差、内存固定）或 Roaring Bitmap。</li>
 *   <li>指标窗口是遍历窗口内全部记录现算的，不是增量聚合；单窗口数千条时开销可忽略，
 *       但数据量增大后应改为增量聚合。</li>
 *   <li>TopN 依赖"下一个窗口的 watermark 推进"才能闭合，因此比指标表晚一个窗口输出。</li>
 * </ul>
 */
public class RealtimeAnalysisJob {

    /**
     * 作业主入口：定义 Source、Watermark、两条计算分支与 Sink，然后提交执行。
     *
     * <p>注意 DataStream API 是惰性求值的：这里只是把算子串成一张执行图，
     * 真正开始计算是在最后一行 {@code env.execute()}。漏写 execute() 不会有任何报错，
     * 但作业什么都不会算——这种"静默不工作"是排查成本很高的一类问题。
     *
     * @param args 命令行参数，见类注释
     */
    public static void main(String[] args) throws Exception {
        // ParameterTool 把 --kafka xxx 这种命令行参数解析成键值对；
        // 第二个参数是默认值，便于本地直接跑不用带一长串参数。
        ParameterTool params = ParameterTool.fromArgs(args);
        String kafkaBootstrap = params.get("kafka", "kafka:9092");
        String topic = params.get("topic", "user_behavior");
        String clickhouseUrl = params.get("clickhouse", "jdbc:clickhouse://localhost:8124/rt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 每 60 秒一次，EXACTLY_ONCE 语义：定期把算子状态和 Kafka 消费位点一起快照下来，
        // 出故障可以恢复到一致的状态，不重不丢。
        // 注意 Exactly-Once 光靠 Checkpoint 是不够的，还需要 Kafka Source 提交位点
        // 与 Sink 写入行为配合（本项目 Sink 用的是幂等 INSERT，未做事务）。
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);

        // 重启策略：失败后固定延迟 10 秒重试，最多 3 次。
        // 这个参数与"脏数据怎么处理"直接相关：如果解析层遇到脏数据抛异常，
        // 作业就会走这里重启；脏数据持续进来时会不停重启直到超过次数而彻底失败。
        // 所以解析层最终选择了静默丢弃而不是抛异常（见 UserBehaviorDeserializer）。
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        // Kafka Source。earliest() 表示从最早位点开始消费，便于全量回放历史数据；
        // 生产环境通常用 latest() 或 committedOffsets()。
        KafkaSource<UserBehavior> source = KafkaSource.<UserBehavior>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(topic)
                .setGroupId("realtime-analysis")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new UserBehaviorDeserializer())
                .build();

        // 事件时间语义（Event Time）：按数据自带的时间戳划窗口，而不是按处理时间。
        //
        // 为什么必须用事件时间：流数据是无界的，多分区并行加上网络传输，乱序是必然的。
        // 若按处理时间（Processing Time）划窗口，同一条数据因到达时刻不同会被分进不同窗口，
        // 结果无法复现，也就没办法和离线全量统计做对账。
        //
        // forBoundedOutOfOrderness(5s)：允许最多 5 秒的乱序。这个参数两头都是代价——
        //   设大了结果更准但延迟更高；设小了延迟低但迟到的数据更容易被丢掉。
        //   5 秒是拍的一个经验值，没有做对比实验去调优。
        //
        // withIdleness(10s)：这是本项目实测踩到并修复的一个坑。
        //   Kafka 分区数小于 Source 并行度时，部分子任务分不到数据、一直空闲，
        //   而 Watermark 取的是所有并行子任务的最小值，空闲子任务不推进 Watermark，
        //   就会把全局 Watermark 卡死 → 窗口永远不触发 → 结果表一直是空的。
        //   更难受的是作业状态显示 RUNNING、日志里也没有任何报错，很难定位。
        //   withIdleness 让长时间没有数据的子任务被暂时排除在 Watermark 计算之外。
        WatermarkStrategy<UserBehavior> watermarks = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                // 事件里是秒级时间戳，Flink 内部用毫秒，这里乘 1000 转换
                .withTimestampAssigner((event, ts) -> event.getTimestamp() * 1000L)
                .withIdleness(Duration.ofSeconds(10));

        DataStream<UserBehavior> events = env.fromSource(source, watermarks, "kafka-source");

        // ================= 分支一：每分钟指标 + 转化漏斗 =================
        // 用 windowAll 而不是 keyBy：PV/UV 是全局指标，需要看到所有用户的行为，
        // 按用户分组算不出全局 UV。代价是这条分支并行度为 1。
        SingleOutputStreamOperator<WindowResult> metrics = events
                .windowAll(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new MetricsWindowProcess())
                .name("metrics-1min");

        // 指标结果写 ClickHouse。
        // 批量执行参数：攒够 1000 条或每 500 毫秒触发一次写入，最多重试 3 次。
        // 批量是为了减少往返开销；加时间上限是为了让数据不至于攒着不写、延迟过大。
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

        // 漏斗：把一行 WindowResult 拆成三行 FunnelRow（pv / cart_fav / buy）再写库，
        // 这样 ClickHouse 里是"长表"结构，前端画漏斗图时按 stage 取值即可。
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

        // ================= 分支二：每分钟商品热度 TopN =================
        // 先 keyBy(itemId) 聚合，把"每个商品在每个窗口内的行为次数"算出来。
        // 这里用 aggregate(AggregateFunction, ProcessWindowFunction) 的组合：
        //   - AggregateFunction 只维护一个累加器（计数 + 类目），增量计算，不用缓存窗口内全部记录；
        //   - ProcessWindowFunction 负责拿到窗口起止时间，补进输出结果里。
        SingleOutputStreamOperator<ItemCnt> counts = events
                .keyBy(e -> e.getItemId())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new CountAggregate(), new CountWindowProcess())
                .name("item-count-1min");

        // 再按窗口起点重新分组，用事件时间定时器在该窗口闭合时统一排序取前 100。
        // 分两步做（先按商品聚合、再按窗口分组）是因为：TopN 需要看到同一窗口内的所有商品，
        // 而窗口内商品数量远小于记录数，先聚合再排序能显著减少参与排序的数据量。
        // 结果表 item_topn_1min 的可见时机比 metrics_1min 晚一个窗口——需要等下一个窗口的
        // watermark 推进过来，当前窗口的定时器才会触发。
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

        // 提交执行图。没有这一行，前面所有算子都只是被"定义"，不会真正运行。
        env.execute("realtime-user-behavior-analysis");
    }

    // ==================================================================
    // 每分钟指标 + 转化漏斗
    // ==================================================================

    /**
     * 窗口函数：遍历一个 1 分钟窗口内的全部记录，算出 PV/UV、四类行为计数和三级漏斗。
     *
     * <p>为什么不用增量聚合：一分钟窗口内的数据量也就几千条，遍历一遍的开销可以忽略；
     * 而增量聚合需要为 UV、漏斗这些去重类指标维护一大堆 Keyed State，
     * 反而带来序列化开销和状态管理的麻烦。属于"数据量小的时候选简单写法"。
     *
     * <p>注意 {@link ProcessAllWindowFunction} 是全局窗口（并行度 1），不是 keyBy 之后的窗口。
     */
    public static class MetricsWindowProcess extends ProcessAllWindowFunction<UserBehavior, WindowResult, TimeWindow> {
        @Override
        public void process(Context ctx, Iterable<UserBehavior> records, Collector<WindowResult> out) {
            long pv = 0, cart = 0, fav = 0, buy = 0;
            Set<Long> users = new HashSet<>();       // 窗口内全部去重用户（UV）
            Set<Long> pvUsers = new HashSet<>();     // 有浏览行为的用户
            Set<Long> cartFavUsers = new HashSet<>();// 有加购或收藏行为的用户
            Set<Long> buyUsers = new HashSet<>();    // 有购买行为的用户

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
                        // 理论上解析层已经过滤过行为类型，这里兜底
                        break;
                }
            }

            // 漏斗口径：以"用户"为单位逐层去重交叉，不是简单的行为计数相减。
            //   第一层 stage1 = 有浏览的用户数
            //   第二层 stage2 = 既浏览过、又加购或收藏过的用户数（两个集合取交集）
            //   第三层 stage3 = 既浏览过、又购买过的用户数
            // 同一用户在一分钟内多次浏览只算一次，所以必须用集合而不是计数。
            // 另外这是"同一分钟窗口内"的转化语义，粒度很细，所以数值会比较稀疏，
            // 这是预期现象；要会话级转化需要放大窗口或改用 Flink CEP。
            long stage2 = cartFavUsers.stream().filter(pvUsers::contains).count();
            long stage3 = buyUsers.stream().filter(pvUsers::contains).count();

            out.collect(new WindowResult(
                    ctx.window().getStart(), ctx.window().getEnd(),
                    pv, users.size(), cart, fav, buy,
                    pvUsers.size(), stage2, stage3));
        }
    }

    // ==================================================================
    // 商品计数（分支二的第一个算子）
    // ==================================================================

    /** 商品计数的累加器：cnt 是行为次数，categoryId 顺带记下以便输出时补上类目。 */
    public static class CountAcc {
        public long cnt;
        public long categoryId;
    }

    /**
     * 增量聚合：每个商品每来一条记录只做 cnt++，不缓存原始记录。
     * 累加器状态由 Flink 托管，配合 Checkpoint 可恢复。
     */
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
            // 会话窗口/滑动窗口才会用到 merge，滚动窗口下不会被调用，这里按语义补上实现
            a.cnt += b.cnt;
            a.categoryId = b.categoryId;
            return a;
        }
    }

    /**
     * 把窗口起止时间补进聚合结果。
     * AggregateFunction 本身拿不到窗口元信息，所以需要用 ProcessWindowFunction 包一层。
     * 输入迭代器里只有一个元素（前面的聚合结果），取第一个即可。
     */
    public static class CountWindowProcess extends ProcessWindowFunction<CountAcc, ItemCnt, Long, TimeWindow> {
        @Override
        public void process(Long itemId, Context ctx, Iterable<CountAcc> input, Collector<ItemCnt> out) {
            CountAcc acc = input.iterator().next();
            out.collect(new ItemCnt(ctx.window().getStart(), ctx.window().getEnd(),
                    itemId, acc.categoryId, acc.cnt));
        }
    }

    // ==================================================================
    // TopN：按窗口收集计数，事件时间定时器触发排序
    // ==================================================================

    /**
     * 取每个窗口内行为次数最多的前 N 个商品。
     *
     * <p>实现要点：用两张 {@link MapState} 分别记"商品 → 次数"和"商品 → 类目"，
     * 每来一条记录就更新状态，并注册一个 {@code windowEnd + 1} 的事件时间定时器。
     * 当 Watermark 越过 windowEnd 时定时器触发，此时该窗口的数据已经收齐，可以做全局排序。
     *
     * <p>{@code +1} 是为了确保定时器严格晚于窗口边界——如果正好等于 windowEnd，
     * 有可能和窗口自身的闭合逻辑产生边界歧义。
     */
    public static class TopNProcess extends KeyedProcessFunction<Long, ItemCnt, TopNRow> {
        private final int topN;
        private transient MapState<Long, Long> cntState;  // itemId -> count
        private transient MapState<Long, Long> catState;  // itemId -> categoryId

        public TopNProcess(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            // MapState 必须在 open() 里通过 RuntimeContext 获取，不能在字段上直接 new
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
            // 把状态里的所有商品取出来，按次数倒序排，取前 topN
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

            // clear() 必须调用！否则 MapState 会随窗口数一直增长，
            // 长时间运行后表现为 Checkpoint 超时或直接 OOM。
            // 这一点很容易漏——本项目是看别人踩坑记录才知道的。
            // 清理完之后，定时器是一次性的（事件时间定时器触发后自动销毁），无需手动注销。
            cntState.clear();
            catState.clear();
        }
    }

    // ==================================================================
    // 结果行类型
    // ==================================================================
    // 说明：这些类只有 public 字段 + 构造函数，没有 getter/setter，
    // 因此不满足 Flink POJO 序列化的判定条件，会退化为 Kryo 通用序列化。
    // 功能上没问题，但序列化效率和跨版本兼容性都不如 POJO。
    // 生产环境建议补齐 getter/setter（保持公共无参构造）以启用 POJO 序列化。
    // ==================================================================

    /** 每分钟指标 + 漏斗结果（一个窗口一行）。 */
    public static class WindowResult {
        public long windowStart, windowEnd;
        public long pv, uv, cart, fav, buy;
        /** stage1=浏览用户数，stage2=浏览且加购/收藏用户数，stage3=浏览且购买用户数 */
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

    /** 单个商品在单个窗口内聚合后的行为次数（分支二的中间结果）。 */
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

    /** TopN 结果行（写 rt.item_topn_1min）。没有 windowEnd，因为前端按 window_start 查询即可。 */
    public static class TopNRow {
        public long windowStart, itemId, categoryId, cnt;

        public TopNRow(long windowStart, long itemId, long categoryId, long cnt) {
            this.windowStart = windowStart;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.cnt = cnt;
        }
    }

    /** 漏斗结果行（写 rt.funnel_1min），stage 取值为 pv / cart_fav / buy。 */
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

    /**
     * 把一行 {@link WindowResult} 拆成三行 {@link FunnelRow}，
     * 让 ClickHouse 里存成"窗口 × 层级"的长表，前端按 stage 直接取数画漏斗。
     */
    public static class FunnelSplitter implements org.apache.flink.api.common.functions.FlatMapFunction<WindowResult, FunnelRow> {
        @Override
        public void flatMap(WindowResult r, Collector<FunnelRow> out) {
            out.collect(new FunnelRow(r.windowStart, "pv", r.stage1));
            out.collect(new FunnelRow(r.windowStart, "cart_fav", r.stage2));
            out.collect(new FunnelRow(r.windowStart, "buy", r.stage3));
        }
    }
}
