package com.example.realtime;

/**
 * 用户行为事件，对应 Kafka 消息体的一条记录。
 *
 * <h3>字段含义（与源数据集一一对应）</h3>
 * <ul>
 *   <li>{@code userId}     用户 ID（脱敏整数编号）</li>
 *   <li>{@code itemId}     商品 ID（脱敏整数编号）</li>
 *   <li>{@code categoryId} 商品所属类目 ID</li>
 *   <li>{@code behavior}   行为类型：pv 浏览 / cart 加购 / fav 收藏 / buy 购买</li>
 *   <li>{@code timestamp}  行为发生的秒级 UNIX 时间戳（东八区）</li>
 * </ul>
 *
 * <h3>为什么要写成这个形状</h3>
 * Flink 的 POJO 序列化有明确判定条件：类必须是 public、有 public 无参构造函数、
 * 所有字段要么 public 要么有标准 getter/setter。满足这些条件时 Flink 会用高效的
 * PojoSerializer（直接按字段读写，不写类型信息）；不满足则退化为 Kryo 通用序列化，
 * 序列化体积和开销都更大，跨版本兼容性也更差。
 *
 * <p>本类满足 POJO 条件，所以走的是 POJO 序列化。
 * 对比 {@link RealtimeAnalysisJob.WindowResult} 等结果类——它们只有 public 字段、
 * 没有 getter/setter，就不满足条件，会退化为 Kryo。
 *
 * <h3>关于 timestamp 的单位</h3>
 * 这里是<b>秒</b>级时间戳，和源数据保持一致，避免在解析层做无谓的数据改写。
 * 转成毫秒是在定义 Watermark 时做的（见 {@code withTimestampAssigner}），
 * 因为 Flink 内部时间语义统一用毫秒。
 */
public class UserBehavior {
    private long userId;
    private long itemId;
    private long categoryId;
    private String behavior;   // pv / cart / fav / buy
    private long timestamp;    // 事件时间（秒）

    /**
     * 无参构造。POJO 序列化在反序列化时需要它来创建实例，不能省略。
     */
    public UserBehavior() {
    }

    /**
     * 供解析层构造事件对象使用。
     *
     * @param userId     用户 ID
     * @param itemId     商品 ID
     * @param categoryId 类目 ID
     * @param behavior   行为类型（pv / cart / fav / buy）
     * @param timestamp  行为发生时间，秒级 UNIX 时间戳
     */
    public UserBehavior(long userId, long itemId, long categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    // ---- getter / setter：POJO 序列化的判定条件之一，同时也是作业里到处在用的访问入口 ----

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public long getItemId() { return itemId; }
    public void setItemId(long itemId) { this.itemId = itemId; }

    /** 商品所属类目，用于商品热度统计时顺带输出类目维度。 */
    public long getCategoryId() { return categoryId; }
    public void setCategoryId(long categoryId) { this.categoryId = categoryId; }

    /** 行为类型。窗口聚合时用它做 switch 分支，区分四类行为。 */
    public String getBehavior() { return behavior; }
    public void setBehavior(String behavior) { this.behavior = behavior; }

    /**
     * 事件时间（秒）。Watermark 的时间戳分配器读的就是这个字段，
     * 所以它是整个事件时间语义的源头——窗口怎么切完全取决于它。
     */
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
