-- 实时分析结果表（由 Flink 作业写入）
CREATE DATABASE IF NOT EXISTS rt;

-- 每分钟窗口的行为指标
CREATE TABLE IF NOT EXISTS rt.metrics_1min
(
    window_start DateTime,
    window_end   DateTime,
    pv           UInt64,
    uv           UInt64,
    cart         UInt64,
    fav          UInt64,
    buy          UInt64
) ENGINE = MergeTree ORDER BY window_start;

-- 每分钟窗口的商品热度 TopN 原始数据
CREATE TABLE IF NOT EXISTS rt.item_topn_1min
(
    window_start DateTime,
    item_id      UInt64,
    category_id  UInt64,
    cnt          UInt64
) ENGINE = MergeTree ORDER BY (window_start, cnt);

-- 浏览 → 加购/收藏 → 购买 转化漏斗（每分钟窗口）
CREATE TABLE IF NOT EXISTS rt.funnel_1min
(
    window_start DateTime,
    stage        LowCardinality(String),
    users        UInt64
) ENGINE = MergeTree ORDER BY (window_start, stage);
