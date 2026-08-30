# Flink Realtime Analysis · 电商用户行为实时分析平台

基于 **Kafka + Flink + ClickHouse + ECharts** 的端到端实时数据处理链路：行为事件流经 Kafka 接入，Flink 以事件时间语义完成窗口聚合，结果落 ClickHouse，前端大屏 5 秒轮询实时刷新。

## 架构

```mermaid
flowchart LR
    A[行为数据 CSV<br/>千万级] -->|Python 回放生产者| B[Kafka<br/>user_behavior topic]
    B -->|Flink Kafka Source| C[Flink 作业<br/>事件时间窗口聚合]
    C -->|JDBC Sink| D[(ClickHouse<br/>3 张结果表)]
    D -->|HTTP 查询| E[Nginx 反向代理]
    E -->|5s 轮询| F[ECharts 实时大屏]
```

## 功能指标

| 模块 | 计算逻辑 | 结果表 |
|------|---------|--------|
| 实时 PV/UV | 事件时间 1 分钟滚动窗口，增量聚合（AggregateFunction），UV 用窗口内 Set 去重 | `rt.metrics_1min` |
| 行为计数 | pv / cart / fav / buy 四类行为分窗口计数 | `rt.metrics_1min` |
| 转化漏斗 | 浏览 → 加购/收藏 → 购买 三层用户去重交叉 | `rt.funnel_1min` |
| 商品热度 TopN | 按商品 keyBy 聚合计数，窗口关闭事件定时器触发排序取 Top 100 | `rt.item_topn_1min` |

## 技术要点（面试高频）

- **事件时间语义**：Watermark 采用 `forBoundedOutOfOrderness(5s)` 处理乱序，时间戳取业务时间戳而非机器时间
- **状态与容错**：Checkpoint EXACTLY_ONCE 模式（60s 周期），固定延迟重启策略；TopN 状态使用 Keyed MapState，窗口关闭后清理
- **增量聚合**：窗口用 AggregateFunction 增量计算（而非缓存全量记录），窗口状态只保留聚合结果与去重用户集合
- **跨语言/跨组件集成**：Python 生产者 → Kafka → Flink → JDBC → ClickHouse，全链路 Docker Compose 一键编排
- **已知取舍**：TopN 比指标表晚一个窗口输出（需等下一个窗口的 watermark 推进才能拿到完整排序）；UV 用 HashSet 去重，生产环境大规模应替换为 HLL/Bitmap

## 性能实测

> 在消费级笔记本（容器限额）上回放 1000 万条事件的实测数据：

| 指标 | 数值 |
|------|------|
| 数据规模 | 10,000,000 条事件 |
| 回放速率 | {{REPLAY_RATE}} 条/s |
| 端到端延迟 | {{LATENCY}} 秒（事件时间窗口闭合 → 大屏可见） |
| 作业吞吐 | {{THROUGHPUT}} 条/s（TaskManager 4 slots） |

## 快速开始

### 环境要求

- Docker Desktop（含 docker compose）
- JDK 21+、Maven 3.6+（编译 Flink 作业）
- Python 3.10+，`pip install kafka-python`

### 1. 编译 Flink 作业

```bash
cd flink-job
mvn package          # 产物 target/realtime-analysis-1.0.jar
```

### 2. 启动集群（Kafka / Flink / ClickHouse / 大屏）

```bash
docker compose up -d
```

### 3. 提交 Flink 作业

```bash
docker exec jobmanager flink run -c com.example.realtime.RealtimeAnalysisJob \
    /opt/flink/usrlib/realtime-analysis-1.0.jar \
    --kafka kafka:9092 --clickhouse jdbc:clickhouse://clickhouse:8123/rt
```

Flink Web UI：http://localhost:8081

### 4. 生成数据并回放

```bash
cd producer
python generate_dataset.py --rows 10000000 --out ../data/user_behavior_10m.csv
python replay_producer.py --file ../data/user_behavior_10m.csv --rate 50000
```

`--rate` 为每秒回放条数，`0` 表示全速回放；数据按事件时间戳均匀分布在最近 30 天，
窗口会随回放推进逐分钟闭合。

### 5. 查看实时大屏

浏览器打开 http://localhost:8090 ，每 5 秒自动刷新：
PV/UV 趋势、行为构成、转化漏斗、商品热度 Top10。

## 项目结构

```
├── docker-compose.yml        # Kafka(KRaft) / Flink / ClickHouse / Nginx 编排
├── flink-job/                # Flink 作业（Java 11 字节码，兼容集群 JRE）
│   └── src/main/java/com/example/realtime/
│       ├── RealtimeAnalysisJob.java      # 主作业：窗口聚合 / 漏斗 / TopN / JDBC Sink
│       ├── UserBehavior.java             # 事件 POJO
│       └── UserBehaviorDeserializer.java # Kafka 消息解析（脏数据容错）
├── producer/
│   ├── generate_dataset.py   # UserBehavior 格式模拟数据生成器（多进程，亿级可扩展）
│   └── replay_producer.py    # 限速回放进 Kafka
├── dashboard/                # ECharts 大屏 + Nginx 反代配置
├── db/init.sql               # ClickHouse 建表脚本
└── data/                     # 数据文件（不入库）
```

## 说明

- 数据为模拟生成的 UserBehavior 格式（user_id, item_id, category_id, behavior, timestamp），
  行为分布按真实场景重尾建模（少数商品承载多数行为）
- JWT/密码等无；Kafka 采用 KRaft 单节点模式，生产环境应部署多副本集群
- 仅用于学习交流
