# Flink Realtime Analysis · 电商用户行为实时分析平台

基于 **Kafka + Flink + ClickHouse + ECharts** 的端到端实时数据处理链路：行为事件流经 Kafka 接入，Flink 以事件时间语义完成窗口聚合，结果落 ClickHouse，前端大屏 5 秒轮询实时刷新。已用 **1000 万条**模拟事件完成端到端实测。

## 架构

```mermaid
flowchart LR
    A[行为数据 CSV<br/>千万级] -->|Python 回放生产者| B[Kafka<br/>user_behavior topic]
    B -->|Flink Kafka Source| C[Flink 作业<br/>事件时间窗口聚合]
    C -->|JDBC Sink| D[(ClickHouse<br/>3 张结果表)]
    D -->|HTTP 查询| E[Nginx 反向代理]
    E -->|5s 轮询| F[ECharts 实时大屏]
```

- **前端**（`dashboard/`）：Vue 风格单页 + ECharts 5，Nginx 反代规避跨域
- **计算**（`flink-job/`）：Flink 1.20，Java DataStream API
- **存储**：ClickHouse MergeTree 三张结果表
- **运行形态**：
  - **形态 A（默认、已实测）**：Kafka 与 Flink 以 Apache 官方二进制运行于 WSL2 Ubuntu，ClickHouse 与大屏以 Docker 容器运行于 Windows，`start-all.cmd` 一键拉起
  - **形态 B（纯容器）**：`docker-compose.yml` 提供 bitnami/kafka + flink + clickhouse + nginx 全容器编排，适合 Docker Hub 可达的网络环境

## 功能指标

| 模块 | 计算逻辑 | 结果表 |
|------|---------|--------|
| 实时 PV/UV | 事件时间 1 分钟滚动窗口，窗口内去重 | `rt.metrics_1min` |
| 行为计数 | pv / cart / fav / buy 四类行为分窗口计数 | `rt.metrics_1min` |
| 转化漏斗 | 同一窗口内 浏览 → 加购/收藏 → 购买 三层用户去重交叉（同窗转化语义；如需会话级转化可放大窗口或改用 Flink CEP） | `rt.funnel_1min` |
| 商品热度 TopN | 按商品 keyBy 聚合计数，窗口关闭事件定时器触发排序取 Top 100 | `rt.item_topn_1min` |

## 技术要点（面试高频）

- **事件时间语义**：Watermark 采用 `forBoundedOutOfOrderness(5s)` 处理乱序；`withIdleness(10s)` 解决分区数小于 Source 并行度时空闲子任务卡死全局 Watermark 的经典问题（本项目实测踩坑并修复）
- **状态与容错**：Checkpoint EXACTLY_ONCE（60s 周期）、固定延迟重启策略；TopN 使用 Keyed MapState + 事件时间定时器，窗口关闭后清理
- **已知取舍**：TopN 比指标表晚一个窗口输出（需等下一个窗口的 watermark 推进才能拿到完整排序）；UV 用窗口内 HashSet 去重，生产规模应替换为 HLL/Bitmap；指标窗口直接遍历窗口内记录现算（每窗口数百条，无状态序列化负担）
- **脏数据容错**：Kafka 反序列化层对非法字段/格式错误的消息直接丢弃，不打挂作业

## 性能实测

单机（Windows 11 + WSL2 Ubuntu，Kafka/Flink 本机进程，ClickHouse 容器）回放 1000 万条模拟事件：

| 指标 | 数值 |
|------|------|
| 数据规模 | 10,000,000 条事件（30 天跨度，4 类行为 90/4/4/2 分布） |
| 回放耗时 | 639 秒（kafka-python 单进程 15,647 条/s，producer 侧上限） |
| 聚合结果 | 43,200 个窗口（30 天 × 1440 分钟，无一遗漏） |
| 指标校验 | PV 9,000,113 / 加购 400,397 / 收藏 399,094 / 购买 200,292，与模拟权重完全一致 |
| TopN 结果 | 4,320,000 行（43,200 窗口 × Top 100） |
| 作业状态 | 全程 RUNNING，0 失败任务，无积压 |

## 快速开始（形态 A，Windows + WSL2）

### 环境要求

- Windows 11 + WSL2 Ubuntu（Ubuntu 内需 JDK 21）
- Docker Desktop（ClickHouse 与大屏容器）
- Windows 侧 JDK 21 + Maven + Python 3.10+（`pip install kafka-python`）

### 一键启动

```
双击 start-all.cmd（保持窗口开启）
```

脚本依次完成：启动 Kafka（WSL）→ 启动 Flink 集群（WSL）→ 提交作业 → 拉起 ClickHouse 与大屏容器。

### 回放数据

新开一个窗口：

```
cd /d D:\daimaxiangmu\flink-realtime-analysis
wsl -e bash -c "cd /mnt/d/daimaxiangmu/flink-realtime-analysis/producer && python3 replay_producer.py --file ../data/user_behavior_10m.csv --rate 0 --bootstrap 127.0.0.1:9092"
```

（首次使用 `python generate_dataset.py --rows 10000000` 生成数据；`--rate 0` 为全速）

### 查看大屏

- 实时大屏：http://localhost:8090 （5 秒刷新）
- Flink UI：http://localhost:8081

### 停止

```
双击 stop-all.cmd
```

### 形态 B：全容器运行（备选）

```
docker compose up -d
docker exec jobmanager flink run -c com.example.realtime.RealtimeAnalysisJob \
    /opt/flink/usrlib/realtime-analysis-1.0.jar
```

需要本机可访问 Docker Hub（或自行配置镜像加速）。

## 示例数据集

| 文件 | 用途 | 说明 |
|------|------|------|
| `data/user_behavior_10m.csv` | 全链路实测 | 1000 万行 UserBehavior 格式模拟数据 |
| `data/shopping.csv` | 关联规则示例 | 购物篮事务数据 |

`producer/generate_dataset.py` 多进程生成，支持亿级扩展，行为分布按真实场景重尾建模。

## 项目结构

```
├── docker-compose.yml        # 形态 B：全容器编排（Kafka/Flink/ClickHouse/Nginx）
├── start-all.cmd             # 形态 A：一键启动（WSL Kafka+Flink + Docker ClickHouse/大屏）
├── stop-all.cmd              # 一键停止
├── flink-job/                # Flink 作业（Java 11 字节码）
│   └── src/main/java/com/example/realtime/
│       ├── RealtimeAnalysisJob.java      # 主作业：窗口聚合 / 漏斗 / TopN / JDBC Sink
│       ├── UserBehavior.java             # 事件 POJO
│       └── UserBehaviorDeserializer.java # Kafka 消息解析（脏数据容错）
├── producer/
│   ├── generate_dataset.py   # 多进程模拟数据生成器
│   └── replay_producer.py    # 限速回放进 Kafka
├── dashboard/                # ECharts 大屏 + Nginx 反代配置
├── db/                       # ClickHouse 建表与用户配置
├── data/                     # 数据文件（不入库）
└── docs/                     # 大屏截图
```

## 说明

- Kafka 采用 KRaft 模式；`docker-compose.yml` 中 JWT/密钥等均为演示配置
- 漏斗为「同一分钟窗口内」的转化语义，粒度极细故数值稀疏，属预期
- 本项目为学习用途，生产环境请使用多副本集群与外部化 Checkpoint 存储
