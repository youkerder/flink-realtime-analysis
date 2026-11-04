# Flink Realtime Analysis · 电商用户行为实时分析平台

基于 **Kafka + Flink + ClickHouse + ECharts** 的端到端实时数据处理链路：行为事件流经 Kafka 接入，Flink 以事件时间语义完成窗口聚合，结果落 ClickHouse，前端大屏 5 秒轮询实时刷新。已使用**阿里天池淘宝用户行为公开数据集**（约 1 亿条真实脱敏行为记录）完成端到端实测。

## 实时大屏演示

<video src="docs/dashboard-demo.mp4" controls muted width="100%"></video>

> 录屏演示：ClickHouse 实时聚合 + ECharts 大屏（每 5 秒刷新）。若内嵌播放不可用，直接观看原始文件：[docs/dashboard-demo.mp4](docs/dashboard-demo.mp4)

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

单机（Windows 11 + WSL2 Ubuntu，Kafka/Flink 本机进程，ClickHouse 容器）全量回放**阿里天池淘宝用户行为公开数据集**的真实数据：

| 指标 | 数值 |
|------|------|
| 数据规模 | 100,150,807 条真实脱敏行为记录（2017-11-25 至 12-03，共 9 天，pv/cart/fav/buy 四类行为） |
| 数据质量处理 | 原始数据混有的脏时间戳记录（1902/2037 年）在 Flink 解析层经范围校验自动丢弃 |
| 聚合结果 | 流式侧覆盖数据集全量 9 天跨度共 12,960 个分钟窗口，无缺失；离线全量扫描按数据自然时间戳切分为 13,094 个窗口，两者相差 134 个，源于窗口边界对齐方式不同（预期内） |
| 指标校验 | 各窗口 PV/UV、四类行为计数与原始数据逐条对账一致，行为分布与官方统计吻合 |
| TopN 结果 | 每窗口商品热度 Top 100，覆盖全部窗口 |
| 作业状态 | 全程 RUNNING，0 失败任务 |

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

> ⚠️ **先排序再回放**：原始 CSV 的行序是按用户/随机打散的，而作业用
> `forBoundedOutOfOrderness(5s)` 的事件时间语义，直接回放会让 96% 的记录被判为迟到丢弃
> （实测保留率仅 3.94%）。回放前请先按事件时间排序：
>
> ```
> python tools/sort_dataset.py --file data/UserBehavior.csv --out data/UserBehavior.sorted.csv
> ```

新开一个窗口：

```
cd /d D:\projects\flink-realtime-analysis
wsl -e bash -c "cd /mnt/d/projects/flink-realtime-analysis/producer && python3 replay_producer.py --file ~/userbehavior_tianchi.csv --rate 0 --bootstrap 127.0.0.1:9092"
```

（`--rate 0` 为全速回放。注意：数据集单文件约 3.4 GB，**未随仓库提供**，
需按下方「示例数据集」一节的说明自行下载；若只是验证链路，也可用
`producer/generate_dataset.py` 生成同格式的模拟数据。）

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

### 形态 C：Windows 原生模式（无 Docker / 无 WSL）

适用场景：Windows 机器上 Docker Desktop 起不来（例如 WSL 被禁用），又需要在本机演示完整链路。
用 Kafka 原生进程 + Flink 本地模式 + 列存兼容层替代容器，**计算逻辑与大屏完全复用**，
仅把结果落地从 `JdbcSink`（JDBC）换成 `HttpResultSink`（ClickHouse HTTP + JSONEachRow）。

```
# 1) 依赖：JDK 17+、Maven、Python 3.10+（pip install "kafka-python>=3.0" duckdb）
# 2) 数据排序（只需一次，约 4 分钟）
python tools/sort_dataset.py --file data/UserBehavior.csv --out tools/UserBehavior.sorted.csv

# 3) 一键启动：Kafka → 兼容层+大屏(8090) → Flink 作业 → 回放 500 万条
python tools/native_up.py --reset --replay 5000000
```

- 大屏：<http://localhost:8090>（兼容层与静态页同端口，天然无跨域，不需要 Nginx）
- 写入统计：<http://localhost:8090/stats>
- 日志：`runtime/logs/job.log`、`runtime/logs/shim.log`
- 该模式保持前台运行，Ctrl+C 一次性停止全部组件；加 `--detach` 可后台运行

> 结果落地实现：`tools/ch_http_server.py` 用 DuckDB（列存）承载 ClickHouse 的 HTTP 接口子集
> （`?query=... FORMAT JSON`、`INSERT ... FORMAT JSONEachRow`、`TRUNCATE`），表结构与
> `db/init.sql` 对齐，并用窗口主键 + `INSERT OR REPLACE` 做幂等（等价 ReplacingMergeTree）。
> 详细定位过程、修复清单与验证数据见 [docs/本地运行修复说明.md](docs/本地运行修复说明.md)。


## 示例数据集

> ⚠️ **数据文件未随仓库提供**：`.gitignore` 已排除 `data/*.csv`（单文件约 3.4 GB，不适合入库），
> 克隆仓库后 `data/` 目录为空。

| 文件 | 用途 | 说明 |
|------|------|------|
| `data/UserBehavior.csv` | 全链路实测 | **阿里天池淘宝用户行为公开数据集**（约 1 亿条真实脱敏行为记录，2017-11-25 至 12-03） |
| `data/shopping.csv` | 关联规则示例 | 购物篮事务数据 |

**数据获取**：UserBehavior 数据集来自阿里天池公开数据集
<https://tianchi.aliyun.com/dataset/649>，下载后放到 `data/UserBehavior.csv` 即可。
若拉不到真实数据，`producer/generate_dataset.py` 可生成同格式的模拟数据，用于跑通链路。

> 数据集说明：UserBehavior 为阿里巴巴官方开源的真实脱敏数据（pv/cart/fav/buy 四类行为）。
> 原始数据中混有少量脏时间戳记录（如 1902/2037 年），已在 Flink 解析层通过时间戳范围校验自动丢弃，
> 校验逻辑见 `UserBehaviorDeserializer`。回放前建议将数据复制到 WSL 家目录以获得更快的读取速度：
> `wsl -e bash -c "cp /mnt/d/projects/flink-realtime-analysis/data/UserBehavior.csv ~/"`

`producer/generate_dataset.py` 仍保留，用于在没有真实数据集时生成同格式的模拟数据。

## 项目结构

```
├── README.md                     # 项目说明（架构 / 指标口径 / 运行方式）
├── .gitignore                    # 排除数据集、构建产物、运行日志
├── docker-compose.yml            # 形态 B：Kafka + Flink + ClickHouse + Nginx 全容器编排
├── start-all.cmd / stop-all.cmd  # 形态 A：Windows 侧一键启停（WSL 集群 + Docker 容器）
├── start-all.sh                  # 形态 A：WSL 内的自愈锚点（探活重启 Kafka/Flink，收到 stop_flag 退出）
├── stop-all.sh                   # WSL 内手工停止集群
├── pipeline_up.sh                # 幂等拉起：Kafka → Flink → 无运行作业时才提交
├── anchor.sh                     # 早期锚点脚本（已被 start-all.sh 取代）
├── flink-supervisor.sh           # 早期守护脚本（仅探活重启）
├── replay_tianchi.sh             # WSL 内全速回放天池数据
├── replay_final.sh               # 限速 3 万条/s 回放（注意 topic 写的是 user_behavior_v2）
├── pull-images.sh                # 多镜像源轮换拉取 Docker 镜像（国内网络）
├── retry_pulls.sh                # 拉镜像断点重试
├── flink-job/                    # Flink 作业（Java 11 字节码）
│   ├── pom.xml                   # 依赖与 shade 打包配置
│   └── src/main/java/com/example/realtime/
│       ├── RealtimeAnalysisJob.java      # 主作业：水位线 / 窗口聚合 / 漏斗 / TopN / Sink
│       ├── UserBehavior.java             # 事件 POJO（用户/商品/类目/行为/时间戳）
│       ├── UserBehaviorDeserializer.java # Kafka 消息解析 + 脏数据过滤
│       └── HttpResultSink.java           # ClickHouse HTTP 协议写入（无 Docker 环境用）
├── producer/
│   ├── replay_producer.py        # CSV → Kafka 限速回放器
│   └── generate_dataset.py       # 同格式模拟数据生成器（多进程）
├── dashboard/
│   ├── index.html                # ECharts 实时大屏（5s 轮询）
│   ├── echarts.min.js            # ECharts 5.6.0 本地库
│   └── nginx.conf                # 静态站点 + /clickhouse/ 反代（规避跨域）
├── db/
│   ├── init.sql                  # 建库 rt + 三张 MergeTree 结果表
│   └── noauth.xml                # ClickHouse 本地免密配置（仅演示）
├── tools/                        # 验证与本地运行工具
│   ├── native_up.py              # 无 Docker 一键启动（Kafka/兼容层/作业/回放）
│   ├── ch_http_server.py         # ClickHouse HTTP 兼容层 + 大屏静态服务（DuckDB）
│   ├── sort_dataset.py           # 按事件时间外部排序（回放前必做）
│   ├── verify_dataset.py         # 全量扫描数据集：行数/脏数据/行为分布/窗口数
│   ├── compare_windows.py        # 与离线口径逐窗口对账
│   ├── JobLogicVerifier.java     # 复用作业算子做全量逻辑校验
│   └── KafkaSourceVerifier.java  # 真实反序列化器消费 Kafka 的校验
├── docs/
│   ├── dashboard-demo.mp4        # 大屏运行录屏
│   ├── dashboard-live.png        # 实时大屏截图
│   ├── 本地运行修复说明.md        # 本机无法运行的定位与修复记录
│   ├── 运行验证与问题排查报告.md  # 全量验证结论 + 问题清单
│   └── 项目文件说明.md            # 每个文件的详细职责说明
└── data/                         # 数据集（不入库）
```

> 每个文件的**详细职责、所属链路环节、是否历史遗留**，见 [docs/项目文件说明.md](docs/项目文件说明.md)。

## 说明

- Kafka 采用 KRaft 模式；`docker-compose.yml` 中 JWT/密钥等均为演示配置
- 漏斗为「同一分钟窗口内」的转化语义，粒度极细故数值稀疏，属预期
- 本项目为学习用途，生产环境请使用多副本集群与外部化 Checkpoint 存储
