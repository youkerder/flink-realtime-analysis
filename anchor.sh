#!/bin/bash
# WSL 常驻锚点：启动 Flink 集群并提交作业，随后永驻（由 Windows 侧隐藏 wsl.exe 持有）
cd ~/flinkRT/flink-1.20.5
bin/start-cluster.sh
sleep 8
bin/flink run -c com.example.realtime.RealtimeAnalysisJob \
    /mnt/d/daimaxiangmu/flink-realtime-analysis/flink-job/target/realtime-analysis-1.0.jar \
    --kafka 127.0.0.1:9092 \
    --clickhouse 'jdbc:clickhouse://172.31.192.1:8124/rt' \
    > /tmp/anchor_submit.log 2>&1
sleep infinity
