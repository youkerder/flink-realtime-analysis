#!/bin/bash
# 一键拉起 pipeline：Kafka（未启动则拉起）→ Flink（未启动则拉起）→ 提交作业（无运行中作业才提交）
cd ~/kafkaRT/kafka_2.13-4.3.1
if ! (echo > /dev/tcp/127.0.0.1/9092) 2>/dev/null; then
  echo "$(date '+%H:%M:%S') 启动 Kafka"
  bin/kafka-server-start.sh -daemon ~/kafkaRT/server.properties
  sleep 8
fi

cd ~/flinkRT/flink-1.20.5
if ! curl -s --max-time 3 http://localhost:8081/overview >/dev/null; then
  echo "$(date '+%H:%M:%S') 启动 Flink 集群"
  bin/start-cluster.sh
  sleep 10
fi

RUNNING=$(curl -s --max-time 5 http://localhost:8081/jobs/overview | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for j in d['jobs'] if j.get('state')=='RUNNING'))" 2>/dev/null || echo 0)
if [ "${RUNNING:-0}" -eq 0 ]; then
  echo "$(date '+%H:%M:%S') 提交 Flink 作业"
  nohup bin/flink run -c com.example.realtime.RealtimeAnalysisJob \
      /mnt/d/daimaxiangmu/flink-realtime-analysis/flink-job/target/realtime-analysis-1.0.jar \
      --kafka 127.0.0.1:9092 --clickhouse 'jdbc:clickhouse://172.31.192.1:8124/rt' \
      > /tmp/submit.log 2>&1 &
  sleep 30
fi

curl -s --max-time 5 http://localhost:8081/jobs/overview > /tmp/pipeline_state.json 2>/dev/null
echo "PIPELINE_UP_DONE" > /tmp/pipeline_state
cat /tmp/pipeline_state.json
