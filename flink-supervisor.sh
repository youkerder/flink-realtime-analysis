#!/bin/bash
# 基础设施守护：Kafka(9092) + Flink(8081) 失联自动重启
while true; do
  if ! (echo > /dev/tcp/127.0.0.1/9092) 2>/dev/null; then
    echo "$(date '+%H:%M:%S') Kafka 失联，自动重启" >> ~/flink-supervisor.log
    cd ~/kafkaRT/kafka_2.13-4.3.1 && bin/kafka-server-start.sh -daemon ~/kafkaRT/server.properties
    sleep 8
  fi
  if ! curl -s --max-time 3 http://localhost:8081/overview >/dev/null 2>&1; then
    echo "$(date '+%H:%M:%S') Flink 失联，自动重启" >> ~/flink-supervisor.log
    cd ~/flinkRT/flink-1.20.5 && bin/start-cluster.sh >/dev/null 2>&1
    sleep 10
  fi
  sleep 20
done
