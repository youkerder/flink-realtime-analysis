#!/bin/bash
# 基础设施守护锚点：保持 Kafka + Flink 存活；检测到 ~/stop_flag 则优雅停止并退出
rm -f ~/stop_flag
while true; do
  if [ -f ~/stop_flag ]; then
    echo "$(date '+%H:%M:%S') 收到停止标记，关闭集群" >> ~/flink-supervisor.log
    cd ~/flinkRT/flink-1.20.5 && bin/stop-cluster.sh >/dev/null 2>&1
    pkill -f 'kafka[.]Kafka' 2>/dev/null
    echo "$(date '+%H:%M:%S') 已全部停止，锚点退出" >> ~/flink-supervisor.log
    exit 0
  fi
  if ! (echo > /dev/tcp/127.0.0.1/9092) 2>/dev/null; then
    echo "$(date '+%H:%M:%S') Kafka 失联，自动重启" >> ~/flink-supervisor.log
    cd ~/kafkaRT/kafka_2.13-4.3.1 && bin/kafka-server-start.sh -daemon ~/kafkaRT/server.properties
    sleep 8
  fi
  if ! curl -s --max-time 3 http://localhost:8081/overview >/dev/null; then
    echo "$(date '+%H:%M:%S') Flink 失联，自动重启" >> ~/flink-supervisor.log
    cd ~/flinkRT/flink-1.20.5 && bin/start-cluster.sh >/dev/null 2>&1
    sleep 10
  fi
  cd ~/kafkaRT/kafka_2.13-4.3.1
  sleep 20
done
