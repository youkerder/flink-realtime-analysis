#!/bin/bash
# Stop Flink cluster and Kafka
cd ~/flinkRT/flink-1.20.5
bin/stop-cluster.sh 2>/dev/null
pkill -f 'kafka[.]Kafka' 2>/dev/null
echo "WSL side stopped."
