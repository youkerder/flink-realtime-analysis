#!/bin/bash
# 天池排序数据集限速回放（3万条/秒，1亿条约55分钟）
python3 /mnt/d/daimaxiangmu/flink-realtime-analysis/producer/replay_producer.py \
    --file ~/userbehavior_sorted.csv --rate 30000 --bootstrap 127.0.0.1:9092 \
    --topic user_behavior_v2 > /tmp/final_replay.log 2>&1
