#!/bin/bash
# 天池数据集全速回放（由隐藏常驻 wsl.exe 持有，独立于任何终端会话）
python3 /mnt/d/daimaxiangmu/flink-realtime-analysis/producer/replay_producer.py \
    --file ~/userbehavior_sorted.csv --rate 0 --bootstrap 127.0.0.1:9092 \
    > /tmp/tianchi_replay.log 2>&1
