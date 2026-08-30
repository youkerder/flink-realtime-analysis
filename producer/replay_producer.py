#!/usr/bin/env python3
"""把 CSV 行为数据按指定速率回放进 Kafka topic。

用法:
  python replay_producer.py --file ../data/user_behavior.csv --rate 50000
  python replay_producer.py --rate 0            # rate<=0 表示不限速全速回放
"""
import argparse
import csv
import json
import time

from kafka import KafkaProducer


def main():
    p = argparse.ArgumentParser(description="CSV -> Kafka 行为数据回放器")
    p.add_argument("--file", default="../data/user_behavior.csv")
    p.add_argument("--bootstrap", default="localhost:9094", help="宿主机访问 Kafka 的地址（映射到容器 9094）")
    p.add_argument("--topic", default="user_behavior")
    p.add_argument("--rate", type=int, default=50000, help="每秒回放条数；<=0 表示不限速")
    p.add_argument("--limit", type=int, default=0, help="最多回放条数，0=全部")
    args = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        batch_size=262144,
        acks=1,
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent, errors = 0, 0
    t0 = time.time()
    with open(args.file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            # 兼容无表头文件：首列不是数字的行（如表头）直接跳过
            if len(row) < 5 or not row[0].strip().isdigit():
                continue
            user_id, item_id, category_id, behavior, ts = row[0], row[1], row[2], row[3], row[4]
            try:
                producer.send(args.topic, key=user_id, value={
                    "user_id": int(user_id),
                    "item_id": int(item_id),
                    "category_id": int(category_id),
                    "behavior": behavior,
                    "timestamp": int(ts),
                })
            except Exception as e:  # noqa: BLE001
                errors += 1
                if errors <= 3:
                    print(f"发送失败: {e}")
            sent += 1
            if interval:
                lag = time.time() - t0 - sent * interval
                if lag < 0:
                    time.sleep(-lag)
            if args.limit and sent >= args.limit:
                break
    producer.flush()
    dt = time.time() - t0
    print(f"回放完成: 发送 {sent:,} 条 / 失败 {errors} 条 / 耗时 {dt:.1f}s（{sent / dt:,.0f} 条/s）")


if __name__ == "__main__":
    main()
