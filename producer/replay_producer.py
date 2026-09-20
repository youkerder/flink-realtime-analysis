#!/usr/bin/env python3
"""把 CSV 行为数据按指定速率回放进 Kafka topic。

用法:
  python replay_producer.py --file ../data/user_behavior.csv --rate 50000
  python replay_producer.py --rate 0            # rate<=0 表示不限速全速回放

设计要点（详见各函数内注释）:
  1. 限速用「补偿式睡眠」而不是逐条 sleep，长时间运行平均速率能对上目标值；
  2. 每条记录以 user_id 作为 Kafka key，保证同一用户的行为落到同一分区、分区内有序；
  3. 生产端开了 linger/batch 以提升批量吞吐。
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

    # 生产端参数：
    #   linger_ms=50       —— 发送前最多等 50ms 攒批，牺牲一点延迟换吞吐
    #   batch_size=262144  —— 单批最大 256KB，配合 linger 提升吞吐
    #   acks=1             —— leader 写入即返回，不等待全部副本。回放场景追求吞吐，
    #                         且数据可重放，不需要 acks=all 的强保证
    #   key_serializer     —— key 用 user_id，保证同一用户进同一分区（分区内有序）
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        batch_size=262144,
        acks=1,
    )

    # 每条记录之间的理论间隔；rate<=0 时为 0，表示不做限速
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent, errors = 0, 0
    t0 = time.time()

    with open(args.file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            # 兼容无表头文件：首列不是数字的行（如表头）直接跳过。
            # 数据集本身没有表头，但这样写顺带能容错字段数不足的坏行。
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
                # 单条发送失败不中断整体回放，只计数并打印前几条，避免刷屏
                errors += 1
                if errors <= 3:
                    print(f"发送失败: {e}")

            sent += 1

            # ---- 限速：补偿式睡眠 ----
            # 思路：算出「按目标速率到此刻应该已经花掉的时间」= sent * interval，
            # 再减去「实际已经花掉的时间」= time.time() - t0，得到落后的量 lag。
            # 只有 lag < 0（也就是实际跑得比目标快、落后了）才睡 -lag 这么久。
            #
            # 这样写的好处是能自动吸收 GC、系统调度带来的抖动：
            # 某一秒因为 GC 慢了，后面就会少睡一点把进度追回来，长时间平均速率贴合目标值。
            #
            # 如果简单地对每条记录 sleep(interval)，循环本身（JSON 序列化、send 调用）
            # 也有开销，累加起来实际速率会明显低于目标值。
            if interval:
                lag = time.time() - t0 - sent * interval
                if lag < 0:
                    time.sleep(-lag)

            # 提前结束（便于抽样回放少量数据快速验证链路）
            if args.limit and sent >= args.limit:
                break

    # flush 会把还在 linger 缓冲里的数据全部发出去，不调用会丢尾部数据
    producer.flush()
    dt = time.time() - t0
    print(f"回放完成: 发送 {sent:,} 条 / 失败 {errors} 条 / 耗时 {dt:.1f}s（{sent / dt:,.0f} 条/s）")


if __name__ == "__main__":
    main()
