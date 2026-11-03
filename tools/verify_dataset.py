#!/usr/bin/env python3
"""全量扫描 UserBehavior.csv，产出与 Flink 作业口径一致的校验统计。

用途：在不启动 Kafka/Flink 的前提下，先验证数据侧结论（总行数、脏时间戳、
行为分布、分钟窗口数），并可导出逐窗口 PV/UV 作为对账基准。
"""
import argparse
import csv
import sys
from collections import Counter

TS_MIN = 1511539200  # 2017-11-25 00:00:00 +08
TS_MAX = 1512336000  # 2017-12-04 00:00:00 +08
LAST_MIN = (1 << 63) - 1  # 事件时间窗口的"全局最大 watermark"哨兵


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--dump-windows", default="", help="可选：输出逐分钟 PV/行为计数 CSV")
    args = ap.parse_args()

    total = 0
    parsed = 0
    bad_rows = 0
    bad_behavior = 0
    ts_low = 0
    ts_high = 0
    ts_zero = 0
    beh = Counter()
    minutes = Counter()          # minute_ts -> pv count
    minute_pv = Counter()
    minute_buy = Counter()
    ts_min_seen = LAST_MIN
    ts_max_seen = -LAST_MIN
    sample_bad = []
    users_min = LAST_MIN
    users_max = 0

    with open(args.file, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            total += 1
            if len(row) < 5:
                bad_rows += 1
                continue
            try:
                u, i, c, b, t = int(row[0]), int(row[1]), int(row[2]), row[3], int(row[4])
            except ValueError:
                bad_rows += 1
                continue
            parsed += 1
            if b not in ("pv", "cart", "fav", "buy"):
                bad_behavior += 1
                continue
            if t == 0:
                ts_zero += 1
            if t < TS_MIN:
                ts_low += 1
                if len(sample_bad) < 5:
                    sample_bad.append((u, i, c, b, t))
                continue
            if t > TS_MAX:
                ts_high += 1
                if len(sample_bad) < 5:
                    sample_bad.append((u, i, c, b, t))
                continue

            beh[b] += 1
            m = t // 60 * 60
            minutes[m] += 1
            if b == "pv":
                minute_pv[m] += 1
            elif b == "buy":
                minute_buy[m] += 1
            if t < ts_min_seen:
                ts_min_seen = t
            if t > ts_max_seen:
                ts_max_seen = t
            if u < users_min:
                users_min = u
            if u > users_max:
                users_max = u
            if total % 10_000_000 == 0:
                print(f"  ... scanned {total:,}", file=sys.stderr, flush=True)

    clean = sum(beh.values())
    print("==== FULL DATASET VERIFICATION ====")
    print(f"file                : {args.file}")
    print(f"total_lines         : {total:,}")
    print(f"parsed_rows         : {parsed:,}")
    print(f"bad_rows(field)     : {bad_rows:,}")
    print(f"bad_behavior        : {bad_behavior:,}")
    print(f"ts_low(<2017-11-25) : {ts_low:,}")
    print(f"ts_high(>2017-12-04): {ts_high:,}")
    print(f"ts_zero             : {ts_zero:,}")
    print(f"kept_rows           : {clean:,}")
    print(f"drop_total          : {total - clean:,}")
    print(f"behaviors           : {dict(beh)}")
    print(f"ts_min_seen         : {ts_min_seen} ({__import__('datetime').datetime.fromtimestamp(ts_min_seen, __import__('datetime').timezone.utc)})")
    print(f"ts_max_seen         : {ts_max_seen} ({__import__('datetime').datetime.fromtimestamp(ts_max_seen, __import__('datetime').timezone.utc)})")
    print(f"user_id range       : {users_min:,} .. {users_max:,}")
    print(f"distinct_min_windows: {len(minutes):,}")
    print(f"expected_min_windows: {(ts_max_seen // 60 - ts_min_seen // 60 + 1):,}")
    print(f"window_span_min     : {ts_min_seen // 60 * 60} .. {ts_max_seen // 60 * 60}")
    print(f"sample_dropped      : {sample_bad}")
    if minutes:
        top = minutes.most_common(5)
        print(f"top_minutes_by_pv   : {[(m, c) for m, c in top]}")
        print(f"min_minute_count    : {min(minutes.values())}")

    if args.dump_windows:
        with open(args.dump_windows, "w", encoding="utf-8", newline="") as out:
            w = csv.writer(out)
            w.writerow(["window_start", "pv", "buy"])
            for m in sorted(minutes):
                w.writerow([m, minute_pv[m], minute_buy[m]])
        print(f"dumped_windows      : {args.dump_windows}")


if __name__ == "__main__":
    main()
