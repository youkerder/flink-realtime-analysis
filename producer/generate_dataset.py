#!/usr/bin/env python3
"""生成 UserBehavior 格式的模拟用户行为数据（可扩展到亿级）。

字段: user_id,item_id,category_id,behavior,timestamp
behavior ∈ {pv, cart, fav, buy}，符合真实场景的重尾分布：
少数商品和用户贡献大多数行为。时间戳按行递增，覆盖最近 --days 天。

用法:
  python generate_dataset.py --rows 10000000 --out ../data/user_behavior.csv
"""
import argparse
import os
import random
import time
from multiprocessing import Pool

BEHAVIORS = ("pv", "cart", "fav", "buy")
WEIGHTS = (0.90, 0.04, 0.04, 0.02)


def gen_chunk(job):
    """生成一个数据分片并写入分片文件，返回 (分片号, 行数)。"""
    idx, rows, users, items, categories, t_start, t_end, seed, part_dir = job
    rng = random.Random(seed)
    brng = random.Random(seed ^ 0x9E3779B9)
    step = (t_end - t_start) / max(rows, 1)
    t = float(t_start)
    buf = []
    with open(os.path.join(part_dir, f"part_{idx:03d}.csv"), "w", encoding="utf-8") as f:
        for _ in range(rows):
            u = int(users * (rng.random() ** 1.5)) + 100000
            it = int(items * (rng.random() ** 2.0)) + 1000000
            c = int(categories * (rng.random() ** 2.0)) + 1
            b = brng.choices(BEHAVIORS, WEIGHTS)[0]
            buf.append(f"{u},{it},{c},{b},{int(t)}")
            t += step
            if len(buf) >= 100_000:
                f.write("\n".join(buf) + "\n")
                buf.clear()
        if buf:
            f.write("\n".join(buf) + "\n")
    return idx, rows


def main():
    p = argparse.ArgumentParser(description="UserBehavior 格式模拟数据生成器")
    p.add_argument("--rows", type=int, default=10_000_000, help="总行数（默认 1000 万）")
    p.add_argument("--users", type=int, default=100_000, help="用户数规模")
    p.add_argument("--items", type=int, default=500_000, help="商品数规模")
    p.add_argument("--categories", type=int, default=10_000, help="类目数规模")
    p.add_argument("--days", type=int, default=30, help="时间跨度（天）")
    p.add_argument("--out", default="../data/user_behavior.csv", help="输出文件")
    p.add_argument("--workers", type=int, default=os.cpu_count(), help="并行进程数")
    args = p.parse_args()

    out_path = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    part_dir = out_path + ".parts"
    os.makedirs(part_dir, exist_ok=True)

    workers = max(1, args.workers)
    chunk_rows = -(-args.rows // workers)  # 向上取整
    jobs = []
    now = int(time.time())
    t_start = now - args.days * 86400
    t_end = now
    remaining = args.rows
    for i in range(workers):
        rows = min(chunk_rows, remaining)
        if rows <= 0:
            break
        # 每个分片的时间片互不重叠，保证整体时间递增
        t0 = t_start + (t_end - t_start) * (sum(chunk_rows for _ in range(i)) / args.rows)
        t1 = t_start + (t_end - t_start) * (min((i + 1) * chunk_rows, args.rows) / args.rows)
        jobs.append((i, rows, args.users, args.items, args.categories, int(t0), int(t1) - 1, now + i, part_dir))
        remaining -= rows

    print(f"生成 {args.rows:,} 行 -> {out_path}（{workers} 进程并行）")
    t0 = time.time()
    with Pool(workers) as pool:
        done = sum(n for _, n in pool.imap_unordered(gen_chunk, jobs))
    print(f"分片生成完成: {done:,} 行，耗时 {time.time() - t0:.1f}s，正在合并...")

    with open(out_path, "w", encoding="utf-8", newline="") as out:
        out.write("user_id,item_id,category_id,behavior,timestamp\n")
        for i in range(len(jobs)):
            part = os.path.join(part_dir, f"part_{i:03d}.csv")
            with open(part, "r", encoding="utf-8") as pf:
                while True:
                    block = pf.read(1 << 20)
                    if not block:
                        break
                    out.write(block)
            os.remove(part)
    os.rmdir(part_dir)
    size_mb = os.path.getsize(out_path) / 1e6
    print(f"完成: {out_path}（{size_mb:,.0f} MB，耗时 {time.time() - t0:.1f}s）")


if __name__ == "__main__":
    main()
