#!/usr/bin/env python3
"""把 UserBehavior 数据集按事件时间戳做外部归并排序。

为什么需要：Flink 作业用事件时间 + forBoundedOutOfOrderness(5s)，
乱序容忍度只有 5 秒。而天池原始 CSV 的行序是按用户/随机打散的（同一分钟的数据
散落在整份文件里），直接回放会让 Watermark 瞬间推进到接近最大时间戳，
之后到达的记录全部被判为迟到并丢弃（实测仅约 3% 记录能进入窗口）。

用法：
  python tools/sort_dataset.py --file data/UserBehavior.csv --out tools/UserBehavior.sorted.csv
"""
import argparse
import os
import heapq
import tempfile


def sort_chunk(lines):
    lines.sort(key=lambda l: int(l.rsplit(",", 1)[1]))
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--chunk-rows", type=int, default=4_000_000)
    args = ap.parse_args()

    tmpdir = tempfile.mkdtemp(prefix="ub_sort_")
    runs = []
    chunk = []
    total = 0
    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or not s[0].isdigit():
                continue
            chunk.append(s)
            if len(chunk) >= args.chunk_rows:
                total += len(chunk)
                runs.append(_spill(chunk, tmpdir, len(runs)))
                print(f"  spilled run {len(runs)} total={total:,}", flush=True)
                chunk = []
    if chunk:
        total += len(chunk)
        runs.append(_spill(chunk, tmpdir, len(runs)))

    with open(args.out, "w", encoding="utf-8", newline="") as out:
        files = [open(p, "r", encoding="utf-8") for p in runs]
        try:
            for line in heapq.merge(*files, key=lambda l: int(l.rsplit(",", 1)[1])):
                out.write(line)
        finally:
            for f in files:
                f.close()
    for p in runs:
        os.remove(p)
    os.rmdir(tmpdir)
    print(f"done: {args.out} rows={total:,}")


def _spill(chunk, tmpdir, idx):
    chunk.sort(key=lambda l: int(l.rsplit(",", 1)[1]))
    p = os.path.join(tmpdir, f"run_{idx:04d}.txt")
    with open(p, "w", encoding="utf-8", newline="") as f:
        for line in chunk:
            f.write(line + "\n")
    chunk.clear()
    return p


if __name__ == "__main__":
    main()
