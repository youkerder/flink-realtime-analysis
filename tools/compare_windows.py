#!/usr/bin/env python3
"""按 Flink 作业完全相同的口径，从（已排序的）CSV 计算每分钟期望指标，并与 Flink 实际输出对账。

指标口径与 RealtimeAnalysisJob.MetricsWindowProcess 一致：
  pv/cart/fav/buy = 窗口内该类行为条数
  uv              = 窗口内去重用户数
  stage1          = 有 pv 的用户数
  stage2          = 有 pv 且有 cart/fav 的用户数
  stage3          = 有 pv 且有 buy 的用户数

用法：
  python tools/compare_windows.py --sorted tools/UserBehavior.sorted.csv \
      --flink tools/out_full_metrics.csv --out tools/diff_report.txt
"""
import argparse
import csv
from collections import defaultdict

TS_MIN = 1511539200
TS_MAX = 1512336000


def expected(path):
    win = defaultdict(lambda: {"pv": 0, "cart": 0, "fav": 0, "buy": 0,
                               "users": set(), "pv_u": set(), "cf_u": set(), "buy_u": set()})
    n = 0
    kept = 0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            n += 1
            if len(row) < 5:
                continue
            try:
                u, b, t = int(row[0]), row[3], int(row[4])
            except ValueError:
                continue
            if b not in ("pv", "cart", "fav", "buy") or t < TS_MIN or t > TS_MAX:
                continue
            kept += 1
            w = win[t // 60 * 60]
            w[b] += 1
            w["users"].add(u)
            if b == "pv":
                w["pv_u"].add(u)
            elif b in ("cart", "fav"):
                w["cf_u"].add(u)
            else:
                w["buy_u"].add(u)
    out = {}
    for ws, w in win.items():
        out[ws * 1000] = (w["pv"], len(w["users"]), w["cart"], w["fav"], w["buy"],
                          len(w["pv_u"]), len(w["cf_u"] & w["pv_u"]), len(w["buy_u"] & w["pv_u"]))
    return out, n, kept


def flink(path):
    out = {}
    dup = 0
    with open(path, encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) < 10:
                continue
            ws = int(row[0])
            vals = tuple(int(x) for x in row[2:10])
            if ws in out:
                dup += 1
            out[ws] = vals
    return out, dup


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sorted", required=True)
    ap.add_argument("--flink", required=True)
    ap.add_argument("--out", default="tools/diff_report.txt")
    args = ap.parse_args()

    exp, total, kept = expected(args.sorted)
    act, dup = flink(args.flink)

    only_exp = sorted(set(exp) - set(act))
    only_act = sorted(set(act) - set(exp))
    both = sorted(set(exp) & set(act))

    cols = ["pv", "uv", "cart", "fav", "buy", "stage1", "stage2", "stage3"]
    diffs = defaultdict(int)
    mismatched = []
    for ws in both:
        e, a = exp[ws], act[ws]
        if e != a:
            bad = [cols[i] for i in range(8) if e[i] != a[i]]
            for c in bad:
                diffs[c] += 1
            if len(mismatched) < 20:
                mismatched.append((ws, e, a))

    lines = []
    lines.append(f"rows_in_file      : {total:,}")
    lines.append(f"rows_kept         : {kept:,}")
    lines.append(f"expected_windows  : {len(exp):,}")
    lines.append(f"flink_windows     : {len(act):,}  (duplicate rows: {dup})")
    lines.append(f"windows_matched   : {len(both):,}")
    lines.append(f"windows_only_exp  : {len(only_exp):,}  {only_exp[:8]}")
    lines.append(f"windows_only_flink: {len(only_act):,}  {only_act[:8]}")
    lines.append(f"mismatch_windows  : {sum(1 for ws in both if exp[ws] != act[ws])}")
    lines.append(f"mismatch_by_col   : {dict(diffs)}")
    for ws, e, a in mismatched:
        lines.append(f"  diff ws={ws} expected={e} actual={a}")
    txt = "\n".join(lines)
    print(txt)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(txt + "\n")


if __name__ == "__main__":
    main()
