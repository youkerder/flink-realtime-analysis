#!/usr/bin/env python3
"""ClickHouse HTTP 协议兼容层 + 大屏静态服务（Windows 原生模式专用）。

背景
----
本项目原设计是 ClickHouse 容器 + Nginx 反代。Windows 上没有 ClickHouse 官方构建，
若本机无法运行 Docker/WSL，整条链路就起不来。这个脚本用 DuckDB（嵌入式列存引擎）
承载 ClickHouse 的 HTTP 接口子集，让 Flink Sink 和大屏都**不用改一行 SQL**：

    Flink --HTTP(INSERT ... FORMAT JSONEachRow)--> 本服务 --DuckDB--> warehouse.duckdb
    浏览器 --GET /clickhouse/?query=SELECT ... FORMAT JSON--> 本服务 --DuckDB--> JSON

同时把 dashboard/ 目录当静态站点发布，因此不需要 Nginx，也没有跨域问题。

端点
----
  GET  /                     大屏页面（dashboard/index.html）
  GET  /clickhouse/?query=…  ClickHouse 查询（返回 FORMAT JSON 结构）
  POST /clickhouse/?query=INSERT INTO … FORMAT JSONEachRow   批量写入
  GET  /stats                各结果表行数（自检用）

用法
----
  python tools/ch_http_server.py --port 8090 --db runtime/warehouse.duckdb
"""
import argparse
import json
import os
import re
import threading
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

import duckdb

DB_LOCK = threading.Lock()
DB = None
DASHBOARD_DIR = None

TABLES = ("metrics_1min", "funnel_1min", "item_topn_1min")


def init_db(path):
    global DB
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    DB = duckdb.connect(path)
    DB.execute("CREATE SCHEMA IF NOT EXISTS rt")
    # 主键 = 幂等键：窗口级结果天然按 (窗口, 维度) 唯一，重复写入等价于覆盖。
    # 这样即使客户端因为超时重发同一批数据（或作业从 checkpoint 重放），结果也不会翻倍。
    # 对应到生产 ClickHouse 就是 ReplacingMergeTree(ver) + ORDER BY 窗口键。
    DB.execute("""
        CREATE TABLE IF NOT EXISTS rt.metrics_1min (
            window_start TIMESTAMP, window_end TIMESTAMP,
            pv BIGINT, uv BIGINT, cart BIGINT, fav BIGINT, buy BIGINT,
            PRIMARY KEY (window_start))
    """)
    DB.execute("""
        CREATE TABLE IF NOT EXISTS rt.funnel_1min (
            window_start TIMESTAMP, stage VARCHAR, users BIGINT,
            PRIMARY KEY (window_start, stage))
    """)
    DB.execute("""
        CREATE TABLE IF NOT EXISTS rt.item_topn_1min (
            window_start TIMESTAMP, item_id BIGINT, category_id BIGINT, cnt BIGINT,
            PRIMARY KEY (window_start, item_id))
    """)


def fmt(value):
    """把值转成 ClickHouse HTTP JSON 的风格：时间/数值都给字符串。"""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def split_format(sql):
    """剥掉 ClickHouse 的 FORMAT 子句，返回 (纯净 SQL, 格式名)。"""
    m = re.search(r"\bFORMAT\s+([A-Za-z0-9]+)\s*;?\s*$", sql, re.IGNORECASE)
    if not m:
        return sql.strip().rstrip(";"), "JSON"
    return sql[:m.start()].strip().rstrip(";"), m.group(1).upper()


def to_clickhouse_json(columns, rows):
    meta = [{"name": c[0], "type": "String"} for c in columns]
    data = [{c[0]: fmt(v) for c, v in zip(columns, row)} for row in rows]
    return json.dumps({"meta": meta, "data": data, "rows": len(data),
                       "statistics": {"elapsed": 0.0, "rows_read": len(data)}}, ensure_ascii=False)


INSERT_COLS = {
    "metrics_1min": ["window_start", "window_end", "pv", "uv", "cart", "fav", "buy"],
    "funnel_1min": ["window_start", "stage", "users"],
    "item_topn_1min": ["window_start", "item_id", "category_id", "cnt"],
}


def do_insert(sql, body):
    """INSERT INTO rt.<table> FORMAT JSONEachRow —— 按行解析后批量写入。"""
    m = re.search(r"INSERT\s+INTO\s+(?:rt\.)?(\w+)", sql, re.IGNORECASE)
    if not m:
        raise ValueError("无法解析 INSERT 目标表: " + sql)
    table = m.group(1)
    if table not in INSERT_COLS:
        raise ValueError("未知表: " + table)
    cols = INSERT_COLS[table]
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        rows.append(tuple(obj.get(c) for c in cols))
    if not rows:
        return "Ok."

    casts = []
    for c in cols:
        if c in ("window_start", "window_end"):
            casts.append("CAST(? AS TIMESTAMP)")
        elif c == "stage":
            casts.append("CAST(? AS VARCHAR)")
        else:
            casts.append("CAST(? AS BIGINT)")
    one_row = "(" + ",".join(casts) + ")"
    stmt = (f"INSERT OR REPLACE INTO rt.{table} ({','.join(cols)}) VALUES "
            + ",".join([one_row] * len(rows)))
    flat = [v for row in rows for v in row]
    with DB_LOCK:
        DB.execute(stmt, flat)
    return "Ok."


def run_query(sql):
    with DB_LOCK:
        cur = DB.execute(sql)
        columns = [(d[0], d[1]) for d in cur.description] if cur.description else []
        rows = cur.fetchall() if cur.description else []
    return to_clickhouse_json(columns, rows)


class Server(ThreadingHTTPServer):
    """默认 backlog 只有 5，三个 Sink + 大屏轮询并发时会出现连接被拒（偶发写入失败）。
    这里放宽队列并让请求线程随主进程退出。"""
    daemon_threads = True
    request_queue_size = 128
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt_, *args):  # 静音，避免刷屏
        pass

    # ---------- 静态资源 ----------
    def _serve_static(self, path):
        if path in ("/", "/index.html"):
            path = "/index.html"
        full = os.path.join(DASHBOARD_DIR, path.lstrip("/").split("?")[0])
        if not os.path.isfile(full):
            self.send_error(404, "not found")
            return
        ctype = "text/html; charset=utf-8"
        if full.endswith(".js"):
            ctype = "application/javascript; charset=utf-8"
        elif full.endswith(".css"):
            ctype = "text/css; charset=utf-8"
        with open(full, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send(self, code, body, ctype="text/plain; charset=utf-8"):
        raw = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    # ---------- 请求入口 ----------
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/clickhouse"):
            params = parse_qs(parsed.query)
            sql = (params.get("query") or params.get("QUERY") or [""])[0]
            return self._handle_sql(sql, "")
        if parsed.path == "/stats":
            out = {}
            with DB_LOCK:
                for t in TABLES:
                    out[t] = DB.execute(f"SELECT count(*) FROM rt.{t}").fetchone()[0]
            return self._send(200, json.dumps(out), "application/json")
        return self._serve_static(self.path)

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace") if length else ""
        if parsed.path.startswith("/clickhouse"):
            params = parse_qs(parsed.query)
            sql = (params.get("query") or [""])[0]
            if not sql:
                sql = body            # 兼容「SQL 直接放 body」
                body = ""
            return self._handle_sql(sql, body)
        return self._send(404, "not found")

    def _handle_sql(self, sql, body):
        sql = unquote(sql).strip()
        if not sql:
            return self._send(400, "empty query")
        try:
            clean, fmt_name = split_format(sql)
            head = clean.lstrip().upper()
            if head.startswith("INSERT"):
                return self._send(200, do_insert(clean, body))
            if head.startswith("TRUNCATE"):
                table = re.sub(r"TRUNCATE\s+TABLE\s+", "", clean, flags=re.IGNORECASE).strip()
                with DB_LOCK:
                    DB.execute(f"DELETE FROM {table}")
                return self._send(200, "Ok.")
            if head.startswith("SET "):
                return self._send(200, "Ok.")   # 兼容客户端初始化语句
            if head.startswith("SELECT") or head.startswith("WITH") or head.startswith("DESCRIBE"):
                return self._send(200, run_query(clean), "application/json")
            with DB_LOCK:
                DB.execute(clean)
            return self._send(200, "Ok.")
        except Exception as exc:  # noqa: BLE001
            return self._send(500, f"DB::Exception: {exc}")


def main():
    global DASHBOARD_DIR
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--db", default="runtime/warehouse.duckdb")
    ap.add_argument("--dashboard", default="dashboard")
    args = ap.parse_args()

    DASHBOARD_DIR = os.path.abspath(args.dashboard)
    init_db(args.db)
    srv = Server(("127.0.0.1", args.port), Handler)
    print(f"clickhouse-http shim + dashboard on http://localhost:{args.port}")
    print(f"  duckdb warehouse : {os.path.abspath(args.db)}")
    print(f"  dashboard dir    : {DASHBOARD_DIR}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
