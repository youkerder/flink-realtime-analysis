#!/usr/bin/env python3
"""一键启动「Windows 原生模式」全链路（无需 Docker / WSL）。

启动顺序：
    Kafka(broker, 9092) → ClickHouse 兼容层 + 大屏(8090) → Flink 作业(本地模式)
可选：
    --replay N   回放 N 行数据到 Kafka（需要 data/ 或 tools/ 下已有排序后的 CSV）
    --reset      清空三张结果表再启动作业

与仓库原脚本（start-all.cmd）的区别：
  * 不依赖 WSL 家目录、不写死 172.31.192.1；
  * 组件已在运行则跳过（幂等，可重复执行）；
  * 日志固定写到 runtime/logs/，便于排错。

用法：
    python tools/native_up.py                # 只启动，不回放
    python tools/native_up.py --replay 5000000
    python tools/native_up.py --reset --replay 5000000
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KAFKA_HOME = os.path.join(ROOT, "runtime", "kafka_2.13-4.3.1")
KAFKA_CONF = os.path.join(KAFKA_HOME, "config", "verify.properties")
FLINK_LIB = os.path.join(ROOT, "runtime", "flink-1.20.5", "lib")
JAR = os.path.join(ROOT, "flink-job", "target", "realtime-analysis-1.0.jar")
LOGS = os.path.join(ROOT, "runtime", "logs")
PIDS = os.path.join(ROOT, "runtime", "pids")
SHIM = os.path.join(ROOT, "tools", "ch_http_server.py")
DASHBOARD_PORT = 8090
KAFKA_PORT = 9092
SPAWNED = []

ADD_OPENS = [
    "--add-opens", "java.base/java.util=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang=ALL-UNNAMED",
    "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens", "java.base/java.io=ALL-UNNAMED",
    "--add-opens", "java.base/java.nio=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
]


def log(msg):
    print(msg, flush=True)


def java_major(exe):
    """返回 java 主版本号（8/11/17/21...），探测失败返回 0。"""
    try:
        out = subprocess.run([exe, "-version"], capture_output=True, text=True, errors="replace").stderr
        m = re.search(r'version "(\d+)', out)
        return int(m.group(1)) if m else 0
    except Exception:
        return 0


def find_java():
    """选一个 >= 17 的 JDK。

    坑：本机默认 JAVA_HOME 可能指向 JDK 8（Maven 老配置），而 Kafka 4.x 与
    Flink 1.20 都要求 17+，直接拿来启动会报
    "Unrecognized option: --add-opens"。所以这里必须校验主版本号，
    不能只看 JAVA_HOME 是否存在。
    """
    cands = []
    home = os.environ.get("JAVA_HOME")
    if home:
        cands.append(os.path.join(home, "bin", "java.exe"))
    cands.append(r"C:\Program Files\Microsoft\jdk-17.0.8.7-hotspot\bin\java.exe")
    for base in (r"C:\Program Files\Java", r"C:\Program Files\Microsoft"):
        if os.path.isdir(base):
            for name in sorted(os.listdir(base), reverse=True):
                if name.lower().startswith("jdk"):
                    cands.append(os.path.join(base, name, "bin", "java.exe"))
    for exe in cands:
        if os.path.isfile(exe) and java_major(exe) >= 17:
            return exe
    return "java"


def port_open(port, host="127.0.0.1"):
    with socket.socket() as s:
        s.settimeout(1.0)
        return s.connect_ex((host, port)) == 0


_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def http_ok(url, timeout=3):
    """本机探测必须绕过系统代理，否则 curl/urllib 会走 http_proxy 得到 502。"""
    try:
        with _OPENER.open(url, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def wait_for(pred, timeout, interval=2, desc=""):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(interval)
    log(f"  ! 等待超时: {desc}")
    return False


def spawn(name, cmd, cwd=None, env=None, detach=False):
    os.makedirs(LOGS, exist_ok=True)
    os.makedirs(PIDS, exist_ok=True)
    log_path = os.path.join(LOGS, f"{name}.log")
    log_file = open(log_path, "w", encoding="utf-8")
    creation = 0
    if os.name == "nt" and detach:
        creation = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    p = subprocess.Popen(cmd, cwd=cwd, stdout=log_file, stderr=subprocess.STDOUT,
                         env=env, creationflags=creation)
    SPAWNED.append(p)
    with open(os.path.join(PIDS, f"{name}.json"), "w", encoding="utf-8") as f:
        json.dump({"pid": p.pid, "cmd": cmd, "log": log_path}, f)
    log(f"  -> {name} 已启动 (pid={p.pid}, log={os.path.relpath(log_path, ROOT)})")
    return p


def ensure_jar(java):
    if os.path.isfile(JAR):
        log(f"[1/4] 作业 jar 已存在: {os.path.relpath(JAR, ROOT)}")
        return
    log("[1/4] 未找到 jar，开始 mvn package ...")
    mvn = "mvn.cmd" if os.name == "nt" else "mvn"
    subprocess.run([mvn, "-B", "-q", "clean", "package", "-DskipTests"],
                   cwd=os.path.join(ROOT, "flink-job"), check=True,
                   env={**os.environ, "JAVA_HOME": os.path.dirname(os.path.dirname(java))})
    log("     构建完成")


def ensure_kafka(java, detach=False):
    if port_open(KAFKA_PORT):
        log("[2/4] Kafka 已在运行 (9092)")
        return
    log("[2/4] 启动 Kafka broker ...")
    spawn("kafka", [java, "--add-opens=java.base/java.nio=ALL-UNNAMED", "-Xmx1g",
                    "-cp", "libs/*", "kafka.Kafka", "config/verify.properties"],
          cwd=KAFKA_HOME, detach=detach)
    wait_for(lambda: port_open(KAFKA_PORT), 90, desc="Kafka 9092")


def ensure_topic(java):
    subprocess.run([java, "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", "libs/*",
                    "org.apache.kafka.tools.TopicCommand", "--bootstrap-server", "localhost:9092",
                    "--create", "--if-not-exists", "--topic", "user_behavior",
                    "--partitions", "3", "--replication-factor", "1"],
                   cwd=KAFKA_HOME, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def ensure_shim(py, detach=False):
    if http_ok(f"http://127.0.0.1:{DASHBOARD_PORT}/stats"):
        log(f"[3/4] 大屏 / ClickHouse 兼容层已在运行 ({DASHBOARD_PORT})")
        return
    log(f"[3/4] 启动 ClickHouse 兼容层 + 大屏 ({DASHBOARD_PORT}) ...")
    spawn("shim", [py, SHIM, "--port", str(DASHBOARD_PORT),
                   "--db", os.path.join(ROOT, "runtime", "warehouse.duckdb"),
                   "--dashboard", os.path.join(ROOT, "dashboard")], cwd=ROOT, detach=detach)
    wait_for(lambda: http_ok(f"http://127.0.0.1:{DASHBOARD_PORT}/stats"), 60, desc="shim 8090")


def truncate_tables():
    for t in ("metrics_1min", "funnel_1min", "item_topn_1min"):
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{DASHBOARD_PORT}/clickhouse/?query=TRUNCATE%20TABLE%20rt.{t}",
                data=b"", method="POST")
            _OPENER.open(req, timeout=10).read()
        except Exception as e:
            log(f"  ! 清空 {t} 失败: {e}")


def job_running():
    path = os.path.join(PIDS, "job.json")
    if not os.path.isfile(path):
        return False
    try:
        pid = json.load(open(path))["pid"]
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                             capture_output=True, text=True, errors="replace").stdout
        return str(pid) in out
    except Exception:
        return False


def ensure_job(java, detach=False):
    if job_running():
        log("[4/4] Flink 作业已在运行")
        return
    log("[4/4] 提交 Flink 作业（本地模式，sink=http）...")
    cp = os.pathsep.join([os.path.join(FLINK_LIB, "*"), JAR])
    spawn("job", [java, "-Xmx4g", *ADD_OPENS, "-cp", cp,
                  "com.example.realtime.RealtimeAnalysisJob",
                  "--kafka", "127.0.0.1:9092",
                  "--sink", "http",
                  "--store", f"http://127.0.0.1:{DASHBOARD_PORT}/clickhouse/"],
          cwd=ROOT, detach=detach)
    time.sleep(8)


def replay(py, rows, src):
    log(f"[replay] 回放 {rows:,} 行 -> Kafka ...")
    cmd = [py, os.path.join(ROOT, "producer", "replay_producer.py"),
           "--file", src, "--bootstrap", "127.0.0.1:9092",
           "--topic", "user_behavior", "--rate", "0", "--limit", str(rows)]
    subprocess.run(cmd, cwd=ROOT)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", type=int, default=0, help="回放行数，0 表示不回放")
    ap.add_argument("--file", default=os.path.join(ROOT, "tools", "UserBehavior.sorted.csv"),
                    help="回放用的、已按事件时间排序的 CSV")
    ap.add_argument("--reset", action="store_true", help="启动前清空结果表")
    ap.add_argument("--detach", action="store_true",
                    help="启动后立即退出，组件在后台继续跑（默认留在前台托管，Ctrl+C 一起停）")
    args = ap.parse_args()

    java = find_java()
    py = sys.executable
    log(f"java  : {java}  (major={java_major(java)})")
    log(f"python: {py}")
    if java_major(java) < 17:
        log("! 未找到 JDK 17+，Kafka 4.x / Flink 1.20 无法启动；请先安装 JDK 17 或设置 JAVA_HOME")
        return 2

    ensure_jar(java)
    ensure_kafka(java, args.detach)
    ensure_topic(java)
    ensure_shim(py, args.detach)
    if args.reset:
        truncate_tables()
    ensure_job(java, args.detach)

    if args.replay:
        if not os.path.isfile(args.file):
            log(f"  ! 回放文件不存在: {args.file}（先跑 tools/sort_dataset.py 生成）")
        else:
            replay(py, args.replay, args.file)
            time.sleep(5)

    log("")
    log("=" * 62)
    log(f"  大屏      : http://localhost:{DASHBOARD_PORT}")
    log(f"  写入统计  : http://localhost:{DASHBOARD_PORT}/stats")
    log(f"  作业日志  : runtime/logs/job.log   兼容层日志: runtime/logs/shim.log")
    log("=" * 62)

    if args.detach:
        return 0

    # 前台托管：保持进程不退出，子进程随本进程一起存活；Ctrl+C 统一停止
    log("服务运行中，按 Ctrl+C 停止全部组件 ...")
    try:
        while True:
            time.sleep(5)
            if not port_open(KAFKA_PORT):
                log("! Kafka 掉线了，请查看 runtime/logs/kafka.log")
    except KeyboardInterrupt:
        log("正在停止 ...")
        for p in SPAWNED:
            try:
                p.terminate()
            except Exception:
                pass
        return 0


if __name__ == "__main__":
    main()
