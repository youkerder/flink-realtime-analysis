#!/bin/bash
# 镜像断点自动重试：每轮对缺失镜像轮换源尝试（单次 20 分钟限时），轮间休息 10 分钟。
# 三个镜像全部就绪后自动退出。最多跑 12 轮（约 5-6 小时）。
cd "$(dirname "$0")"

IMAGES=("bitnami/kafka:3.7" "flink:1.20.0" "clickhouse/clickhouse-server:24.8")
SRCS=("docker.1panel.live" "docker.m.daocloud.io" "docker.1ms.run" "hub.rat.dev")

need() { ! docker image inspect "$1" >/dev/null 2>&1; }

src_idx=0
for round in $(seq 1 12); do
  echo "===== 第 $round 轮 $(date '+%m-%d %H:%M') ====="
  all_done=1
  for i in "${!IMAGES[@]}"; do
    img="${IMAGES[$i]}"
    if ! need "$img"; then
      echo "[$img] 已就绪，跳过"
      continue
    fi
    all_done=0
    src="${SRCS[$(( (src_idx + i) % ${#SRCS[@]} ))]}"
    full="$src/$img"
    echo "--- [$img] 尝试 $full（限时 20 分钟）---"
    if timeout 1200 docker pull "$full"; then
      docker tag "$full" "$img" && echo "[$img] 成功（来自 $src）"
    else
      echo "[$img] 本轮未完成"
    fi
  done
  if [ "$all_done" -eq 1 ]; then
    echo "===== 三个镜像全部就绪，重试循环结束 ====="
    docker images --format "{{.Repository}}:{{.Tag}} {{.Size}}" | grep -E "kafka|flink|clickhouse"
    exit 0
  fi
  src_idx=$((src_idx + 1))
  echo "----- 休息 10 分钟后进入下一轮 -----"
  sleep 600
done
echo "===== 达到最大轮数，仍有镜像未就绪 ====="
docker images --format "{{.Repository}}:{{.Tag}} {{.Size}}" | grep -E "kafka|flink|clickhouse" || true
