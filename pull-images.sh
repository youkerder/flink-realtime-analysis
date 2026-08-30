#!/bin/bash
# 逐个镜像拉取，失败自动换源：1panel → DaoCloud → hub.rat.dev → 官方源
# 单次尝试限时 600 秒；已下载的层会被 Docker 缓存复用，重试不浪费
set -u
cd "$(dirname "$0")"

pull_one() {
  local image="$1"
  local sources=("docker.1panel.live" "docker.m.daocloud.io" "hub.rat.dev" "")
  for src in "${sources[@]}"; do
    local full="$image"
    [ -n "$src" ] && full="$src/$image"
    echo "===== [$image] 尝试源: ${src:-官方 docker.io} ====="
    if timeout 600 docker pull "$full"; then
      if [ -n "$src" ]; then
        docker tag "$full" "$image" && echo "===== [$image] 成功（来自 $src）"
      else
        echo "===== [$image] 成功（来自官方源）"
      fi
      return 0
    fi
    echo "===== [$image] 该源失败/超时，换下一个源"
  done
  echo "===== [$image] 所有源均失败"
  return 1
}

echo "########## [1/3] Kafka ##########"
pull_one "bitnami/kafka:3.7"
echo "########## [2/3] Flink ##########"
pull_one "flink:1.20.0"
echo "########## [3/3] ClickHouse ##########"
pull_one "clickhouse/clickhouse-server:24.8"

echo "########## 最终镜像清单 ##########"
docker images --format "{{.Repository}}:{{.Tag}} {{.Size}}" | grep -E "kafka|flink|clickhouse" || true
