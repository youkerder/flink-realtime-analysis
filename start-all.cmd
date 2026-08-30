@echo off
chcp 65001 >nul
title Flink Realtime Analysis - Start All
echo ============================================
echo  Flink Realtime Analysis
echo  [1/4] launching keeper (Kafka + Flink) ...
powershell -NoProfile -Command "Start-Process -FilePath 'wsl.exe' -ArgumentList '-e','bash','/mnt/d/daimaxiangmu/flink-realtime-analysis/start-all.sh' -WindowStyle Hidden"
:waitcluster
timeout /t 5 /nobreak >nul
wsl -e bash -c "curl -s --max-time 3 http://localhost:8081/overview >/dev/null 2>&1" && goto clusterup
echo   waiting for cluster ...
goto waitcluster
:clusterup
echo  [2/4] cluster is up. truncating result tables ...
wsl -e bash -c "curl -s -X POST -d '' 'http://172.31.192.1:8124/?query=TRUNCATE%20TABLE%20rt.metrics_1min'; curl -s -X POST -d '' 'http://172.31.192.1:8124/?query=TRUNCATE%20TABLE%20rt.funnel_1min'; curl -s -X POST -d '' 'http://172.31.192.1:8124/?query=TRUNCATE%20TABLE%20rt.item_topn_1min'" >nul
echo  [3/4] submitting Flink job ...
wsl -e bash -c "cd ~/flinkRT/flink-1.20.5 && nohup bin/flink run -c com.example.realtime.RealtimeAnalysisJob /mnt/d/daimaxiangmu/flink-realtime-analysis/flink-job/target/realtime-analysis-1.0.jar --kafka 127.0.0.1:9092 --clickhouse 'jdbc:clickhouse://172.31.192.1:8124/rt' > /tmp/submit.log 2>&1 & sleep 25"
echo  [4/4] starting ClickHouse + dashboard containers ...
cd /d "%~dp0"
docker compose up -d clickhouse dashboard
echo.
echo  ============================================
echo   All started:
echo     Dashboard : http://localhost:8090
echo     Flink UI  : http://localhost:8081
echo   Replay data (any window):
echo     wsl -e bash -c "cd /mnt/d/daimaxiangmu/flink-realtime-analysis/producer && python3 replay_producer.py --file ~/user_behavior_10m.csv --rate 0 --bootstrap 127.0.0.1:9092"
echo   This window can be closed. Stop: stop-all.cmd
echo  ============================================
timeout /t 15 >nul
