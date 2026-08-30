@echo off
chcp 65001 >nul
REM 一键启动实时分析平台：在 WSL 内启动 Kafka + Flink 集群并提交作业，然后拉起 ClickHouse/大屏容器
REM 注意：请保持本窗口开启（Kafka/Flink 进程随本会话存活）；停止请运行 stop-all.cmd
wsl -e bash -c "cd ~/kafkaRT/kafka_2.13-4.3.1 && bin/kafka-server-start.sh -daemon ~/kafkaRT/server.properties && sleep 8 && cd ~/flinkRT/flink-1.20.5 && bin/start-cluster.sh && sleep 10 && nohup bin/flink run -c com.example.realtime.RealtimeAnalysisJob /mnt/d/daimaxiangmu/flink-realtime-analysis/flink-job/target/realtime-analysis-1.0.jar --kafka 127.0.0.1:9092 --clickhouse 'jdbc:clickhouse://172.31.192.1:8124/rt' > /tmp/submit.log 2>&1 && echo 'Flink 作业已提交'"
cd /d "%~dp0"
docker compose up -d clickhouse dashboard
echo.
echo ============================================
echo   全栈已启动:
echo    - Flink UI : http://localhost:8081
echo    - 实时大屏 : http://localhost:8090
echo   回放数据（新开窗口）:
echo    cd producer ^&^& python replay_producer.py
echo      --file ../data/user_behavior_10m.csv --rate 0
echo      --bootstrap 127.0.0.1:9092
echo   保持本窗口开启，停止请运行 stop-all.cmd
echo ============================================
cmd /k
