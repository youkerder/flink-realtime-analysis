@echo off
chcp 65001 >nul
REM 停止实时分析平台
wsl -e bash -c "cd ~/flinkRT/flink-1.20.5 && bin/stop-cluster.sh; pkill -f 'kafka[.]Kafka' 2>/dev/null; echo WSL 侧已停止"
cd /d "%~dp0"
docker compose stop clickhouse dashboard
echo 已全部停止
pause
