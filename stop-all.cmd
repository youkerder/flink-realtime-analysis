@echo off
chcp 65001 >nul
title Flink Realtime Analysis - Stop All
echo Stopping platform ...
wsl -e bash -c "touch ~/stop_flag; sleep 12; rm -f ~/stop_flag; echo WSL side stopped"
cd /d "%~dp0"
docker compose stop clickhouse dashboard
echo.
echo  All stopped.
pause
