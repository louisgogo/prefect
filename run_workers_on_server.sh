#!/bin/bash
# 在服务器 10.18.8.191 上执行此脚本，让 flow 在服务器上运行
# 用法：cd /root/prefect && chmod +x run_workers_on_server.sh && ./run_workers_on_server.sh

set -e
cd "$(dirname "$0")"

# 连本机 Prefect Server，这样 serve() 进程在服务器上执行 flow
export PREFECT_API_URL=http://127.0.0.1:4200/api

if [ -d "venv" ]; then
  source venv/bin/activate
fi

echo "=== 在服务器上启动 Prefect Workers（flow 将在本机执行）==="
echo "PREFECT_API_URL=$PREFECT_API_URL"
echo "按 Ctrl+C 停止"
echo ""

exec python deploy_to_server.py
