#!/bin/bash
# 在服务器上执行：先 git pull、安装依赖，再启动 deploy_to_server.py（推送流程）
# 可用于 systemd 的 ExecStart，或手动执行：./scripts/update_and_serve.sh
set -e
cd "$(dirname "$0")/.."
export PREFECT_API_URL=http://127.0.0.1:4200/api
[ -d venv ] && source venv/bin/activate
echo "=== git pull ==="
git pull
echo "=== pip install -r requirements.txt ==="
pip install -r requirements.txt -q
echo "=== 启动 Workers（推送流程）==="
exec python deploy_to_server.py
