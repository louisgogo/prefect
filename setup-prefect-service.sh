#!/bin/bash
# Prefect 服务开机启动 + 开放 4200 端口（Rocky / RHEL / CentOS）
# 用法：
#   方式1：同目录下放 prefect-service.conf（可复制 prefect-service.conf.example 再改），然后执行脚本
#   方式2：直接改下面默认值，上传后执行：chmod +x setup-prefect-service.sh && sudo ./setup-prefect-service.sh

set -e

# 默认配置（无配置文件时使用）
PREFECT_USER="${PREFECT_USER:-root}"
PREFECT_HOME="${PREFECT_HOME:-/root/prefect}"
VENV_DIR="${VENV_DIR:-venv}"
PREFECT_PORT="${PREFECT_PORT:-4200}"
# 浏览器/本地上传工作流时访问的地址，必填！填服务器 IP 或域名，例如 10.18.8.191（不能填 0.0.0.0）
PREFECT_SERVER_PUBLIC_HOST="${PREFECT_SERVER_PUBLIC_HOST:-}"

# 若存在同目录下的配置文件，则覆盖上述默认值
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/prefect-service.conf" ]]; then
  echo "读取配置: $SCRIPT_DIR/prefect-service.conf"
  # shellcheck source=prefect-service.conf
  source "$SCRIPT_DIR/prefect-service.conf"
fi

# 服务文件名（改这里则 enable/start 会一起变）
SVC_FILE="/etc/systemd/system/prefect.service"
# unit 名必须与文件名一致：prefect.service -> prefect
UNIT_NAME="$(basename "$SVC_FILE" .service)"

# 未配置时尝试用本机 IP，供浏览器/本地上传使用（不能是 0.0.0.0）
if [[ -z "$PREFECT_SERVER_PUBLIC_HOST" ]]; then
  PREFECT_SERVER_PUBLIC_HOST="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi
if [[ -z "$PREFECT_SERVER_PUBLIC_HOST" || "$PREFECT_SERVER_PUBLIC_HOST" == "0.0.0.0" ]]; then
  echo "警告: 未设置 PREFECT_SERVER_PUBLIC_HOST（浏览器访问用的服务器 IP）"
  echo "      页面会提示 Can't connect to Server API。请在 prefect-service.conf 中设置："
  echo "      PREFECT_SERVER_PUBLIC_HOST=10.18.8.191   # 改成你的服务器 IP"
  echo "      然后重新执行本脚本并: sudo systemctl restart prefect"
  PREFECT_SERVER_PUBLIC_HOST="0.0.0.0"
fi
# 供 UI/客户端使用的 API 地址（必须是浏览器能访问的 IP/域名）
PREFECT_API_URL="http://${PREFECT_SERVER_PUBLIC_HOST}:${PREFECT_PORT}/api"

echo "=== Prefect 服务配置 ==="
echo "用户: $PREFECT_USER"
echo "项目目录: $PREFECT_HOME"
echo "虚拟环境: $PREFECT_HOME/$VENV_DIR"
echo "端口: $PREFECT_PORT"
echo "API 地址(供浏览器/本地上传): $PREFECT_API_URL"
echo ""

if ! id "$PREFECT_USER" &>/dev/null; then
  echo "创建用户: $PREFECT_USER"
  useradd -m -s /bin/bash "$PREFECT_USER"
  echo "请执行: sudo passwd $PREFECT_USER"
else
  echo "用户 $PREFECT_USER 已存在"
fi

PREFECT_BIN="$PREFECT_HOME/$VENV_DIR/bin/prefect"
if [[ ! -x "$PREFECT_BIN" ]]; then
  echo "错误: 未找到 $PREFECT_BIN"
  echo "请先在服务器创建 venv 并安装: cd $PREFECT_HOME && python3 -m venv $VENV_DIR && source $VENV_DIR/bin/activate && pip install prefect"
  exit 1
fi

cat > "$SVC_FILE" << EOF
[Unit]
Description=Prefect Server (workflow API & UI)
After=network.target

[Service]
Type=simple
User=$PREFECT_USER
Group=$PREFECT_USER
WorkingDirectory=$PREFECT_HOME
ExecStart=$PREFECT_BIN server start --host 0.0.0.0
Restart=always
RestartSec=10
Environment=PREFECT_API_URL=$PREFECT_API_URL

[Install]
WantedBy=multi-user.target
EOF

echo "已写入: $SVC_FILE"
systemctl daemon-reload
systemctl enable "$UNIT_NAME"
echo "已设置开机自启"

if command -v firewall-cmd &>/dev/null; then
  firewall-cmd --permanent --add-port=${PREFECT_PORT}/tcp 2>/dev/null || true
  firewall-cmd --reload 2>/dev/null || true
  echo "已开放端口 $PREFECT_PORT/tcp"
else
  echo "请手动开放端口 $PREFECT_PORT（或云安全组）"
fi

echo ""
echo "=== 后续 ==="
echo "1. 属主: sudo chown -R $PREFECT_USER:$PREFECT_USER $PREFECT_HOME"
echo "2. 启动: sudo systemctl start $UNIT_NAME"
echo "3. 状态: sudo systemctl status $UNIT_NAME"
echo "4. 本地上传工作流: 设置 PREFECT_API_URL=http://<服务器IP>:$PREFECT_PORT/api"
