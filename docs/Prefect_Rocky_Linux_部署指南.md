# Prefect 在 Rocky Linux 上的部署指南

Rocky Linux 与 RHEL/CentOS 兼容，以下提供两种部署方式：**直接安装** 和 **Docker**。

---

## 一、环境准备

### 1. 安装 Python 3.10+

```bash
# 安装 Python 3 和 pip
sudo dnf install -y python3 python3-pip python3-devel

# 若系统自带的 Python 版本低于 3.10，可启用 CodeReady 或 安装较新版本
python3 --version   # 需 >= 3.10
```

### 2. （可选）创建专用用户

```bash
sudo useradd -m -s /bin/bash prefect
sudo passwd prefect
# 后续若用该用户跑服务，可切换： sudo su - prefect
```

---

## 二、方式 A：直接安装 Prefect Server

### 1. 安装 Prefect

```bash
# 使用当前用户或 prefect 用户
pip3 install --user prefect
# 或使用虚拟环境（推荐）
python3 -m venv ~/prefect-env
source ~/prefect-env/bin/activate
pip install -U prefect
```

### 2. 验证安装

```bash
prefect version
```

### 3. 使用 systemd 运行 Prefect Server（开机自启、崩溃自重启）

创建服务文件：

```bash
sudo vim /etc/systemd/system/prefect-server.service
```

内容示例（按需修改用户、路径）：

```ini
[Unit]
Description=Prefect Server
After=network.target

[Service]
Type=simple
User=prefect
Group=prefect
WorkingDirectory=/home/prefect
# 若用虚拟环境，把 /usr/bin/prefect 改为 /home/prefect/prefect-env/bin/prefect
ExecStart=/usr/bin/prefect server start --host 0.0.0.0
Restart=always
RestartSec=10
Environment=PREFECT_API_URL=http://127.0.0.1:4200/api

[Install]
WantedBy=multi-user.target
```

若 Prefect 装在虚拟环境里：

```ini
ExecStart=/home/prefect/prefect-env/bin/prefect server start --host 0.0.0.0
```

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable prefect-server
sudo systemctl start prefect-server
sudo systemctl status prefect-server
```

访问 UI：`http://<服务器IP>:4200`

### 4. 开放防火墙端口（若启用 firewalld）

```bash
sudo firewall-cmd --permanent --add-port=4200/tcp
sudo firewall-cmd --reload
```

---

## 三、方式 B：使用 Docker 运行 Prefect Server

### 1. 安装 Docker

```bash
sudo dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $USER
# 重新登录后生效
```

### 2. 运行 Prefect Server 容器

```bash
docker run -d \
  --name prefect-server \
  -p 4200:4200 \
  prefecthq/prefect:3-latest \
  prefect server start --host 0.0.0.0
```

需要数据持久化时，可挂载卷（具体路径按官方文档或你使用的 Prefect 版本调整）：

```bash
docker run -d \
  --name prefect-server \
  -p 4200:4200 \
  -v prefect-data:/root/.prefect \
  prefecthq/prefect:3-latest \
  prefect server start --host 0.0.0.0
```

### 3. 开机自启容器

```bash
docker update --restart=unless-stopped prefect-server
```

同样在防火墙中开放 4200 端口（见上文）。

---

## 四、部署 Worker（执行 Flow）

Worker 需要能连上 Prefect Server（或 Prefect Cloud），并拉取你的项目代码执行 flow。

### 1. 在 Rocky 上安装 Prefect 与项目依赖

```bash
python3 -m venv ~/prefect-worker-env
source ~/prefect-worker-env/bin/activate
pip install -U prefect
# 安装你项目所需依赖，例如
pip install pandas sqlalchemy psycopg2-binary openpyxl
```

### 2. 配置 Worker 连接 Server

```bash
# 连接自建 Server
export PREFECT_API_URL=http://<Prefect-Server-IP>:4200/api
prefect config set PREFECT_API_URL=http://<Prefect-Server-IP>:4200/api
```

### 3. 使用 systemd 运行 Worker（与你在本地用的 serve 方式对应）

你本地用 `flow.serve()` 长期跑并注册到 Server，同一套代码在服务器上也可以这样跑；若改用 **Worker + work pool**，则在服务器上只跑 worker。

**方式 3a：直接跑你的部署脚本（多进程 serve）**

例如跑你的 `deploy_production.py` 或 `deploy_to_server.py`：

```bash
# 示例：/opt/prefect/flows 为项目路径
cd /opt/prefect/flows
source /path/to/venv/bin/activate
python deploy_production.py
```

用 systemd 托管（示例）：

```ini
# /etc/systemd/system/prefect-worker.service
[Unit]
Description=Prefect Worker (serve)
After=network.target

[Service]
Type=simple
User=prefect
Group=prefect
WorkingDirectory=/opt/prefect/flows
Environment=PREFECT_API_URL=http://127.0.0.1:4200/api
ExecStart=/opt/prefect/flows/venv/bin/python deploy_production.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**方式 3b：只跑 Prefect Worker（需在 Server UI 里先建好 work pool）**

```bash
# 在 Server UI 中创建 work pool 后，在服务器上启动 worker
prefect worker start --pool <your-pool-name>
```

同样可写成 systemd 服务，把 `ExecStart` 改为上述命令。

---

## 五、简要对照

| 项目         | 方式 A（直接安装）     | 方式 B（Docker）        |
|--------------|------------------------|---------------------------|
| 安装         | `pip install prefect`  | `docker run ... prefect` |
| 进程管理     | systemd                | Docker + restart 策略    |
| 端口         | 4200                   | 4200                      |
| 适用场景     | 希望直接用系统 Python/venv | 希望环境隔离、易迁移   |

---

## 六、常见问题

1. **无法访问 UI**  
   检查：`systemctl status prefect-server` 或 `docker ps`；防火墙是否放行 4200；`prefect server start` 是否使用 `--host 0.0.0.0`。

2. **Worker 连不上 Server**  
   确认 `PREFECT_API_URL` 为 `http://<IP>:4200/api`（带 `/api`）；Server 与 Worker 之间网络、防火墙是否可达。

3. **Flow 依赖本地文件/数据库**  
   Worker 所在机器需能访问相同路径或数据库；若用 Docker 跑 Worker，需挂载代码目录或配置好数据库连接。

按上述步骤即可在 Rocky Linux 上完成 Prefect Server 与 Worker 的部署；你当前项目仍可用本地 `deploy_production.py` / `deploy_to_server.py` 推送 flow 到该 Server。
