# 在服务器 10.18.8.191 上执行 Flow

要让 flow 在服务器上跑（而不是在本地跑），需要：**把同一套代码和依赖放到服务器上，在服务器上运行“部署/worker”脚本**，让 `flow.serve()` 在服务器上连本机 Prefect Server 并执行 flow。

---

## 一、整体关系

| 角色 | 作用 |
|------|------|
| **Prefect Server**（10.18.8.191:4200） | 提供 API 和 UI，记录 deployment、flow run |
| **Worker / serve 进程**（也在 10.18.8.191 上跑） | 连到本机 Server，收到触发后**在服务器上执行** flow |

本地只做两件事：  
1）把代码同步到服务器；  
2）在浏览器打开 http://10.18.8.191:4200 触发运行。  
**不再在本地运行** `deploy_to_server.py` 做长期 serve。

---

## 二、步骤概览

1. 把整份 Prefect 项目拷贝到服务器（例如 `/root/prefect`）。
2. 在服务器上装依赖、配好环境（venv + `pip install -r requirements.txt`）。
3. 在服务器上运行“worker/部署”脚本，且连**本机** Prefect API：`PREFECT_API_URL=http://127.0.0.1:4200/api`。
4. （可选）用 systemd 把步骤 3 做成开机自启。

---

## 三、具体操作

### 1. 把代码放到服务器

在**本机**把整个 `D:\prefect` 项目同步到服务器（保留 `modules`、`utils`、`deploy_to_server.py`、`requirements.txt` 等），例如：

```bash
# 本机（PowerShell）示例：用 scp 同步到服务器
scp -r D:\prefect\modules D:\prefect\utils D:\prefect\deploy_to_server.py D:\prefect\deploy_production.py D:\prefect\requirements.txt root@10.18.8.191:/root/prefect/
```

或使用 rsync、Git、压缩包上传等，保证服务器上有完整代码（含 `modules`、`utils` 及 `mypackage` 等依赖路径，若项目里有的话也要一起拷）。

### 2. 在服务器上准备环境

SSH 到 10.18.8.191 后：

```bash
cd /root/prefect
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# 若项目依赖 mypackage 等，需一并安装或放到 PYTHONPATH
```

确认 Prefect Server 已在本机运行（例如 `systemctl status prefect`），且本机可访问：`curl -s http://127.0.0.1:4200/api/health` 有正常返回。

### 3. 在服务器上运行“worker/部署”脚本（连本机 API）

在**服务器**上执行（注意是连 **127.0.0.1**，不是 10.18.8.191）：

```bash
cd /root/prefect
source venv/bin/activate
export PREFECT_API_URL=http://127.0.0.1:4200/api
python deploy_to_server.py
```

这样三个 `flow.serve()` 进程会在**本机**启动，并连到本机的 Prefect Server；在 UI 里点的“运行”会由**服务器**上的这些进程执行。

- 若希望用脚本里写死的地址，可在服务器上把 `deploy_to_server.py` 顶部的 `PREFECT_SERVER_URL` 改为 `"http://127.0.0.1:4200"`，再直接运行 `python deploy_to_server.py`（不设环境变量也可以）。

### 4. （可选）开机自启：用 systemd 跑 worker

在服务器上新建服务（例如 `/etc/systemd/system/prefect-workers.service`）：

```ini
[Unit]
Description=Prefect Workers (flow.serve on server)
After=network.target prefect.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/root/prefect
Environment=PREFECT_API_URL=http://127.0.0.1:4200/api
ExecStart=/root/prefect/venv/bin/python deploy_to_server.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

然后：

```bash
sudo systemctl daemon-reload
sudo systemctl enable prefect-workers
sudo systemctl start prefect-workers
sudo systemctl status prefect-workers
```

这样重启后也会在服务器上跑 flow，无需再手动执行 `deploy_to_server.py`。

---

## 四、本地之后怎么做

- **不再在本地长期运行** `deploy_to_server.py`（否则 flow 会在本地执行）。
- 代码更新后：把更新后的项目再同步到服务器（同步骤 1），然后**重启**服务器上的 worker（若用 systemd：`sudo systemctl restart prefect-workers`），新的 flow 代码就会在服务器上生效。
- 触发运行：浏览器打开 http://10.18.8.191:4200，在 UI 里对对应 deployment 点“Run”即可，执行会在 10.18.8.191 上完成。

---

## 五、小结

| 项目 | 说明 |
|------|------|
| 代码位置 | 整份 Prefect 项目（含 modules、utils、deploy_to_server.py 等）放在服务器，例如 `/root/prefect` |
| 依赖 | 在服务器 venv 里 `pip install -r requirements.txt`（及项目其它依赖） |
| 连哪个 API | 在服务器上必须用 `PREFECT_API_URL=http://127.0.0.1:4200/api`（连本机 Prefect Server） |
| 谁执行 flow | 在服务器上运行的 `deploy_to_server.py` 启动的 serve 进程 = 在 10.18.8.191 上执行 flow |
| 开机自启 | 用 systemd 服务跑 `python deploy_to_server.py`（如上 `prefect-workers.service`） |

按上述做完后，flow 就会在服务器 10.18.8.191 上执行，本地只负责同步代码和在浏览器里触发运行。
