# 自动 git pull 与推送流程（systemd 配置）

实现：**每次 Prefect Server / Workers 启动或重启时，自动 git pull，Workers 再自动把流程推送到本机 Prefect**。

---

## 一、两个服务的分工

| 服务 | 作用 | 自动做的事 |
|------|------|------------|
| **prefect.service**（Server） | 提供 API & UI（`prefect server start`） | 启动前执行 `git pull`，保持代码最新 |
| **prefect-workers.service**（Workers） | 运行 `deploy_to_server.py`，向 Prefect 注册并执行 flow | 启动前 `git pull` → `pip install -r requirements.txt` → 运行 `deploy_to_server.py`（即推送流程） |

“推送流程”发生在 **Workers** 启动时；Server 只负责 API/UI，可选在启动前 git pull。

---

## 二、你当前的 Prefect Server unit 如何改

你原来的配置：

```ini
[Unit]
Description=Prefect Server (workflow API & UI)
After=network.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/root/prefect
ExecStart=/root/prefect/venv/bin/prefect server start --host 0.0.0.0
Restart=always
RestartSec=10
Environment=PREFECT_API_URL=http://10.18.8.191:4200/api

[Install]
WantedBy=multi-user.target
```

**只加一行**：在 `ExecStart=` 前面增加“启动前自动 git pull”：

```ini
ExecStartPre=-/usr/bin/git -C /root/prefect pull
ExecStart=/root/prefect/venv/bin/prefect server start --host 0.0.0.0
```

- `ExecStartPre=`：在启动 Server 之前执行。
- `-`：这条命令失败（例如网络问题）也不阻止 Server 启动。
- `/root/prefect` 若不是你的项目路径，改成实际路径。

完整示例见项目里的 **`prefect.service.example`**。

---

## 三、Workers：自动 git pull + 推送流程

Workers 才是“推送流程”的一方，建议在 **Workers** 的 unit 里做 git pull 和 pip install，再启动 `deploy_to_server.py`。

**prefect-workers.service** 示例（已写入 **`prefect-workers.service.example`**）：

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

# 启动前：拉取最新代码（失败则不启动，避免用旧代码推送）
ExecStartPre=/usr/bin/git -C /root/prefect pull
# 启动前：安装/更新依赖（- 表示失败不阻止启动）
ExecStartPre=-/root/prefect/venv/bin/pip install -r /root/prefect/requirements.txt -q

ExecStart=/root/prefect/venv/bin/python deploy_to_server.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

部署步骤：

```bash
sudo cp /root/prefect/prefect-workers.service.example /etc/systemd/system/prefect-workers.service
sudo systemctl daemon-reload
sudo systemctl enable prefect-workers
sudo systemctl start prefect-workers
```

之后每次 **Workers 启动或重启**（包括 `systemctl restart prefect-workers`）都会：

1. 在 `/root/prefect` 执行 `git pull`
2. 执行 `pip install -r requirements.txt`
3. 运行 `deploy_to_server.py`，把流程推送到本机 Prefect 并保持 workers 运行。

---

## 四、可选：用脚本统一“更新 + 推送”

若希望用一条脚本完成“git pull + 安装依赖 + 推送流程”（不依赖 systemd 的 ExecStartPre），可用 **`scripts/update_and_serve.sh`**：

```bash
chmod +x /root/prefect/scripts/update_and_serve.sh
/root/prefect/scripts/update_and_serve.sh
```

或把 Workers 的 `ExecStart` 改成执行该脚本：

```ini
ExecStart=/bin/bash /root/prefect/scripts/update_and_serve.sh
```

（此时可去掉 Workers unit 里的两条 `ExecStartPre`。）

---

## 五、小结

| 需求 | 做法 |
|------|------|
| Server 启动时自动 git pull | 在 prefect.service 里加一行：`ExecStartPre=-/usr/bin/git -C /root/prefect pull` |
| Workers 启动时自动 git pull + 推送流程 | 使用 `prefect-workers.service.example`（内含 ExecStartPre git pull、pip install，ExecStart 为 deploy_to_server.py） |
| 路径不是 /root/prefect | 把上述 `/root/prefect` 和 unit 里的 WorkingDirectory 改成你的项目路径 |

按上述修改后，**重启 Workers**（或重启整机后由 systemd 自动拉代码并启动）即会拉取最新代码并把流程推送到服务器上的 Prefect。
