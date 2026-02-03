# Git 同步后，在服务器上如何将流程推送到 Prefect

你用 Git 把代码同步到服务器后，需要在**服务器上**运行一次「部署/worker」脚本，才能把最新流程**推送到**该机器上的 Prefect Server，并在服务器上执行 flow。

---

## 一、原理简述

| 动作 | 说明 |
|------|------|
| **Git 同步** | 只是把代码更新到服务器（例如 `git pull`），Prefect 还不知道新流程。 |
| **推送到 Prefect** | 在服务器上执行 `deploy_to_server.py`（或 `run_workers_on_server.sh`），脚本里的 `flow.serve()` 会向**本机** Prefect Server 注册 deployment，并启动 worker 进程，之后在 UI 里点的「运行」会在服务器上执行。 |

所以：**Git 负责代码同步，在服务器上跑部署脚本 = 把流程推送到服务器上的 Prefect。**

---

## 二、在服务器上的操作步骤

假设项目在服务器上的目录是 `/root/prefect`（或你 clone 的路径）。

### 方式 A：每次 Git 拉取后手动推送

```bash
# 1. 进入项目目录
cd /root/prefect

# 2. 拉取最新代码（你已用 Git 做同步）
git pull

# 3. 若依赖有变更，可更新虚拟环境
source venv/bin/activate
pip install -r requirements.txt

# 4. 将流程推送到本机 Prefect，并启动 workers（连本机 127.0.0.1:4200）
export PREFECT_API_URL=http://127.0.0.1:4200/api
python deploy_to_server.py
```

或用封装好的脚本（脚本里已设置 `PREFECT_API_URL` 和 venv）：

```bash
cd /root/prefect
git pull
./run_workers_on_server.sh
```

`deploy_to_server.py` 会**持续运行**（三个 flow 的 serve 进程），流程已推送到本机 Prefect，在浏览器打开 `http://服务器IP:4200` 即可触发运行。需要停止时按 Ctrl+C。

---

### 方式 B：用 systemd 常驻 workers，Git 拉取后重启即可

若已按 `prefect-workers.service.example` 配置了 systemd 服务：

1. **日常更新代码后，只需重启服务，即完成「推送」：**

   ```bash
   cd /root/prefect
   git pull
   # 如有依赖变更
   source venv/bin/activate && pip install -r requirements.txt
   # 重启 workers，新代码会重新向 Prefect 注册并执行
   sudo systemctl restart prefect-workers
   ```

2. **服务里已经写好了**  
   - `WorkingDirectory=/root/prefect`  
   - `Environment=PREFECT_API_URL=http://127.0.0.1:4200/api`  
   - `ExecStart=.../venv/bin/python deploy_to_server.py`  

所以重启后，流程会再次推送到本机 Prefect，并在服务器上执行。

---

## 三、小结

| 步骤 | 在哪儿做 | 做什么 |
|------|----------|--------|
| 1 | 服务器 | `cd /root/prefect` → `git pull`（代码已同步） |
| 2 | 服务器 | （可选）`pip install -r requirements.txt` |
| 3 | 服务器 | **推送流程到 Prefect**：`./run_workers_on_server.sh` 或 `python deploy_to_server.py`（需 `PREFECT_API_URL=http://127.0.0.1:4200/api`） |
| 4 | 若用 systemd | Git 拉取后执行 `sudo systemctl restart prefect-workers` 即完成推送 |

要点：**在服务器上运行部署脚本（或重启 prefect-workers），就是把当前代码里的流程推送到服务器上的 Prefect，并在该机上执行。**
