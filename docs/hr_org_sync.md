# HR 组织架构每日同步

部署脚本注册的 `子流程-HR组织架构每日同步` 使用 `0 2 * * *`（`Asia/Shanghai`）计划，
每天 02:00 执行一次。它调用 FastAPI 项目中已有的
`AIPlatform/app/scripts/sync_hr_master_data.py`，更新管理员组织架构页使用的 HR 镜像表。

默认运行环境：

- 脚本：`/root/fastapi/AIPlatform/app/scripts/sync_hr_master_data.py`
- 环境文件：`/root/fastapi/AIPlatform/.env`
- Python：`/root/fastapi/AIPlatform/.venv/bin/python`
- HR 接口：`http://dc-api.xgd.com`

Prefect worker 需要能够读取 FastAPI 环境文件中的 `HR_SYNC_USERNAME` 和
`HR_SYNC_PASSWORD`。如不使用默认路径，可在 worker 环境中设置：

```text
HR_SYNC_APP_ROOT=/path/to/fastapi/AIPlatform
HR_SYNC_ENV_FILE=/path/to/fastapi/AIPlatform/.env
HR_SYNC_PYTHON=/path/to/fastapi/AIPlatform/.venv/bin/python
HR_SYNC_BASE_URL=http://dc-api.xgd.com
```

不要把账号密码写入 deployment parameters、代码或提交到 Git；flow 只从 worker
环境变量或指定环境文件读取，并不会把凭据写入 Prefect 参数。
