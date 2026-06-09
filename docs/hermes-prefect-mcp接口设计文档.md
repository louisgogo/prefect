# Hermes-Prefect MCP 接口设计文档

## 1. 概述

本文档定义 Hermes（AI Agent 系统）与 Prefect 工作流编排平台的对接规范。

- **Prefect Server 地址**: `http://10.18.8.191:4200`
- **Prefect API 地址**: `http://10.18.8.191:4200/api`
- **通信协议**: HTTP REST + SSE（Server-Sent Events）
- **数据格式**: JSON

---

## 2. 认证方式

Prefect 2.x 开源版默认**无认证**（若启用 Basic Auth 则按实际配置）。

```
# 默认请求头
Content-Type: application/json
```

若后续启用 API Key：
```
Authorization: Bearer <PREFECT_API_KEY>
```

---

## 3. 核心接口清单

### 3.1 列出所有 Deployments

**用途**: Hermes 向用户展示当前可用的流程列表。

```http
GET /api/deployments/filter
```

**请求体**:
```json
{
  "sort": "CREATED_DESC",
  "limit": 100,
  "offset": 0
}
```

**响应**:
```json
[
  {
    "id": "uuid",
    "name": "主流程-业务线损益计算",
    "flow_id": "uuid",
    "tags": ["业务线核算", "月度任务"],
    "parameters": {"year": 2025, "months": [1,2,3]},
    "description": "业务线损益计算流程...",
    "is_schedule_active": true,
    "created": "2025-01-01T00:00:00Z"
  }
]
```

**关键字段说明**:
| 字段 | 说明 |
|------|------|
| `id` | Deployment UUID，触发流程时需要 |
| `name` | 展示名称 |
| `parameters` | 默认参数，触发时可覆盖 |
| `tags` | 分类标签 |

---

### 3.2 获取单个 Deployment 详情

**用途**: 查看某个流程的详细参数定义。

```http
POST /api/deployments/{deployment_id}
```

**响应**: 同 3.1 的单条数据结构，额外包含 `parameter_openapi_schema`（参数校验 schema）。

---

### 3.3 触发 Flow Run（同步等待/异步触发）

**用途**: Hermes 代表用户执行某个流程。

```http
POST /api/deployments/{deployment_id}/create_flow_run
```

**请求体**:
```json
{
  "name": "hermes-triggered-20250609",
  "parameters": {
    "year": 2025,
    "month": 5
  },
  "tags": ["hermes", "user-request"],
  "idempotency_key": "unique-key-from-hermes"
}
```

**响应**:
```json
{
  "id": "da4bad4b-f3f3-48f3-b912-abb145f6930e",
  "flow_id": "uuid",
  "deployment_id": "uuid",
  "name": "hermes-triggered-20250609",
  "state": {
    "id": "uuid",
    "type": "SCHEDULED",
    "name": "Scheduled",
    "message": null
  },
  "parameters": {"year": 2025, "month": 5},
  "tags": ["hermes", "user-request"],
  "created": "2026-06-09T09:30:00Z"
}
```

**必须保存的字段**: `id`（flow_run_id，后续查询状态和回调都需要）。

---

### 3.4 查询 Flow Run 状态

**用途**: 轮询或回调后确认流程当前状态。

```http
GET /api/flow_runs/{flow_run_id}
```

**响应**:
```json
{
  "id": "da4bad4b-f3f3-48f3-b912-abb145f6930e",
  "name": "hermes-triggered-20250609",
  "state": {
    "type": "COMPLETED",
    "name": "Completed",
    "message": "Finished in state Completed()",
    "timestamp": "2026-06-09T09:38:46Z"
  },
  "flow_version": "1.0",
  "start_time": "2026-06-09T09:30:43Z",
  "end_time": "2026-06-09T09:38:46Z",
  "total_run_time": 483.0,
  "estimated_run_time": 483.0
}
```

**状态机**:
```
SCHEDULED -> RUNNING -> COMPLETED
                      -> FAILED
                      -> CANCELLED
                      -> CRASHED
```

---

### 3.5 获取 Flow Run 日志

**用途**: 向用户展示执行详情或排查问题。

```http
POST /api/logs/filter
```

**请求体**:
```json
{
  "logs": {
    "flow_run_id": {
      "any_": ["da4bad4b-f3f3-48f3-b912-abb145f6930e"]
    }
  },
  "sort": "TIMESTAMP_ASC",
  "limit": 500
}
```

**响应**:
```json
[
  {
    "id": "uuid",
    "created": "2026-06-09T09:38:46Z",
    "updated": "2026-06-09T09:38:46Z",
    "name": "save_recon_results",
    "level": 20,
    "message": "--> Excel 已导出至: /mnt/xgd_share/...",
    "timestamp": "2026-06-09T09:38:46Z",
    "flow_run_id": "da4bad4b-f3f3-48f3-b912-abb145f6930e",
    "task_run_id": "6eb...",
    "logger_name": "prefect.task_runs"
  }
]
```

**日志级别说明**: `10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL`

---

### 3.6 获取 Task Runs 列表

**用途**: 查看流程中各任务的执行状态。

```http
POST /api/task_runs/filter
```

**请求体**:
```json
{
  "task_runs": {
    "flow_run_id": {
      "any_": ["da4bad4b-f3f3-48f3-b912-abb145f6930e"]
    }
  },
  "sort": "EXPECTED_START_TIME_ASC"
}
```

**响应**:
```json
[
  {
    "id": "uuid",
    "name": "save_recon_results-6eb",
    "state": {"type": "COMPLETED", "name": "Completed"},
    "expected_start_time": "2026-06-09T09:30:46Z",
    "start_time": "2026-06-09T09:30:46Z",
    "end_time": "2026-06-09T09:38:46Z",
    "total_run_time": 480.0
  }
]
```

---

### 3.7 取消 Flow Run

**用途**: 用户要求停止正在执行的流程。

```http
POST /api/flow_runs/{flow_run_id}/set_state
```

**请求体**:
```json
{
  "state": {
    "type": "CANCELLED",
    "name": "Cancelled",
    "message": "Cancelled by Hermes user request"
  },
  "force": true
}
```

---

### 3.8 列出最近执行的 Flow Runs

**用途**: 查看历史执行记录。

```http
POST /api/flow_runs/filter
```

**请求体**:
```json
{
  "flow_runs": {},
  "sort": "START_TIME_DESC",
  "limit": 20
}
```

可按 deployment 过滤：
```json
{
  "flow_runs": {
    "deployment_id": {"any_": ["deployment-uuid"]}
  },
  "sort": "START_TIME_DESC",
  "limit": 10
}
```

---

## 4. 回调方案设计（Prefect -> Hermes）

Prefect 开源版没有内置 Webhook 通知机制，因此采用以下方案：

### 方案 A：轮询 + SSE 推送（推荐）

**架构**:
```
Hermes 触发 Flow -> 保存 flow_run_id -> 后台轮询 Prefect API
                              |
                     状态变更时通过 SSE 推送给前端/用户
```

**实现步骤**:
1. Hermes 调用 `POST /api/deployments/{id}/create_flow_run`
2. 保存返回的 `flow_run_id`
3. Hermes 后台任务每 5-10 秒调用 `GET /api/flow_runs/{flow_run_id}`
4. 检测到状态变为 `COMPLETED` / `FAILED` / `CRASHED` / `CANCELLED` 时停止轮询
5. 若完成，再调用 `POST /api/logs/filter` 获取关键日志反馈给用户

**轮询终止条件**:
- 状态为终态（`COMPLETED`, `FAILED`, `CRASHED`, `CANCELLED`）
- 轮询超过最大超时（如 30 分钟）

**伪代码**:
```python
async def poll_flow_run(flow_run_id: str, callback_fn):
    while True:
        run = await get_flow_run(flow_run_id)
        state = run["state"]["type"]

        if state in ("COMPLETED", "FAILED", "CRASHED", "CANCELLED"):
            logs = await get_flow_logs(flow_run_id)
            callback_fn(state=state, logs=logs, run=run)
            break

        await asyncio.sleep(5)
```

---

### 方案 B：Flow 内嵌 Webhook Task（侵入式）

在 Prefect flow 代码中，于开始和结束位置添加回调 Task：

```python
import httpx
from prefect import task

@task
async def notify_hermes(event: str, payload: dict):
    """通知 Hermes 系统"""
    async with httpx.AsyncClient() as client:
        await client.post(
            "https://hermes.example.com/api/callbacks/prefect",
            json={
                "event": event,          # "started" | "completed" | "failed"
                "flow_run_id": payload["flow_run_id"],
                "deployment_name": payload["deployment_name"],
                "result": payload.get("result"),
                "logs_summary": payload.get("logs_summary")
            },
            headers={"Authorization": "Bearer <HERMES_CALLBACK_SECRET>"},
            timeout=30.0
        )
```

**优点**: 实时性好，无需轮询。
**缺点**: 需要修改每个 flow 的代码；若 Hermes 不可达，回调丢失。

**适用场景**: 对实时性要求极高的场景，且能接受维护 flow 代码中的回调逻辑。

---

### 方案 C：数据库事件表（可靠但延迟）

Hermes 在触发 flow 时，在共享数据库中写入一条"执行请求"记录：

```sql
CREATE TABLE hermes_flow_requests (
    id SERIAL PRIMARY KEY,
    flow_run_id UUID,
    deployment_name VARCHAR,
    status VARCHAR DEFAULT 'pending',
    callback_url VARCHAR,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

然后在 Prefect flow 的最后一步，更新此表状态，Hermes 通过监听数据库变更（或轮询）获取结果。

**优点**: 可靠性高，不依赖网络回调可达性。
**缺点**: 需要 Prefect 和 Hermes 共用数据库或数据库可互相访问。

---

## 5. 错误处理规范

### 5.1 Prefect API 错误码

| HTTP 状态 | 场景 | Hermes 处理建议 |
|-----------|------|----------------|
| 404 | Deployment 不存在 | 提示用户流程名称可能已变更，建议刷新列表 |
| 409 | Idempotency Key 冲突 | 直接返回已存在的 flow_run_id |
| 422 | 参数校验失败 | 提取 `detail` 中的字段错误，提示用户修正参数 |
| 500 | Prefect Server 内部错误 | 重试一次，仍失败则通知管理员 |
| 503 | Prefect Server 未启动 | 提示用户服务暂时不可用 |

### 5.2 Flow Run 失败处理

当 `state.type` 为 `FAILED` 或 `CRASHED`：
1. 调用 `POST /api/logs/filter` 获取 ERROR/WARNING 级别日志
2. 提取最后 20 条日志作为错误摘要
3. 向用户展示摘要，并提供"重新执行"选项

---

## 6. 当前可用 Deployment 清单（参考）

基于 `deploy_to_server.py`，当前注册的 deployments 如下：

| Deployment Name | 功能 | 典型参数 | 执行时长 |
|----------------|------|---------|---------|
| `主流程-业务线损益计算` | 核心损益计算 | `year`, `months` | ~10-30 min |
| `主流程-综合比例年底重算` | 综合比例计算 | `year`, `month` | ~5-10 min |
| `主流程-数据导入` | Excel 数据导入 | `year`, `month`, `replace_existing` | ~5 min |
| `主流程-AI数据ETL` | AI 平台数据视图 | `type` | ~5 min |
| `主流程-预算更新` | FONE 预算拉取 | `report_date`, `version` | ~5 min |
| `主流程-往来对账` | 内部往来对账 | （自动取上月） | ~5-10 min |
| `主流程-业务线Staging抽取` | EAV 格式抽取 | （自动取上月） | ~5 min |
| `主流程-数据库视图更新` | 中文视图刷新 | `skip_fone_grant` | ~2 min |
| `子流程-利润表刷新` | 利润表重算 | （无参，处理所有月） | ~10 min |
| `子流程-拉取预算综合比例` | 预算比例同步 | （自动日期） | ~2 min |
| `子流程-FONE往来对账` | FONE API 对账 | `year`, `month` | ~5 min |
| `子流程-利润表收集汇总` | 报表收集 | （默认上月） | ~5 min |

**注意**: 实际部署名称以 Prefect UI 或 API 返回为准。

---

## 7. 示例：完整交互流程

### 7.1 用户说"帮我执行上个月的往来对账"

```
1. Hermes 调用 POST /api/deployments/filter
   -> 找到 name="主流程-往来对账" 的 deployment_id

2. Hermes 调用 POST /api/deployments/{id}/create_flow_run
   -> 参数可留空（flow 会自动取上月）
   -> 获得 flow_run_id = "abc-123"

3. Hermes 回复用户：
   "已启动往来对账流程（Run ID: abc-123），预计 5-10 分钟完成。"

4. Hermes 后台轮询 GET /api/flow_runs/abc-123

5. 状态变为 COMPLETED 后：
   -> 调用 POST /api/logs/filter 获取关键日志
   -> 提取 "--> Excel 已导出至: ..." 等摘要
   -> 回复用户："往来对账完成，结果已导出至 /mnt/xgd_share/..."
```

### 7.2 用户说"查看最近的业务线损益计算结果"

```
1. Hermes 调用 POST /api/flow_runs/filter
   -> 按 deployment_name 过滤 "主流程-业务线损益计算"
   -> 取最近 3 条记录

2. Hermes 展示：
   - 2026-06-09 09:00: COMPLETED（耗时 15 分钟）
   - 2026-06-08 09:00: FAILED（参数错误，year 缺失）
   - 2026-06-07 09:00: COMPLETED（耗时 12 分钟）

3. 用户可进一步选择某条查看详细日志
```

---

## 8. 附录：OpenAPI 文档获取

Prefect 自带完整的 OpenAPI/Swagger 文档：

```
http://10.18.8.191:4200/docs    # Swagger UI
http://10.18.8.191:4200/redoc   # ReDoc
http://10.18.8.191:4200/openapi.json  # OpenAPI JSON
```

可通过 `openapi.json` 自动生成客户端 SDK。
