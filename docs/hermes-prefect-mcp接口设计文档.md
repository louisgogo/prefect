# Hermes-Prefect 对接文档（Webhook 推送模式）

## 1. 概述

本文档定义 Hermes（AI Agent 系统）与 Prefect 工作流编排平台的对接规范。

**当前主推方案：Webhook 推送模式**
- Prefect flow 执行完成后，主动通过 HTTP POST 将结果和日志推送到 Hermes
- Hermes 只需提供一个接收端点，无需轮询 Prefect API
- 用户触发流程后，Hermes 可**秒级回复**"已启动"，结果异步送达

**环境信息**:
- **Prefect Server**: `http://10.18.8.191:4200`
- **Prefect API**: `http://10.18.8.191:4200/api`
- **数据格式**: JSON

---

## 2. Hermes 需要做什么

### 2.1 提供 Webhook 接收端点

Hermes 需要暴露一个 HTTP POST 接口，供 Prefect 调用：

```
POST https://hermes.your-domain.com/api/callbacks/prefect
```

**请求头**:
```
Content-Type: application/json
Authorization: Bearer <HERMES_WEBHOOK_SECRET>   # 若配置了密钥
```

**请求体示例（completed 事件）**:
```json
{
  "event": "completed",
  "flow_run_id": "da4bad4b-f3f3-48f3-b912-abb145f6930e",
  "flow_run_name": "主流程-往来对账",
  "deployment_id": "uuid",
  "flow_name": "往来对账",
  "timestamp": "2026-06-09T09:38:46Z",
  "payload": {
    "target_date": "2026-05-01",
    "output_path": "/mnt/xgd_share/10-内部往来对账/5-对账结果/核对结果_202605.xlsx",
    "wanglai_count": 42,
    "transaction_count": 0,
    "cashflow_count": 3,
    "summary": "往来差异 42 条，销售/采购差异 0 条，现金流差异 3 条"
  },
  "logs": [
    {"timestamp": "2026-06-09T09:30:43Z", "level": "INFO", "message": "往来对账流程启动，目标月份: 2026-05-01"},
    {"timestamp": "2026-06-09T09:38:45Z", "level": "INFO", "message": "【阶段2】完成对账核对..."},
    {"timestamp": "2026-06-09T09:38:46Z", "level": "WARN", "message": "[WARN] 写入 recon_result_wanglai 失败: ..."},
    {"timestamp": "2026-06-09T09:38:46Z", "level": "INFO", "message": "--> Excel 已导出至: /mnt/xgd_share/..."}
  ]
}
```

**请求体示例（failed 事件）**:
```json
{
  "event": "failed",
  "flow_run_id": "da4bad4b-f3f3-48f3-b912-abb145f6930e",
  "flow_run_name": "主流程-往来对账",
  "deployment_id": "uuid",
  "flow_name": "往来对账",
  "timestamp": "2026-06-09T09:30:00Z",
  "payload": {
    "target_date": "2026-05-01",
    "error": "阶段1失败，写库错误: connection timeout",
    "error_type": "RuntimeError"
  },
  "logs": [
    {"timestamp": "2026-06-09T09:30:43Z", "level": "INFO", "message": "往来对账流程启动..."},
    {"timestamp": "2026-06-09T09:31:05Z", "level": "ERROR", "message": "insert_recon_data_task 失败: connection timeout"}
  ]
}
```

**请求体示例（started 事件）**:
```json
{
  "event": "started",
  "flow_run_id": "da4bad4b-f3f3-48f3-b912-abb145f6930e",
  "flow_run_name": "主流程-往来对账",
  "deployment_id": "uuid",
  "flow_name": "往来对账",
  "timestamp": "2026-06-09T09:30:00Z",
  "payload": {},
  "logs": []
}
```

### 2.2 Hermes 端处理逻辑建议

```python
@app.post("/api/callbacks/prefect")
async def handle_prefect_webhook(request: Request):
    body = await request.json()
    event = body["event"]
    flow_run_id = body["flow_run_id"]
    flow_name = body["flow_name"]
    payload = body.get("payload", {})
    logs = body.get("logs", [])

    if event == "started":
        # 可选：记录"正在执行中"状态
        update_task_status(flow_run_id, status="running")

    elif event == "completed":
        summary = payload.get("summary", "流程执行完成")
        output_path = payload.get("output_path")
        # 提取关键日志中的 WARN/ERROR 提示用户
        warnings = [log for log in logs if log["level"] in ("WARN", "ERROR")]
        message = f"{flow_name} 执行完成\n{summary}"
        if output_path:
            message += f"\n导出文件: {output_path}"
        if warnings:
            message += f"\n注意: 执行过程中有 {len(warnings)} 条警告"
        push_to_user(flow_run_id, message)
        update_task_status(flow_run_id, status="completed", result=payload)

    elif event == "failed":
        error = payload.get("error", "未知错误")
        # 从日志中提取 ERROR 级别的最后一条
        error_logs = [log for log in logs if log["level"] == "ERROR"]
        last_error = error_logs[-1]["message"] if error_logs else error
        message = f"{flow_name} 执行失败\n错误: {error}\n最后异常: {last_error}"
        push_to_user(flow_run_id, message)
        update_task_status(flow_run_id, status="failed", error=error)

    return {"status": "ok"}
```

### 2.3 配置对接

将以下信息提供给 Prefect 维护方：

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `HERMES_WEBHOOK_URL` | Hermes 接收端点 | `https://hermes.internal/api/callbacks/prefect` |
| `HERMES_WEBHOOK_SECRET` | 认证密钥（可选） | `prefect-secret-2026` |

Prefect 维护方将在服务器环境变量中配置，重启后生效。

---

## 3. 已接入 Webhook 的流程清单

| Deployment Name | 功能 | 典型 payload 字段 | webhook 状态 |
|----------------|------|------------------|-------------|
| `主流程-往来对账` | 内部往来对账 | `output_path`, `wanglai_count`, `transaction_count`, `cashflow_count`, `summary` | **已接入** |
| 其他流程 | ... | ... | 暂未接入 |

> 当前仅 `recon_flow`（往来对账）作为试点接入了 webhook。其他流程如需接入，请联系 Prefect 维护方。

---

## 4. 完整交互流程示例

### 4.1 用户说"帮我执行上个月的往来对账"

```
1. Hermes 调用 POST /api/deployments/filter
   -> 找到 name="主流程-往来对账" 的 deployment_id

2. Hermes 调用 POST /api/deployments/{id}/create_flow_run
   -> 参数可留空（flow 会自动取上月）
   -> 获得 flow_run_id = "abc-123"

3. Hermes **秒级回复用户**：
   "已启动往来对账流程（Run ID: abc-123），完成后会通知您。"

4. [5-10 分钟后]
   Prefect 执行 completed -> POST Hermes webhook
   -> Hermes 收到 payload + logs
   -> Hermes 主动推送给用户：
   "往来对账执行完成\n往来差异 42 条...\n导出文件: /mnt/xgd_share/..."
```

### 4.2 用户说"往来对账执行得怎么样了？"

若 webhook 未收到（或用户中途询问）：

```
1. Hermes 查询本地记录的 flow_run_id（如 "abc-123"）
2. 若本地无记录，可调用 GET /api/flow_runs/abc-123 查询 Prefect
3. 回复用户："当前状态: RUNNING，已运行 3 分钟"
```

---

## 5. Prefect API 参考（辅助查询）

Webhook 是主通道，但以下 API 仍可供 Hermes 在需要时调用：

### 5.1 列出 Deployments

```http
POST /api/deployments/filter
```

用于向用户展示"当前可以执行哪些流程"。

### 5.2 触发 Flow Run

```http
POST /api/deployments/{deployment_id}/create_flow_run
```

```json
{
  "name": "hermes-triggered-20250609",
  "parameters": {"year": 2025, "month": 5},
  "tags": ["hermes", "user-request"]
}
```

**响应中的 `id` 字段必须保存**，即 `flow_run_id`。

### 5.3 查询 Flow Run 状态（兜底查询）

```http
GET /api/flow_runs/{flow_run_id}
```

状态值：`SCHEDULED` / `RUNNING` / `COMPLETED` / `FAILED` / `CANCELLED` / `CRASHED`

### 5.4 取消 Flow Run

```http
POST /api/flow_runs/{flow_run_id}/set_state
```

```json
{
  "state": {"type": "CANCELLED", "name": "Cancelled"},
  "force": true
}
```

### 5.5 获取日志（如 webhook 中日志不够详细）

```http
POST /api/logs/filter
```

```json
{
  "logs": {"flow_run_id": {"any_": ["abc-123"]}},
  "sort": "TIMESTAMP_ASC",
  "limit": 500
}
```

---

## 6. 错误处理

### 6.1 Prefect API 错误

| HTTP 状态 | 场景 | Hermes 处理 |
|-----------|------|------------|
| 404 | Deployment 不存在 | 提示用户流程名称可能已变更 |
| 422 | 参数校验失败 | 提取字段错误，提示用户修正 |
| 500 | Prefect Server 错误 | 重试一次，仍失败则通知管理员 |

### 6.2 Webhook 未到达

若 Prefect 执行完成后 Hermes 未收到 webhook：
1. 检查 Hermes 端点是否可达（Prefect 日志中会记录通知失败）
2. 使用 `GET /api/flow_runs/{flow_run_id}` 手动查询状态兜底
3. 检查 `HERMES_WEBHOOK_URL` 环境变量是否配置正确

### 6.3 Flow 执行失败

当收到 `event: failed`：
1. 从 `payload.error` 获取错误类型
2. 从 `logs` 中筛选 `level: ERROR` 的日志作为详情
3. 向用户展示错误摘要，提供"重新执行"选项

---

## 7. 附录

### 7.1 Prefect OpenAPI 文档

```
http://10.18.8.191:4200/docs    # Swagger UI
http://10.18.8.191:4200/openapi.json  # OpenAPI JSON（可自动生成客户端）
```

### 7.2 日志级别对照

| 数值 | 字符串 | 含义 |
|------|--------|------|
| 10 | DEBUG | 调试信息 |
| 20 | INFO | 普通信息 |
| 30 | WARN | 警告（不影响流程，但需关注） |
| 40 | ERROR | 错误（可能导致结果不完整） |
| 50 | CRITICAL | 严重错误 |

Webhook 中附带的是 INFO 及以上级别（>=20）的日志，按时间正序排列。
