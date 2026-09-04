"""
Prefect MCP Callback Helper — 供 Prefect Flow 内嵌使用的通用 Webhook Task。

用法：
    1. 将此文件复制到你的 Prefect 项目目录
    2. 在 Flow 中导入并调用 notify_hermes / hermes_flow_started / hermes_flow_completed / hermes_flow_failed

环境变量（可选）：
    HERMES_CALLBACK_URL   — MCP Server 回调地址
    HERMES_CALLBACK_SECRET — 回调密钥

回调 Payload 格式（与 Hermes 接口设计文档对齐）：
    {
      "event": "started|completed|failed",
      "flow_run_id": "...",
      "flow_run_name": "主流程-往来对账",
      "deployment_id": "...",
      "flow_name": "往来对账",
      "timestamp": "2026-06-09T09:38:46Z",
      "payload": {"output_path": "...", "summary": "..."},
      "logs": [{"timestamp": "...", "level": "INFO", "message": "..."}]
    }
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any

import httpx

# 若安装了 prefect，则使用其 @task 装饰器；否则提供一个无操作回退
try:
    from prefect import task
except ImportError:  # pragma: no cover

    def task(fn=None, **kwargs):
        if fn is None:
            return lambda f: f
        return fn


_DEFAULT_CALLBACK_URL = os.getenv("HERMES_CALLBACK_URL", "http://10.18.8.191:8002/callback/prefect")
_DEFAULT_SECRET = os.getenv("HERMES_CALLBACK_SECRET", "")
_TIMEOUT = 30.0


def _get_prefect_context() -> dict[str, Any]:
    """尝试从 Prefect 运行时上下文自动提取元数据。"""
    ctx_info: dict[str, Any] = {}
    try:
        from prefect.context import FlowRunContext

        ctx = FlowRunContext.get()
        if ctx and ctx.flow_run:
            fr = ctx.flow_run
            ctx_info["flow_run_id"] = str(fr.id)
            ctx_info["flow_run_name"] = fr.name
            if fr.deployment_id:
                ctx_info["deployment_id"] = str(fr.deployment_id)
            if hasattr(fr, "flow_id") and fr.flow_id:
                ctx_info["flow_name"] = getattr(fr, "flow_name", str(fr.flow_id))
    except Exception:
        pass
    return ctx_info


@task(name="notify_hermes", retries=2, retry_delay_seconds=5, log_prints=True)
async def notify_hermes(
    event: str,
    flow_run_id: str | None = None,
    flow_run_name: str | None = None,
    deployment_id: str | None = None,
    flow_name: str | None = None,
    payload: dict[str, Any] | None = None,
    logs: list[dict] | None = None,
    callback_url: str | None = None,
    secret: str | None = None,
) -> dict[str, Any]:
    """
    向 Hermes MCP Server 发送回调通知。

    Args:
        event: 事件类型，如 "started" / "completed" / "failed"
        flow_run_id: 当前 Flow Run 的 UUID。若留空，会尝试从 Prefect 上下文自动获取
        flow_run_name: Flow Run 名称
        deployment_id: Deployment UUID
        flow_name: Flow 名称
        payload: 业务结果字典（如 {"output_path": "...", "summary": "..."}）
        logs: 日志列表（如 [{"timestamp": "...", "level": "INFO", "message": "..."}]）
        callback_url: MCP Server 回调地址
        secret: 回调密钥
    """
    url = (callback_url or _DEFAULT_CALLBACK_URL).rstrip("/")
    sec = secret if secret is not None else _DEFAULT_SECRET

    # 自动补全 Prefect 上下文
    ctx = _get_prefect_context()
    flow_run_id = flow_run_id or ctx.get("flow_run_id")
    flow_run_name = flow_run_name or ctx.get("flow_run_name")
    deployment_id = deployment_id or ctx.get("deployment_id")
    flow_name = flow_name or ctx.get("flow_name")

    body = {
        "event": event,
        "flow_run_id": flow_run_id,
        "flow_run_name": flow_run_name,
        "deployment_id": deployment_id,
        "flow_name": flow_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload or {},
        "logs": logs or [],
    }

    headers = {"Content-Type": "application/json"}
    if sec:
        headers["x-callback-secret"] = sec

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=body, headers=headers, timeout=_TIMEOUT)
            resp.raise_for_status()
            return {"status": "ok", "response": resp.json()}
    except httpx.HTTPStatusError as e:
        print(
            f"[HermesCallback] HTTP error {e.response.status_code}: {e.response.text}",
            file=sys.stderr,
        )
        return {"status": "error", "detail": f"HTTP {e.response.status_code}: {e.response.text}"}
    except Exception as e:
        print(f"[HermesCallback] Exception: {e}", file=sys.stderr)
        return {"status": "error", "detail": str(e)}


# ── 便捷的封装函数（可选）──────────────────────────────────────────────────


@task(name="hermes_flow_started", retries=1, retry_delay_seconds=3, log_prints=True)
async def hermes_flow_started(
    logs: list[dict] | None = None,
    callback_url: str | None = None,
    secret: str | None = None,
) -> dict[str, Any]:
    """在 Flow 开始时调用，通知 Hermes 流程已启动。"""
    return await notify_hermes(
        event="started",
        payload={},
        logs=logs or [],
        callback_url=callback_url,
        secret=secret,
    )


@task(name="hermes_flow_completed", retries=2, retry_delay_seconds=5, log_prints=True)
async def hermes_flow_completed(
    payload: dict[str, Any] | None = None,
    logs: list[dict] | None = None,
    callback_url: str | None = None,
    secret: str | None = None,
) -> dict[str, Any]:
    """在 Flow 成功结束时调用，通知 Hermes 流程已完成并带回结果。"""
    return await notify_hermes(
        event="completed",
        payload=payload or {},
        logs=logs or [],
        callback_url=callback_url,
        secret=secret,
    )


@task(name="hermes_flow_failed", retries=2, retry_delay_seconds=5, log_prints=True)
async def hermes_flow_failed(
    error_message: str | None = None,
    error_type: str | None = None,
    payload: dict[str, Any] | None = None,
    logs: list[dict] | None = None,
    callback_url: str | None = None,
    secret: str | None = None,
) -> dict[str, Any]:
    """在 Flow 失败时调用，通知 Hermes 流程执行失败。"""
    merged_payload = dict(payload or {})
    if error_message:
        merged_payload["error"] = error_message
    if error_type:
        merged_payload["error_type"] = error_type
    return await notify_hermes(
        event="failed",
        payload=merged_payload,
        logs=logs or [],
        callback_url=callback_url,
        secret=secret,
    )
