"""Hermes / auto-mcp callback 通知模块

为 Prefect flow 提供统一的回调能力。flow 执行结果会 POST 到 auto-mcp
的 /callback/prefect 端点，由 auto-mcp 转发给对应助手。

环境变量:
    HERMES_CALLBACK_URL: callback 接收地址（优先）
    HERMES_WEBHOOK_URL: 兼容旧名，同上
    HERMES_CALLBACK_SECRET: 可选认证密钥，非空时会在请求头带 x-callback-secret

使用方式:
    1. 在 flow 开头调用 notify_hermes_task(event="started", ...)
    2. 在 flow 结尾调用 notify_hermes_task(event="completed", payload={...})
    3. 失败时自动触发 event="failed"

示例:
    from modules.common.tasks.notify_hermes_task import notify_hermes_task

    @flow(name="主流程-往来对账")
    def recon_flow():
        notify_hermes_task(event="started", flow_name="往来对账")
        try:
            ... # 原有业务逻辑
            notify_hermes_task(
                event="completed",
                flow_name="往来对账",
                payload={"excel_path": "/mnt/xgd_share/...", "summary": "..."},
            )
        except Exception as e:
            notify_hermes_task(event="failed", flow_name="往来对账", payload={"error": str(e)})
            raise
"""

import os
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from prefect import get_run_logger, task
from prefect.context import FlowRunContext

DEFAULT_CALLBACK_URL = os.environ.get(
    "HERMES_CALLBACK_URL",
    os.environ.get("HERMES_WEBHOOK_URL", "http://10.19.9.191:8002/callback/prefect"),
)
DEFAULT_CALLBACK_SECRET = os.environ.get("HERMES_CALLBACK_SECRET", "")
DEFAULT_PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://127.0.0.1:4200/api")
_TIMEOUT = 30.0

_LEVEL_MAP = {10: "DEBUG", 20: "INFO", 30: "WARN", 40: "ERROR", 50: "CRITICAL"}


def _fetch_run_logs(run_id: str, api_url: str, limit: int = 30) -> list[dict[str, Any]]:
    """从 Prefect API 拉取当前 flow run 的日志摘要"""
    try:
        resp = httpx.post(
            f"{api_url}/logs/filter",
            json={
                "logs": {"flow_run_id": {"any_": [run_id]}},
                "sort": "TIMESTAMP_DESC",
                "limit": limit,
            },
            timeout=10.0,
        )
        resp.raise_for_status()
        logs = resp.json()
        logs.reverse()
        return [
            {
                "timestamp": log.get("timestamp", ""),
                "level": _LEVEL_MAP.get(log.get("level", 20), "INFO"),
                "message": log.get("message", ""),
            }
            for log in logs
        ]
    except Exception:
        return []


@task(name="notify_hermes", log_prints=True, retries=2, retry_delay_seconds=5)
def notify_hermes_task(
    event: str,
    payload: Optional[dict[str, Any]] = None,
    flow_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    secret: Optional[str] = None,
    include_logs: bool = True,
) -> dict[str, Any]:
    """发送 callback 通知到 auto-mcp

    Args:
        event: 事件类型。started | completed | failed | task_completed
        payload: 附加业务数据，如 {"excel_path": "...", "summary": "...", "error": "..."}
        flow_name: 流程名称（留空则自动从上下文获取）
        callback_url: callback 地址（留空则从环境变量读取）
        secret: 认证密钥（留空则从 HERMES_CALLBACK_SECRET 读取）
        include_logs: completed/failed 时是否附带 Prefect 日志（默认 True，started 时无效）

    Returns:
        {"success": bool, "status_code": int | None, "detail": str}
    """
    logger = get_run_logger()
    url = (callback_url or DEFAULT_CALLBACK_URL).rstrip("/")
    sec = secret if secret is not None else DEFAULT_CALLBACK_SECRET

    if not url:
        logger.warning("[notify_hermes] HERMES_CALLBACK_URL / HERMES_WEBHOOK_URL 未配置，跳过通知")
        return {"success": False, "status_code": None, "detail": "callback_url not set"}

    ctx = FlowRunContext.get()
    run_id = str(ctx.flow_run.id) if ctx and ctx.flow_run else None
    run_name = ctx.flow_run.name if ctx and ctx.flow_run else None
    deployment_id = (
        str(ctx.flow_run.deployment_id)
        if ctx and ctx.flow_run and ctx.flow_run.deployment_id
        else None
    )
    flow_name = flow_name or run_name
    timestamp = datetime.now(timezone.utc).isoformat()

    body: dict[str, Any] = {
        "event": event,
        "flow_run_id": run_id,
        "flow_run_name": run_name,
        "deployment_id": deployment_id,
        "flow_name": flow_name,
        "timestamp": timestamp,
        "payload": payload or {},
        "logs": [],
    }

    if include_logs and event in ("completed", "failed") and run_id:
        logs = _fetch_run_logs(run_id, DEFAULT_PREFECT_API_URL, limit=30)
        body["logs"] = logs
        logger.info(f"[notify_hermes] 附带 {len(logs)} 条日志")

    headers = {"Content-Type": "application/json"}
    if sec:
        headers["x-callback-secret"] = sec

    try:
        response = httpx.post(url, json=body, headers=headers, timeout=_TIMEOUT)
        response.raise_for_status()
        logger.info(f"[notify_hermes] {event} 通知成功 ({response.status_code})")
        return {"success": True, "status_code": response.status_code, "detail": response.text}
    except httpx.HTTPStatusError as e:
        logger.warning(
            f"[notify_hermes] {event} 通知失败: HTTP {e.response.status_code} - {e.response.text}"
        )
        return {"success": False, "status_code": e.response.status_code, "detail": e.response.text}
    except httpx.RequestError as e:
        logger.warning(f"[notify_hermes] {event} 通知失败: 网络错误 - {e}")
        return {"success": False, "status_code": None, "detail": str(e)}


@task(name="hermes_flow_started", log_prints=True, retries=1, retry_delay_seconds=3)
def hermes_flow_started(
    flow_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    secret: Optional[str] = None,
) -> dict[str, Any]:
    """在 Flow 开始时调用"""
    return notify_hermes_task(
        event="started",
        flow_name=flow_name,
        callback_url=callback_url,
        secret=secret,
        include_logs=False,
    )


@task(name="hermes_flow_completed", log_prints=True, retries=2, retry_delay_seconds=5)
def hermes_flow_completed(
    payload: Optional[dict[str, Any]] = None,
    flow_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    secret: Optional[str] = None,
) -> dict[str, Any]:
    """在 Flow 成功结束时调用"""
    return notify_hermes_task(
        event="completed",
        payload=payload,
        flow_name=flow_name,
        callback_url=callback_url,
        secret=secret,
        include_logs=True,
    )


@task(name="hermes_flow_failed", log_prints=True, retries=2, retry_delay_seconds=5)
def hermes_flow_failed(
    error_message: Optional[str] = None,
    error_type: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
    flow_name: Optional[str] = None,
    callback_url: Optional[str] = None,
    secret: Optional[str] = None,
) -> dict[str, Any]:
    """在 Flow 失败时调用"""
    merged_payload = dict(payload or {})
    if error_message:
        merged_payload["error"] = error_message
    if error_type:
        merged_payload["error_type"] = error_type
    return notify_hermes_task(
        event="failed",
        payload=merged_payload,
        flow_name=flow_name,
        callback_url=callback_url,
        secret=secret,
        include_logs=True,
    )
