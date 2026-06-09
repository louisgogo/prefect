"""Hermes Webhook 通知模块

为 Prefect flow 提供统一的 Hermes 回调能力。
Hermes 触发 flow 后秒回复用户，flow 执行结果通过 webhook 异步通知回 Hermes。

环境变量:
    HERMES_WEBHOOK_URL: Hermes 回调接收地址，例: http://hermes.internal/api/callbacks/prefect
    HERMES_WEBHOOK_SECRET: 认证密钥，Bearer Token

使用方式:
    1. 在 flow 开头调用 notify_hermes_task(event="started", ...)
    2. 在 flow 结尾调用 notify_hermes_task(event="completed", payload={"result": ...})
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
                payload={"excel_path": "/mnt/xgd_share/...", "summary": "..."}
            )
        except Exception as e:
            notify_hermes_task(event="failed", flow_name="往来对账", payload={"error": str(e)})
            raise
"""

import os
from typing import Any, Optional

import httpx

from prefect import get_run_logger, task
from prefect.context import FlowRunContext

DEFAULT_WEBHOOK_URL = os.environ.get("HERMES_WEBHOOK_URL", "")
DEFAULT_SECRET = os.environ.get("HERMES_WEBHOOK_SECRET", "")
DEFAULT_PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://127.0.0.1:4200/api")

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
        # 按时间正序排列，方便阅读
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
    webhook_url: Optional[str] = None,
    secret: Optional[str] = None,
    include_logs: bool = True,
) -> dict[str, Any]:
    """发送 webhook 通知到 Hermes

    Args:
        event: 事件类型。started | completed | failed | task_completed
        payload: 附加业务数据，如 {"excel_path": "...", "summary": "...", "error": "..."}
        flow_name: 流程名称（留空则自动从上下文获取）
        webhook_url: Hermes 回调地址（留空则从环境变量读取）
        secret: 认证密钥（留空则从环境变量读取）
        include_logs: completed/failed 时是否附带 Prefect 日志（默认 True，started 时无效）

    Returns:
        {"success": bool, "status_code": int | None, "detail": str}
    """
    logger = get_run_logger()
    url = webhook_url or DEFAULT_WEBHOOK_URL
    token = secret or DEFAULT_SECRET

    if not url:
        logger.warning("[notify_hermes] HERMES_WEBHOOK_URL 未配置，跳过通知")
        return {"success": False, "status_code": None, "detail": "HERMES_WEBHOOK_URL not set"}

    # 自动从 Prefect 上下文提取运行信息
    ctx = FlowRunContext.get()
    run_id = str(ctx.flow_run.id) if ctx and ctx.flow_run else None
    run_name = ctx.flow_run.name if ctx and ctx.flow_run else None
    deployment_id = (
        str(ctx.flow_run.deployment_id)
        if ctx and ctx.flow_run and ctx.flow_run.deployment_id
        else None
    )

    body: dict[str, Any] = {
        "event": event,
        "flow_run_id": run_id,
        "flow_run_name": run_name,
        "deployment_id": deployment_id,
        "flow_name": flow_name or run_name,
        "timestamp": ctx.flow_run.start_time.isoformat()
        if ctx and ctx.flow_run and ctx.flow_run.start_time
        else None,
        "payload": payload or {},
    }

    # completed / failed 时自动抓取日志摘要
    if include_logs and event in ("completed", "failed") and run_id:
        logs = _fetch_run_logs(run_id, DEFAULT_PREFECT_API_URL, limit=30)
        body["logs"] = logs
        logger.info(f"[notify_hermes] 附带 {len(logs)} 条日志")

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        response = httpx.post(url, json=body, headers=headers, timeout=15.0)
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
