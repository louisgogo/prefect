"""FONE 往来对账 API 调用任务。

通过 AIHub FONE 代理执行脚本，认证 token 只从 worker 环境变量读取。
"""

import json
import os
from typing import Any, Dict

import requests

from prefect import task

FONE_PROXY_BASE_URL = os.environ.get(
    "AIHUB_FONE_PROXY_BASE_URL", "https://aihub.xgd.com/api/proxy/fone"
).rstrip("/")
FONE_PROXY_TOKEN_ENV = "AIHUB_FONE_API_TOKEN"

APP_ID = "62908e353a35730f118e4e5c"
APP_USER_ID = "6a0183b79e00504b378bc0f0"


def _is_fone_api_success(data: Dict[str, Any]) -> bool:
    """兼容 FONE 不同接口版本的成功标记。"""
    if data.get("isSuccess") is True:
        return True
    if data.get("status") is True:
        return True
    return str(data.get("code")) == "0"


def _get_fone_proxy_token() -> str:
    """读取 AIHub FONE 代理 token，禁止使用代码内默认值。"""
    token = (os.environ.get(FONE_PROXY_TOKEN_ENV) or "").strip()
    if not token:
        raise RuntimeError(f"缺少环境变量 {FONE_PROXY_TOKEN_ENV}")
    return token


def _fone_proxy_headers() -> Dict[str, str]:
    """构造 AIHub FONE 代理请求头。"""
    return {
        "Authorization": f"Bearer {_get_fone_proxy_token()}",
        "Content-Type": "application/json",
    }


def _build_script_text(start_date: str, end_date: str) -> str:
    """构造 0501 脚本内容，替换日期变量。"""
    template_path = os.path.join(os.path.dirname(__file__), "fone_recon_script_template.txt")
    with open(template_path, "r", encoding="utf-8") as file:
        template = file.read()
    return template.replace("'__START_DATE__'", "'" + start_date + "'").replace(
        "'__END_DATE__'", "'" + end_date + "'"
    )


@task(name="execute_fone_recon_script", log_prints=True, retries=0)
def execute_fone_recon_script_task(start_date: str, end_date: str) -> Dict[str, Any]:
    """通过 AIHub FONE 代理执行 0501 往来对账脚本。"""
    url = f"{FONE_PROXY_BASE_URL}/api/Script/ExcuteScriptText"
    script_text = _build_script_text(start_date, end_date)
    payload = {
        "appID": APP_ID,
        "appUserId": APP_USER_ID,
        "scriptText": script_text,
        "context": "",
        "fContentId": "66f3691df6f9db36f4cc9e32",
        "taskId": "script_prefect_" + str(int(__import__("time").time())),
        "scriptName": "0501-获取ERP科目余额表-WebApi",
        "logParams": {
            "开始日期": start_date,
            "结束日期": end_date,
        },
    }

    print(f"--> 通过 AIHub FONE 代理执行脚本: {start_date} ~ {end_date}")
    try:
        response = requests.post(
            url,
            json=payload,
            headers=_fone_proxy_headers(),
            timeout=900,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError("执行脚本请求超时，执行状态未知；请检查 FONE 运行记录后再决定是否重试") from exc
    except requests.RequestException as exc:
        raise RuntimeError("AIHub FONE 代理请求失败") from exc

    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError("AIHub FONE 代理响应不是有效 JSON，执行状态未知") from exc
    if not _is_fone_api_success(data):
        raise RuntimeError(
            "FONE 脚本接口调用失败: " f"status={data.get('status')}, message={data.get('message')}"
        )

    inner_data = data.get("data", {})
    if isinstance(inner_data, str):
        try:
            inner_data = json.loads(inner_data)
        except json.JSONDecodeError:
            inner_data = {}
    if not isinstance(inner_data, dict):
        inner_data = {}

    script_status = inner_data.get("status")
    console_logs = inner_data.get("consoleLogs") or []
    error_messages = inner_data.get("errorMessage") or []
    warning_messages = inner_data.get("warningMessage") or []
    print(
        "FONE 往来脚本执行摘要: "
        f"status={script_status}, logs={len(console_logs)}, "
        f"warnings={len(warning_messages)}, errors={len(error_messages)}"
    )
    if error_messages:
        raise RuntimeError(f"脚本执行返回 {len(error_messages)} 条错误")
    if script_status != 0:
        raise RuntimeError(f"脚本内部状态非成功: status={script_status}")

    return {
        "api_success": True,
        "script_status": script_status,
        "console_log_count": len(console_logs),
        "warning_count": len(warning_messages),
        "error_count": len(error_messages),
    }
