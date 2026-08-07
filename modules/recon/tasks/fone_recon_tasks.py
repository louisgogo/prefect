"""FONE 往来对账 API 调用任务。

通过 AIHub FONE 代理执行脚本，认证 token 只从 worker 环境变量读取。
"""

import json
import os
import re
import uuid
from datetime import date
from typing import Any, Dict

import requests
from prefect import task

FONE_PROXY_BASE_URL = os.environ.get(
    "AIHUB_FONE_PROXY_BASE_URL", "https://aihub.xgd.com/api/proxy/fone"
).rstrip("/")
FONE_PROXY_TOKEN_ENV = "AIHUB_FONE_API_TOKEN"

APP_ID = "62908e353a35730f118e4e5c"
APP_USER_ID = "6a0183b79e00504b378bc0f0"
FONE_RECON_CONTENT_ID = "66f3691df6f9db36f4cc9e32"
FONE_RECON_SCRIPT_NAME = "0501-获取ERP科目余额表-WebApi"
FONE_RECON_OPERATE_SOURCE_NAME = "Prefect-往来数据"

_VARIABLE_MARKER_PATTERN = re.compile(r"@([A-Za-z0-9_\u3400-\u9fff]+)@")


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


def _parse_fone_recon_content_response(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """解析并校验 FONE 0501 内容定义。"""
    if not isinstance(response_data, dict) or not _is_fone_api_success(response_data):
        raise RuntimeError("FONE 往来脚本内容读取接口返回失败")

    outer_data = response_data.get("data")
    if not isinstance(outer_data, dict):
        raise RuntimeError("FONE 往来脚本内容响应缺少 data 对象")
    if outer_data.get("appId") != APP_ID:
        raise RuntimeError("FONE 往来脚本内容所属应用与预期不一致")

    raw_definition = outer_data.get("data")
    try:
        definition = (
            json.loads(raw_definition) if isinstance(raw_definition, str) else raw_definition
        )
    except json.JSONDecodeError as exc:
        raise RuntimeError("FONE 往来脚本定义不是有效 JSON") from exc

    if not isinstance(definition, dict):
        raise RuntimeError("FONE 往来脚本定义必须是对象")
    if not isinstance(definition.get("variables"), list):
        raise RuntimeError("FONE 往来脚本定义缺少 variables 列表")
    if not isinstance(definition.get("scriptText"), str) or not definition["scriptText"]:
        raise RuntimeError("FONE 往来脚本定义缺少 scriptText")
    return definition


def _compile_fone_recon_script(definition: Dict[str, Any], start_date: str, end_date: str) -> str:
    """将内容库中的 0501 脚本变量编译成可执行脚本文本。"""
    try:
        parsed_start_date = date.fromisoformat(start_date)
        parsed_end_date = date.fromisoformat(end_date)
    except (TypeError, ValueError) as exc:
        raise ValueError("start_date 和 end_date 必须是 YYYY-MM-DD 格式") from exc
    if parsed_start_date > parsed_end_date:
        raise ValueError("start_date 不能晚于 end_date")

    body = definition.get("scriptText", "")
    markers = set(_VARIABLE_MARKER_PATTERN.findall(body))
    missing_date_markers = {"开始日期", "结束日期"} - markers
    if missing_date_markers:
        raise RuntimeError("FONE 往来脚本缺少日期变量占位符: " + ", ".join(sorted(missing_date_markers)))

    variables_by_name = {}
    for variable in definition.get("variables", []):
        if not isinstance(variable, dict) or not variable.get("name"):
            raise RuntimeError("FONE 往来脚本 variables 中存在无效变量")
        name = variable["name"]
        if name in variables_by_name:
            raise RuntimeError(f"FONE 往来脚本 variables 中存在重复变量: {name}")
        variables_by_name[name] = variable

    undefined_markers = markers - variables_by_name.keys()
    if undefined_markers:
        raise RuntimeError("FONE 往来脚本存在未定义变量: " + ", ".join(sorted(undefined_markers)))

    values = []
    for variable in definition.get("variables", []):
        name = variable["name"]
        if name not in markers:
            continue
        if name == "开始日期":
            value = start_date
        elif name == "结束日期":
            value = end_date
        elif variable.get("id"):
            value = variable["id"]
        elif variable.get("value") is not None:
            value = variable["value"]
        elif variable.get("defaultValue") is not None:
            value = variable["defaultValue"]
        else:
            raise RuntimeError(f"FONE 往来脚本变量 {name} 缺少可编译值")
        values.append((name, value))

    for name, _ in values:
        body = body.replace(f"@{name}@", name)

    unresolved = sorted(set(_VARIABLE_MARKER_PATTERN.findall(body)))
    if unresolved:
        raise RuntimeError("FONE 往来脚本仍有未解析变量: " + ", ".join(unresolved))

    prefix = "".join(
        f"var {name}={json.dumps(value, ensure_ascii=False)};" for name, value in values
    )
    return prefix + body


def _http_error_summary(response: requests.Response) -> str:
    """提取不包含请求正文、token 或脚本文本的 HTTP 错误摘要。"""

    def sanitize(value: Any) -> str:
        text = str(value).strip().replace("\r", " ").replace("\n", " ")
        text = re.sub(r"(?i)bearer\s+[a-z0-9._~+/=-]+", "Bearer ***REDACTED***", text)
        text = re.sub(
            r"(?i)(password|token|authorization|secret)(\s*[:=]\s*)[^,;\s]+",
            r"\1\2***REDACTED***",
            text,
        )
        return text[:300]

    summary = f"HTTP {response.status_code}"
    try:
        response_data = response.json()
    except ValueError:
        response_text = sanitize(response.text or "")
        if response_text:
            summary += f", response={response_text}"
        return summary

    if not isinstance(response_data, dict):
        return summary

    details = []
    for key in ("code", "status", "message", "error", "detail", "title"):
        value = response_data.get(key)
        if value not in (None, "", [], {}) and isinstance(value, (str, int, float, bool)):
            details.append(f"{key}={sanitize(value)}")
    if details:
        summary += ", " + ", ".join(details)
    return summary


@task(name="execute_fone_recon_script", log_prints=True, retries=0)
def execute_fone_recon_script_task(start_date: str, end_date: str) -> Dict[str, Any]:
    """通过 AIHub FONE 代理执行 0501 往来对账脚本。"""
    headers = _fone_proxy_headers()
    try:
        content_response = requests.post(
            f"{FONE_PROXY_BASE_URL}/api/FContent/GetFContent",
            json={"_id": FONE_RECON_CONTENT_ID},
            headers=headers,
            timeout=60,
        )
        content_response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError("读取 FONE 往来脚本定义超时") from exc
    except requests.RequestException as exc:
        details = _http_error_summary(exc.response) if exc.response is not None else str(exc)
        raise RuntimeError(f"读取 FONE 往来脚本定义失败: {details}") from exc

    try:
        definition = _parse_fone_recon_content_response(content_response.json())
    except ValueError as exc:
        raise RuntimeError("FONE 往来脚本内容响应不是有效 JSON") from exc
    script_text = _compile_fone_recon_script(definition, start_date, end_date)
    task_id = f"script_prefect_recon_{uuid.uuid4().hex}"
    payload = {
        "appID": APP_ID,
        "appUserId": APP_USER_ID,
        "scriptText": script_text,
        "context": "",
        "fContentId": FONE_RECON_CONTENT_ID,
        "taskId": task_id,
        "scriptName": FONE_RECON_SCRIPT_NAME,
        "logParams": {
            "开始日期": start_date,
            "结束日期": end_date,
        },
        "operateSourceName": FONE_RECON_OPERATE_SOURCE_NAME,
        "from": "report",
    }

    print(f"--> 通过 AIHub FONE 代理执行脚本: {start_date} ~ {end_date}")
    try:
        response = requests.post(
            f"{FONE_PROXY_BASE_URL}/api/Script/ExcuteScriptText",
            json=payload,
            headers=headers,
            timeout=900,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError("执行脚本请求超时，执行状态未知；请检查 FONE 运行记录后再决定是否重试") from exc
    except requests.RequestException as exc:
        details = _http_error_summary(exc.response) if exc.response is not None else str(exc)
        raise RuntimeError(f"AIHub FONE 代理请求失败，执行状态可能未知: {details}") from exc

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
        "task_id": task_id,
        "script_status": script_status,
        "console_log_count": len(console_logs),
        "warning_count": len(warning_messages),
        "error_count": len(error_messages),
    }
