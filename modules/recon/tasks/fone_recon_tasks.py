"""FONE 往来对账 API 调用任务。

通过 AIHub FONE 代理执行脚本，认证 token 只从 worker 环境变量读取。
"""

import json
import os
import re
import time
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
FONE_RECON_CONTENT_ID = "66f3691df6f9db36f4cc9e32"
FONE_RECON_SCRIPT_NAME = "0501-获取ERP科目余额表-WebApi"
FONE_RECON_OPERATE_SOURCE_NAME = "Prefect-往来数据"
FONE_RECON_VERIFY_TIMEOUT_SECONDS = int(os.environ.get("FONE_RECON_VERIFY_TIMEOUT_SECONDS", "900"))
FONE_RECON_VERIFY_INTERVAL_SECONDS = int(os.environ.get("FONE_RECON_VERIFY_INTERVAL_SECONDS", "15"))

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


def _read_fone_recon_target_state(target_date: str) -> Dict[str, Any]:
    """读取 Fone2BI_IntCommCheck 指定期间的行数和 ID 范围。"""
    from mypackage.utilities import engine_to_mysql
    from sqlalchemy import text

    engine = engine_to_mysql()
    with engine.connect() as connection:
        row = (
            connection.execute(
                text(
                    """
                SELECT COUNT(*) AS row_count, MIN(id) AS min_id, MAX(id) AS max_id
                FROM Fone2BI_IntCommCheck
                WHERE `日期` = :target_date
                """
                ),
                {"target_date": target_date},
            )
            .mappings()
            .one()
        )
    return {
        "row_count": int(row["row_count"]),
        "min_id": row["min_id"],
        "max_id": row["max_id"],
    }


def _fone_recon_target_refreshed(
    previous_state: Dict[str, Any], current_state: Dict[str, Any]
) -> bool:
    """目标期间必须非空，且相对执行前状态发生变化。"""
    if current_state["row_count"] <= 0:
        return False
    if previous_state["row_count"] <= 0:
        return True
    previous_signature = (
        previous_state["row_count"],
        previous_state["min_id"],
        previous_state["max_id"],
    )
    current_signature = (
        current_state["row_count"],
        current_state["min_id"],
        current_state["max_id"],
    )
    return current_signature != previous_signature


def _wait_for_fone_recon_target_refresh(
    target_date: str,
    previous_state: Dict[str, Any],
) -> Dict[str, Any]:
    """轮询业务目标表，直到刷新后的状态连续两次保持稳定。"""
    deadline = time.monotonic() + FONE_RECON_VERIFY_TIMEOUT_SECONDS
    next_progress_log = time.monotonic()
    stable_candidate = None
    while True:
        current_state = _read_fone_recon_target_state(target_date)
        if _fone_recon_target_refreshed(previous_state, current_state):
            if stable_candidate == current_state:
                return current_state
            stable_candidate = current_state
        else:
            stable_candidate = None
        now = time.monotonic()
        if now >= deadline:
            raise RuntimeError(
                "等待 FONE 目标表刷新完成超时: "
                f"target_date={target_date}, before={previous_state}, current={current_state}"
            )
        if now >= next_progress_log:
            print(
                "FONE 脚本仍在后台执行，等待目标表刷新: "
                f"target_date={target_date}, current_rows={current_state['row_count']}"
            )
            next_progress_log = now + 60
        time.sleep(min(FONE_RECON_VERIFY_INTERVAL_SECONDS, max(0, deadline - time.monotonic())))


def _mark_fone_script_backend_running(task_id: str, headers: Dict[str, str]) -> None:
    """调用 FONE 原生后台运行接口，让长任务脱离已超时的前台请求。"""
    try:
        response = requests.post(
            f"{FONE_PROXY_BASE_URL}/api/Script/BackendRunning",
            json={"appId": APP_ID, "taskId": task_id},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        try:
            response_data = response.json()
        except ValueError:
            print("[WARN] FONE 后台运行确认响应不是有效 JSON，将继续按目标表验证")
            return
        if not _is_fone_api_success(response_data):
            print(
                "[WARN] FONE 后台运行确认接口返回失败，将继续按目标表验证: "
                f"status={response_data.get('status')}, message={response_data.get('message')}"
            )
    except requests.RequestException as exc:
        details = _http_error_summary(exc.response) if exc.response is not None else str(exc)
        print(f"[WARN] FONE 后台运行确认请求失败，将继续按目标表验证: {details}")


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
    previous_target_state = _read_fone_recon_target_state(start_date)
    # appUserId 由 AIHub 第三方绑定自动注入；调用方传入会触发 82403 字段冲突。
    payload = {
        "appID": APP_ID,
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
        if response.status_code == 504:
            print("AIHub 网关 60 秒超时，FONE 脚本可能仍在执行；" f"转后台并等待目标表刷新，task_id={task_id}")
            _mark_fone_script_backend_running(task_id, headers)
            target_state = _wait_for_fone_recon_target_refresh(
                start_date,
                previous_target_state,
            )
            print(
                "FONE 后台脚本已通过目标表验证: " f"target_date={start_date}, rows={target_state['row_count']}"
            )
            return {
                "api_success": True,
                "task_id": task_id,
                "script_status": 0,
                "console_log_count": 0,
                "warning_count": 1,
                "error_count": 0,
                "gateway_timeout_recovered": True,
                "target_row_count": target_state["row_count"],
            }
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

    target_state = _wait_for_fone_recon_target_refresh(
        start_date,
        previous_target_state,
    )
    print("FONE 往来目标表刷新验证通过: " f"target_date={start_date}, rows={target_state['row_count']}")

    return {
        "api_success": True,
        "task_id": task_id,
        "script_status": script_status,
        "console_log_count": len(console_logs),
        "warning_count": len(warning_messages),
        "error_count": len(error_messages),
        "gateway_timeout_recovered": False,
        "target_row_count": target_state["row_count"],
    }
