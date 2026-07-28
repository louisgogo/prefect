"""FONE 收入、费用明细刷新任务。

脚本正文由 FONE 内容库动态读取，避免在仓库中复制带环境配置和凭据的报表脚本。
"""

import json
import os
import re
import time
import uuid
from typing import Any, Dict, Optional, Tuple

import requests

from prefect import task

from .fone_recon_tasks import APP_ID, FONE_PROXY_BASE_URL, _fone_proxy_headers, _is_fone_api_success

FONE_DETAIL_APP_USER_ID = "62b570fd90fc832e38480750"
FONE_DETAIL_PERMISSION_USER_ENV = "FONE_DETAIL_PERMISSION_USER"

FONE_DETAIL_SCRIPTS = {
    "income": {
        "content_id": "661b866863556863c96d4bbf",
        "script_name": "040299-企微推送FONE收入成本明细",
        "operate_source_name": "Prefect-收入成本明细",
    },
    "expense": {
        "content_id": "65b317024528e1401bd17289",
        "script_name": "040199-企微推送FONE费用明细",
        "operate_source_name": "Prefect-费用明细",
    },
}

_PERMISSION_USER_ASSIGNMENT = "var userID = user.account;"
_VARIABLE_MARKER_PATTERN = re.compile(r"@([A-Za-z0-9_\u3400-\u9fff]+)@")

_TABLE_QUERIES = {
    "income": {
        "FONE_MRPT_AC_OffLineFormat": """
            SELECT COUNT(*), MIN(`id`), MAX(`id`),
                   COUNT(DISTINCT `会计期间`), MIN(`会计期间`), MAX(`会计期间`)
            FROM `fone_db`.`FONE_MRPT_AC_OffLineFormat`
        """,
    },
    "expense": {
        "FONE_MRPT_FY_OffLineFormat": """
            SELECT COUNT(*), MIN(`id`), MAX(`id`),
                   COUNT(DISTINCT `会计期间`), MIN(`会计期间`), MAX(`会计期间`)
            FROM `fone_db`.`FONE_MRPT_FY_OffLineFormat`
        """,
        "FONE_MRPT_FY_OffLineDetail": """
            SELECT COUNT(*), MIN(`id`), MAX(`id`),
                   COUNT(DISTINCT CONCAT(`年`, '-', `月`)),
                   MIN(CONCAT(`年`, '-', `月`)), MAX(CONCAT(`年`, '-', `月`))
            FROM `fone_db`.`FONE_MRPT_FY_OffLineDetail`
        """,
    },
}


def resolve_fone_detail_refresh_parameters(
    year: int,
    month: int,
    permission_user: Optional[str] = None,
) -> Tuple[int, int, str]:
    """校验显式期间并解析拥有法人权限的 FONE 用户。"""
    if isinstance(year, bool) or not isinstance(year, int) or not 1000 <= year <= 9999:
        raise ValueError("year 必须是四位整数年份")
    if isinstance(month, bool) or not isinstance(month, int) or not 1 <= month <= 12:
        raise ValueError("month 必须是 1-12 的整数")

    resolved_user = (
        permission_user or os.environ.get(FONE_DETAIL_PERMISSION_USER_ENV) or ""
    ).strip()
    if not resolved_user:
        raise ValueError(
            "必须传入 permission_user，或设置环境变量 " f"{FONE_DETAIL_PERMISSION_USER_ENV}；API 登录用户本身没有明细刷新权限"
        )
    return year, month, resolved_user


def _parse_fone_content_response(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """解析 GetFContent 的双层 JSON 数据。"""
    if not isinstance(response_data, dict) or not _is_fone_api_success(response_data):
        raise RuntimeError("FONE 内容读取接口返回失败")

    outer_data = response_data.get("data")
    if not isinstance(outer_data, dict):
        raise RuntimeError("FONE 内容读取响应缺少 data 对象")
    raw_definition = outer_data.get("data")
    try:
        definition = (
            json.loads(raw_definition) if isinstance(raw_definition, str) else raw_definition
        )
    except json.JSONDecodeError as exc:
        raise RuntimeError("FONE 脚本定义不是有效 JSON") from exc

    if not isinstance(definition, dict):
        raise RuntimeError("FONE 脚本定义必须是对象")
    if not isinstance(definition.get("variables"), list):
        raise RuntimeError("FONE 脚本定义缺少 variables 列表")
    if not isinstance(definition.get("scriptText"), str) or not definition["scriptText"]:
        raise RuntimeError("FONE 脚本定义缺少 scriptText")
    return definition


def _compile_fone_detail_script(
    definition: Dict[str, Any],
    year: int,
    month: int,
    permission_user: str,
) -> str:
    """编译 FONE 变量，并替换唯一的权限用户赋值。"""
    year, month, permission_user = resolve_fone_detail_refresh_parameters(
        year, month, permission_user
    )
    body = definition.get("scriptText", "")
    markers = set(_VARIABLE_MARKER_PATTERN.findall(body))
    missing_period_markers = {"实际数年", "实际数月"} - markers
    if missing_period_markers:
        raise RuntimeError("FONE 脚本缺少期间变量占位符: " + ", ".join(sorted(missing_period_markers)))

    variables_by_name = {}
    for variable in definition.get("variables", []):
        if not isinstance(variable, dict) or not variable.get("name"):
            raise RuntimeError("FONE variables 中存在无效变量")
        name = variable["name"]
        if name in variables_by_name:
            raise RuntimeError(f"FONE variables 中存在重复变量: {name}")
        variables_by_name[name] = variable

    undefined_markers = markers - variables_by_name.keys()
    if undefined_markers:
        raise RuntimeError(f"FONE 脚本存在未定义变量: {', '.join(sorted(undefined_markers))}")

    values = []
    for variable in definition.get("variables", []):
        name = variable["name"]
        if name not in markers:
            continue
        if name == "实际数年":
            value = str(year)
        elif name == "实际数月":
            value = f"M{month}"
        elif variable.get("id"):
            value = variable["id"]
        elif variable.get("value") is not None:
            value = variable["value"]
        elif variable.get("defaultValue") is not None:
            value = variable["defaultValue"]
        else:
            raise RuntimeError(f"FONE 变量 {name} 缺少可编译值")
        values.append((name, value))

    for name, _ in values:
        body = body.replace(f"@{name}@", name)

    unresolved = sorted(set(_VARIABLE_MARKER_PATTERN.findall(body)))
    if unresolved:
        raise RuntimeError(f"FONE 脚本仍有未解析变量: {', '.join(unresolved)}")

    assignment_count = body.count(_PERMISSION_USER_ASSIGNMENT)
    if assignment_count != 1:
        raise RuntimeError("FONE 脚本权限用户赋值必须且只能出现一次，" f"实际出现 {assignment_count} 次")
    body = body.replace(
        _PERMISSION_USER_ASSIGNMENT,
        f"var userID = {json.dumps(permission_user, ensure_ascii=False)};",
        1,
    )
    prefix = "".join(
        f"var {name}={json.dumps(value, ensure_ascii=False)};" for name, value in values
    )
    return prefix + body


def _read_fone_detail_table_state(detail_type: str) -> Dict[str, Any]:
    """读取目标表的聚合状态，不返回明细行。"""
    if detail_type not in _TABLE_QUERIES:
        raise ValueError(f"不支持的 FONE 明细类型: {detail_type}")

    from mypackage.utilities import connect_to_fone

    connection_result = connect_to_fone()
    if not connection_result:
        raise RuntimeError("无法连接 FONE MySQL")
    connection, cursor = connection_result
    tables = {}
    try:
        for table_name, query in _TABLE_QUERIES[detail_type].items():
            cursor.execute(query)
            row = cursor.fetchone()
            tables[table_name] = {
                "row_count": int(row[0]),
                "id_min": row[1],
                "id_max": row[2],
                "distinct_periods": int(row[3]),
                "period_min": str(row[4]) if row[4] is not None else None,
                "period_max": str(row[5]) if row[5] is not None else None,
            }
    finally:
        cursor.close()
        connection.close()
    return {"detail_type": detail_type, "tables": tables}


def _validate_fone_detail_table_state(
    detail_type: str,
    state: Dict[str, Any],
    year: int,
    month: int,
    previous_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """校验非零行数、唯一期间及刷新前后状态变化。"""
    if detail_type not in _TABLE_QUERIES:
        raise ValueError(f"不支持的 FONE 明细类型: {detail_type}")
    expected_periods = {
        "FONE_MRPT_AC_OffLineFormat": f"{year}-{month:02d}-01",
        "FONE_MRPT_FY_OffLineFormat": f"{year}-{month:02d}-01",
        "FONE_MRPT_FY_OffLineDetail": f"{year}-M{month}",
    }
    current_tables = state.get("tables", {})
    previous_tables = (previous_state or {}).get("tables", {})

    for table_name in _TABLE_QUERIES[detail_type]:
        table_state = current_tables.get(table_name)
        if not table_state:
            raise RuntimeError(f"缺少 FONE 表校验结果: {table_name}")
        if table_state["row_count"] <= 0:
            raise RuntimeError(f"FONE 刷新后表为空: {table_name}")
        expected_period = expected_periods[table_name]
        if (
            table_state["distinct_periods"] != 1
            or table_state["period_min"] != expected_period
            or table_state["period_max"] != expected_period
        ):
            raise RuntimeError(
                f"FONE 表 {table_name} 期间异常，期望 {expected_period}，"
                f"实际 {table_state['period_min']} 至 {table_state['period_max']}"
            )
        if table_state["id_min"] is None or table_state["id_max"] is None:
            raise RuntimeError(f"FONE 表缺少刷新后的 ID 范围: {table_name}")

        prior = previous_tables.get(table_name)
        if prior:
            before_signature = (prior.get("row_count"), prior.get("id_min"), prior.get("id_max"))
            after_signature = (
                table_state.get("row_count"),
                table_state.get("id_min"),
                table_state.get("id_max"),
            )
            if before_signature == after_signature:
                raise RuntimeError(f"FONE 表刷新前后状态未变化: {table_name}")
    return state


@task(name="get_fone_detail_table_state", log_prints=True, retries=0)
def get_fone_detail_table_state_task(detail_type: str) -> Dict[str, Any]:
    """获取收入或费用目标表的刷新前状态。"""
    state = _read_fone_detail_table_state(detail_type)
    counts = {name: value["row_count"] for name, value in state["tables"].items()}
    print(f"FONE {detail_type} 刷新前表状态: {counts}")
    return state


@task(name="execute_fone_income_expense_script", log_prints=True, retries=0)
def execute_fone_income_expense_script_task(
    detail_type: str,
    year: int,
    month: int,
    permission_user: str,
) -> Dict[str, Any]:
    """动态读取并执行收入或费用脚本；不输出脚本日志和认证信息。"""
    if detail_type not in FONE_DETAIL_SCRIPTS:
        raise ValueError(f"不支持的 FONE 明细类型: {detail_type}")
    spec = FONE_DETAIL_SCRIPTS[detail_type]
    headers = _fone_proxy_headers()

    try:
        content_response = requests.post(
            f"{FONE_PROXY_BASE_URL}/api/FContent/GetFContent",
            json={"_id": spec["content_id"]},
            headers=headers,
            timeout=60,
        )
        content_response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError("读取 FONE 脚本定义超时") from exc
    except requests.RequestException as exc:
        raise RuntimeError("读取 FONE 脚本定义失败") from exc

    try:
        definition = _parse_fone_content_response(content_response.json())
    except ValueError as exc:
        raise RuntimeError("FONE 内容读取响应不是有效 JSON") from exc
    script_text = _compile_fone_detail_script(
        definition=definition,
        year=year,
        month=month,
        permission_user=permission_user,
    )
    task_id = f"script_prefect_{detail_type}_{year}_{month:02d}_{uuid.uuid4().hex}"
    payload = {
        "appID": APP_ID,
        "appUserId": FONE_DETAIL_APP_USER_ID,
        "scriptText": script_text,
        "context": "",
        "fContentId": spec["content_id"],
        "taskId": task_id,
        "scriptName": spec["script_name"],
        "logParams": {
            "实际数月": f"{month}月份-M{month}",
            "实际数年": f"{year}年度-{year}",
            "权限用户": permission_user,
        },
        "operateSourceName": spec["operate_source_name"],
        "from": "report",
    }

    started = time.monotonic()
    try:
        response = requests.post(
            f"{FONE_PROXY_BASE_URL}/api/Script/ExcuteScriptText",
            json=payload,
            headers=headers,
            timeout=1800,
        )
        response.raise_for_status()
    except requests.exceptions.Timeout as exc:
        raise RuntimeError("FONE 脚本执行超时，当前执行状态未知；请先检查目标表和 FONE 运行记录，禁止自动重试") from exc
    except requests.RequestException as exc:
        raise RuntimeError("FONE 脚本执行请求失败，执行状态可能未知") from exc

    try:
        response_data = response.json()
    except ValueError as exc:
        raise RuntimeError("FONE 脚本执行响应不是有效 JSON，执行状态未知") from exc
    inner_data = response_data.get("data")
    if isinstance(inner_data, str):
        try:
            inner_data = json.loads(inner_data)
        except json.JSONDecodeError:
            inner_data = {}
    if not isinstance(inner_data, dict):
        inner_data = {}

    errors = inner_data.get("errorMessage") or []
    warnings = inner_data.get("warningMessage") or []
    console_logs = inner_data.get("consoleLogs") or []
    summary = {
        "detail_type": detail_type,
        "task_id": task_id,
        "elapsed_seconds": round(time.monotonic() - started, 2),
        "api_status": response_data.get("status"),
        "api_success": _is_fone_api_success(response_data),
        "script_status": inner_data.get("status"),
        "console_log_count": len(console_logs),
        "warning_count": len(warnings),
        "error_count": len(errors),
    }
    print(f"FONE {detail_type} 脚本执行摘要: {summary}")
    if not summary["api_success"] or summary["script_status"] != 0 or summary["error_count"] > 0:
        raise RuntimeError(
            f"FONE {detail_type} 脚本执行失败: "
            f"script_status={summary['script_status']}, error_count={summary['error_count']}"
        )
    return summary


@task(name="validate_fone_detail_table_state", log_prints=True, retries=0)
def validate_fone_detail_table_state_task(
    detail_type: str,
    year: int,
    month: int,
    previous_state: Dict[str, Any],
) -> Dict[str, Any]:
    """回读目标表并验证本次脚本确实重建了指定期间。"""
    current_state = _read_fone_detail_table_state(detail_type)
    validated = _validate_fone_detail_table_state(
        detail_type=detail_type,
        state=current_state,
        year=year,
        month=month,
        previous_state=previous_state,
    )
    counts = {name: value["row_count"] for name, value in validated["tables"].items()}
    print(f"FONE {detail_type} 刷新后校验通过: {counts}")
    return validated
