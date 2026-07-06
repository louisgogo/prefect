"""FONE 往来对账 - API 调用 Tasks

封装 FONE 系统的登录和脚本执行接口，供 Prefect Flow 调用。
"""

import json
import os
import sys
from typing import Any, Dict

import requests

from prefect import task

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

FONE_LOGIN_URL = "http://10.18.6.222"
FONE_SCRIPT_URL = "https://epm.xgd.com"
USERNAME = "api"
PASSWORD = "KJDJ@D444JFK2#FFCVJF"

APP_ID = "62908e353a35730f118e4e5c"
APP_USER_ID = "6a0183b79e00504b378bc0f0"
GLOBAL_USER_ID = "6a0182c19e00504b378bc0dc"


def _is_fone_api_success(data: Dict[str, Any]) -> bool:
    """兼容 FONE 不同接口版本的成功标记。"""
    if data.get("isSuccess") is True:
        return True
    if data.get("status") is True:
        return True
    return str(data.get("code")) == "0"


def _redact_fone_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """脱敏 FONE 响应中的认证票据，仅用于日志和错误信息。"""
    redacted = dict(data)
    response_data = redacted.get("data")
    if isinstance(response_data, dict):
        redacted["data"] = dict(response_data)
        for key in ("ticket", "Ticket"):
            if redacted["data"].get(key):
                redacted["data"][key] = "***REDACTED***"
    return redacted


def _build_script_text(start_date: str, end_date: str) -> str:
    """构造 0501 脚本内容，替换日期变量。（生产环境配置）"""
    template_path = os.path.join(os.path.dirname(__file__), "fone_recon_script_template.txt")
    with open(template_path, "r", encoding="utf-8") as f:
        template = f.read()
    return template.replace("'__START_DATE__'", "'" + start_date + "'").replace(
        "'__END_DATE__'", "'" + end_date + "'"
    )


@task(name="get_fone_token", log_prints=True)
def get_fone_token_task() -> Dict[str, str]:
    """调用 FONE 登录接口获取 ticket 和 user_id。"""
    url = f"{FONE_LOGIN_URL}/api/login/prod"
    payload = {"username": USERNAME, "password": PASSWORD}
    headers = {"Content-Type": "application/json"}

    print(f"--> 请求登录接口: {url}")
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
    except requests.exceptions.Timeout:
        raise RuntimeError("登录请求超时 - FONE 服务器可能无法访问")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"连接失败 - 无法连接到 FONE 服务器 ({FONE_LOGIN_URL}): {e}")

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"登录响应解析失败 (HTTP {resp.status_code}): {e}, 原始内容: {resp.text}")

    print(f"<-- 登录响应 (HTTP {resp.status_code}):")
    print(json.dumps(_redact_fone_response(data), ensure_ascii=False, indent=2))

    if not _is_fone_api_success(data):
        raise RuntimeError(f"登录失败: {_redact_fone_response(data)}")

    ticket = data.get("data", {}).get("ticket") or data.get("data", {}).get("Ticket")
    user_id = data.get("data", {}).get("user_id")
    if not ticket:
        raise RuntimeError(f"登录响应中未找到 ticket: {data}")

    print(f"✓ 登录成功！获取 ticket 成功")
    return {"ticket": ticket, "user_id": user_id}


@task(name="execute_fone_recon_script", log_prints=True)
def execute_fone_recon_script_task(ticket: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """调用 FONE 执行脚本接口，运行 0501 往来对账脚本。"""
    url = f"{FONE_SCRIPT_URL}/api/Script/ExcuteScriptText"
    headers = {
        "Content-Type": "application/json",
        "ewaresoft-fone-applicationid": APP_ID,
        "ewaresoft-fone-applicationuserid": APP_USER_ID,
        "ewaresoft-fone-globaluserid": GLOBAL_USER_ID,
        "Authorization": ticket,
    }

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

    print(f"\n--> 请求执行脚本接口: {url}")
    print(f"--> 脚本日期范围: {start_date} ~ {end_date}")

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=900)
    except requests.exceptions.Timeout:
        raise RuntimeError("执行脚本请求超时 (900s)")
    except Exception as e:
        raise RuntimeError(f"执行脚本请求异常: {e}")

    print(f"<-- 执行脚本响应 (HTTP {resp.status_code})")

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"执行脚本响应解析失败: {e}, 原始内容: {resp.text}")

    print(json.dumps(data, ensure_ascii=False, indent=2))

    if not _is_fone_api_success(data):
        raise RuntimeError(f"执行脚本接口调用失败: {data}")

    inner_data_str = data.get("data", "{}")
    try:
        inner_data = json.loads(inner_data_str)
    except json.JSONDecodeError:
        inner_data = {"raw": inner_data_str}

    script_status = inner_data.get("status")
    console_logs = inner_data.get("consoleLogs", [])
    error_messages = inner_data.get("errorMessage", [])

    print(f"\n✓ 脚本执行 API 调用成功")
    print(f"--> 脚本内部状态: {script_status}")
    print(f"--> 控制台日志条数: {len(console_logs)}")

    if console_logs:
        print("--> 控制台日志:")
        for log in console_logs:
            print(f"    {log}")

    if error_messages:
        print(f"--> 错误信息: {error_messages}")
        raise RuntimeError(f"脚本执行返回错误: {error_messages}")

    if script_status != 0:
        raise RuntimeError(f"脚本内部状态非成功: status={script_status}")

    return {
        "api_success": True,
        "script_status": script_status,
        "console_logs": console_logs,
        "error_messages": error_messages,
        "raw_response": data,
    }
