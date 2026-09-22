"""Daily synchronization of the HR master data used by the admin org chart."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

from dotenv import dotenv_values
from prefect import flow, get_run_logger

DEFAULT_APP_ROOT = "/root/fastapi/AIPlatform"
DEFAULT_ENV_FILE = f"{DEFAULT_APP_ROOT}/.env"
DEFAULT_BASE_URL = "http://dc-api.xgd.com"


def _required_setting(value: Optional[str], env_name: str, default: str = "") -> str:
    resolved = (value or os.environ.get(env_name) or default).strip()
    if not resolved:
        raise ValueError(f"未配置 {env_name}")
    return resolved


@flow(name="hr_org_sync_flow", log_prints=True)
def hr_org_sync_flow(
    app_root: Optional[str] = None,
    env_file: Optional[str] = None,
    base_url: Optional[str] = None,
    python_executable: Optional[str] = None,
) -> dict[str, Any]:
    """Run the checked-in FastAPI HR mirror script once.

    The worker supplies HR_SYNC_USERNAME/HR_SYNC_PASSWORD through its environment;
    secrets are never persisted in the deployment parameters or source code.
    """
    logger = get_run_logger()
    resolved_root = Path(
        _required_setting(app_root, "HR_SYNC_APP_ROOT", DEFAULT_APP_ROOT)
    ).resolve()
    resolved_env = Path(_required_setting(env_file, "HR_SYNC_ENV_FILE", DEFAULT_ENV_FILE)).resolve()
    env_values = dotenv_values(resolved_env)
    resolved_base_url = _required_setting(
        base_url
        or os.environ.get("HR_SYNC_BASE_URL")
        or str(env_values.get("HR_SYNC_BASE_URL") or ""),
        "HR_SYNC_BASE_URL",
        DEFAULT_BASE_URL,
    )
    username = _required_setting(
        os.environ.get("HR_SYNC_USERNAME") or str(env_values.get("HR_SYNC_USERNAME") or ""),
        "HR_SYNC_USERNAME",
    )
    password = _required_setting(
        os.environ.get("HR_SYNC_PASSWORD") or str(env_values.get("HR_SYNC_PASSWORD") or ""),
        "HR_SYNC_PASSWORD",
    )
    script = resolved_root / "scripts" / "sync_hr_master_data.py"
    if not script.is_file():
        raise FileNotFoundError(f"HR同步脚本不存在：{script}")
    if not resolved_env.is_file():
        raise FileNotFoundError(f"HR同步环境文件不存在：{resolved_env}")

    interpreter = Path(
        _required_setting(
            python_executable,
            "HR_SYNC_PYTHON",
            str(resolved_root / ".venv" / "bin" / "python"),
        )
    )
    if not interpreter.is_file():
        interpreter = Path(sys.executable)

    child_env = os.environ.copy()
    child_env.update({"HR_SYNC_USERNAME": username, "HR_SYNC_PASSWORD": password})
    command = [
        str(interpreter),
        str(script),
        "--env-file",
        str(resolved_env),
        "--base-url",
        resolved_base_url,
    ]
    logger.info("开始同步 HR 组织与人员主数据")
    completed = subprocess.run(
        command,
        cwd=str(resolved_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if completed.returncode:
        detail = (completed.stderr or completed.stdout or "HR同步失败").strip()
        raise RuntimeError(detail[-4000:])

    output = (completed.stdout or "").strip()
    try:
        result = json.loads(output)
    except json.JSONDecodeError:
        result = {"output": output}
    logger.info(
        "HR同步完成：%s 个组织，%s 名人员",
        result.get("organizations", "?"),
        result.get("employees", "?"),
    )
    return result
