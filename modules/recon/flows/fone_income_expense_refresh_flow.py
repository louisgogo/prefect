"""顺序刷新并验证 FONE 收入、费用明细的 Prefect 子流程。"""

from typing import Any, Dict, Optional

from prefect import flow

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..tasks.fone_income_expense_tasks import (
    execute_fone_income_expense_script_task,
    get_fone_detail_table_state_task,
    resolve_fone_detail_refresh_parameters,
    validate_fone_detail_table_state_task,
)


@flow(name="fone_income_expense_refresh_flow", log_prints=True)
def fone_income_expense_refresh_flow(
    year: int,
    month: int,
    permission_user: Optional[str] = None,
) -> Dict[str, Any]:
    """刷新显式年月的 FONE 收入和费用明细，并逐阶段回读验证。

    `permission_user` 必须是已配置法人组织权限的 FONE 账号；也可通过
    `FONE_DETAIL_PERMISSION_USER` 环境变量提供。收入先执行，验证通过后才执行费用。
    """
    year, month, permission_user = resolve_fone_detail_refresh_parameters(
        year=year,
        month=month,
        permission_user=permission_user,
    )
    flow_name = "FONE收入费用明细刷新"
    notify_hermes_task(
        event="started",
        flow_name=flow_name,
        payload={"year": year, "month": month},
    )

    try:
        print("【阶段1/2】刷新 FONE 收入成本明细")
        income_before = get_fone_detail_table_state_task("income")
        income_execution = execute_fone_income_expense_script_task(
            detail_type="income",
            year=year,
            month=month,
            permission_user=permission_user,
        )
        income_state = validate_fone_detail_table_state_task(
            detail_type="income",
            year=year,
            month=month,
            previous_state=income_before,
        )

        print("【阶段2/2】刷新 FONE 费用明细")
        expense_before = get_fone_detail_table_state_task("expense")
        expense_execution = execute_fone_income_expense_script_task(
            detail_type="expense",
            year=year,
            month=month,
            permission_user=permission_user,
        )
        expense_state = validate_fone_detail_table_state_task(
            detail_type="expense",
            year=year,
            month=month,
            previous_state=expense_before,
        )

        result = {
            "year": year,
            "month": month,
            "income_execution": income_execution,
            "income_tables": income_state["tables"],
            "expense_execution": expense_execution,
            "expense_tables": expense_state["tables"],
        }
        notify_hermes_task(
            event="completed",
            flow_name=flow_name,
            payload={
                "year": year,
                "month": month,
                "income_rows": income_state["tables"]["FONE_MRPT_AC_OffLineFormat"]["row_count"],
                "expense_rows": expense_state["tables"]["FONE_MRPT_FY_OffLineFormat"]["row_count"],
                "expense_detail_rows": expense_state["tables"]["FONE_MRPT_FY_OffLineDetail"][
                    "row_count"
                ],
                "summary": f"FONE收入费用明细刷新完成：{year}年{month}月",
            },
        )
        return result
    except Exception as exc:
        notify_hermes_task(
            event="failed",
            flow_name=flow_name,
            payload={
                "year": year,
                "month": month,
                "error": str(exc),
                "error_type": type(exc).__name__,
            },
        )
        raise RuntimeError(f"FONE收入费用明细刷新失败: {exc}") from exc
