"""独立刷新现金流量表的 Prefect 子流程。"""

from datetime import date
from typing import Any, Dict, Optional

from prefect import flow

from utils.date_utils import get_date_range_by_month

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..tasks.data_import_tasks import read_excel_data_task, update_cashflow_data_task
from .data_import_flow import DEFAULT_ROOT_DIRECTORY


def _get_cashflow_refresh_defaults_by_date(reference_date: Optional[date] = None) -> Dict[str, int]:
    """返回现金流刷新在 Prefect UI 中使用的当前年月默认参数。"""
    current = reference_date or date.today()
    return {"year": current.year, "month": current.month}


@flow(name="cashflow_refresh_flow", log_prints=True)
def cashflow_refresh_flow(
    year: int,
    month: int,
    replace_existing: bool = True,
    root_directory: Optional[str] = None,
) -> Dict[str, Any]:
    """按显式年月只刷新 ``fact_cashflow`` 和 ``excel_cashflow_intl``。

    该流程复用数据导入的 Excel 映射逻辑，但不会刷新汇率、利润表或其他手工刷新表。
    ``replace_existing`` 默认为 True，适合现金流来源修订后直接重算指定月份。
    """
    if not 1 <= month <= 12:
        raise ValueError("month 必须是 1-12")
    if root_directory is None:
        root_directory = DEFAULT_ROOT_DIRECTORY

    date_range = get_date_range_by_month(year, month)
    start_date = date_range.min().strftime("%Y-%m-%d")
    end_date = date_range.max().strftime("%Y-%m-%d")
    flow_name = "现金流量表刷新"
    payload = {
        "year": year,
        "month": month,
        "replace_existing": replace_existing,
        "root_directory": root_directory,
    }
    notify_hermes_task(event="started", flow_name=flow_name, payload=payload)

    try:
        print(f"开始刷新 {year}年{month}月现金流量表")
        dfs = read_excel_data_task(root_directory)
        result = update_cashflow_data_task(
            dfs,
            start_date,
            end_date,
            replace_existing=replace_existing,
        )
        notify_hermes_task(
            event="completed",
            flow_name=flow_name,
            payload={**payload, **result, "summary": f"{year}年{month}月现金流量表刷新完成"},
        )
        return {**payload, **result, "start_date": start_date, "end_date": end_date}
    except Exception as exc:
        notify_hermes_task(
            event="failed",
            flow_name=flow_name,
            payload={**payload, "error": str(exc), "error_type": type(exc).__name__},
        )
        raise RuntimeError(f"现金流量表刷新失败: {exc}") from exc
