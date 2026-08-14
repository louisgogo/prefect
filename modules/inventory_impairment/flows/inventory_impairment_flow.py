"""季度存货跌价计算子流程。"""

from datetime import date
from typing import Dict, Optional

import pandas as pd
from prefect import flow

from ..tasks.inventory_impairment_tasks import (
    calculate_quarterly_inventory_impairment_task,
    get_default_inventory_impairment_period,
    get_quarter_periods,
    load_inventory_impairment_sources_task,
    load_recorded_inventory_impairment_task,
    prepare_fact_profit_bd_rows,
    reconcile_quarterly_inventory_impairment_task,
    replace_inventory_impairment_in_fact_profit_bd_task,
    resolve_inventory_impairment_period,
)


def _get_inventory_impairment_defaults_by_date(
    reference_date: Optional[date] = None,
) -> Dict[str, object]:
    """返回部署和 UI 使用的最近已结束季度默认参数。"""
    year, quarter = get_default_inventory_impairment_period(reference_date)
    return {
        "year": year,
        "quarter": quarter,
        "write_to_fact_profit_bd": True,
        "validate_against_profit_bd": True,
        "tolerance": 0.01,
    }


@flow(name="inventory_impairment_flow", log_prints=True)
def inventory_impairment_flow(
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    write_to_fact_profit_bd: bool = True,
    validate_against_profit_bd: bool = True,
    tolerance: float = 0.01,
) -> Dict[str, object]:
    """计算指定季度各主体存货跌价金额，通过平台同步业报并核对。"""
    year, quarter = resolve_inventory_impairment_period(year, quarter)
    periods, quarter_period = get_quarter_periods(year, quarter)
    period_text = ", ".join(period.strftime("%Y-%m-%d") for period in periods)
    print(
        f"开始季度存货跌价计算：year={year}, quarter={quarter}, "
        f"源期间=[{period_text}]，季度期间={quarter_period.date()}"
    )

    sources = load_inventory_impairment_sources_task(periods)
    result = calculate_quarterly_inventory_impairment_task(
        df_inventory=sources["inventory"],
        df_in_transit=sources["in_transit"],
        periods=periods,
        quarter_period=quarter_period,
    )
    fact_profit_bd_rows = prepare_fact_profit_bd_rows(result["quarterly"], quarter_period)
    result["fact_profit_bd_rows"] = fact_profit_bd_rows

    if write_to_fact_profit_bd:
        result["write_metrics"] = replace_inventory_impairment_in_fact_profit_bd_task(
            rows=fact_profit_bd_rows,
            quarter_period=quarter_period,
        )

    if validate_against_profit_bd:
        recorded = load_recorded_inventory_impairment_task(quarter_period)
        result["recorded"] = recorded
        result["reconciliation"] = reconcile_quarterly_inventory_impairment_task(
            calculated=result["quarterly"],
            recorded=recorded,
            tolerance=tolerance,
        )

    print(f"季度存货跌价计算子流程完成：{year}年第{quarter}季度")
    return result
