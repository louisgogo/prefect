"""从预算表中获取综合比例流程"""
import os
import sys

from prefect import flow

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from datetime import datetime

from dateutil.relativedelta import relativedelta

from ..tasks.fetch_budget_shared_rate_tasks import (
    fetch_latest_budget_rate_task,
    update_fact_bus_shared_rate_task,
)


def _get_default_dates() -> dict:
    """计算默认日期范围：今年1月1日 到 上月底"""
    today = datetime.now()
    start_dt = datetime(today.year, 1, 1)
    first_day_this_month = datetime(today.year, today.month, 1)
    end_dt = first_day_this_month - relativedelta(days=1)
    return {"start_date": start_dt.strftime("%Y-%m-%d"), "end_date": end_dt.strftime("%Y-%m-%d")}


_FLOW_DEFAULTS = _get_default_dates()


@flow(name="fetch_budget_shared_rate_flow", log_prints=True)
def fetch_budget_shared_rate_flow(
    start_date: str = _FLOW_DEFAULTS["start_date"], end_date: str = _FLOW_DEFAULTS["end_date"]
) -> None:
    """
    预算综合比例获取流程
    负责：
    1. 从 bud_bus_shared_rate 中筛选提取综合比例指标
    2. 将指标铺开写入 fact_bus_shared_rate（覆盖年初至上月底）
    """
    print("开始获取预算综合比例流程")

    # 解析日期
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    print(f"处理日期范围: {start_dt.strftime('%Y-%m-%d')} 至 {end_dt.strftime('%Y-%m-%d')}")

    if start_dt > end_dt:
        print(f"计算出的日期范围无效，可能在1月发生。暂不更新。")
        return

    # 1. 获取最新综合比例并填充
    df_rates = fetch_latest_budget_rate_task(start_dt, end_dt)

    # 2. 更新写入数据库
    update_fact_bus_shared_rate_task(df_rates, start_dt, end_dt)

    print("预算综合比例获取并覆盖写入完成")
