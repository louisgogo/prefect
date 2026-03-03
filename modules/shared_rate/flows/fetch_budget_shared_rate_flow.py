"""从预算表中获取综合比例流程"""
from prefect import flow
import sys
import os

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ..tasks.fetch_budget_shared_rate_tasks import (
    fetch_latest_budget_rate_task,
    update_fact_bus_shared_rate_task
)

@flow(name="fetch_budget_shared_rate_flow", log_prints=True)
def fetch_budget_shared_rate_flow() -> None:
    """
    预算综合比例获取流程
    负责：
    1. 从 bud_bus_shared_rate 中筛选提取综合比例指标
    2. 将指标铺开写入 fact_bus_shared_rate（覆盖年初至上月底）
    """
    print("开始获取预算综合比例流程")
    
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    today = datetime.now()
    start_date = datetime(today.year, 1, 1)
    first_day_this_month = datetime(today.year, today.month, 1)
    end_date = first_day_this_month - relativedelta(days=1)
    
    if start_date > end_date:
        print(f"计算出的日期范围无效，可能在1月发生。暂不更新。")
        return

    # 1. 获取最新综合比例
    df_rates = fetch_latest_budget_rate_task(start_date, end_date)
    
    # 2. 更新写入数据库
    update_fact_bus_shared_rate_task(df_rates, start_date, end_date)
    
    print("预算综合比例获取并覆盖写入完成")
