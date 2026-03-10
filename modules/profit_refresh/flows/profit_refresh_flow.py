"""利润表刷新流程"""
from prefect import flow
from typing import Optional
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ..tasks.profit_refresh_tasks import (
    load_revenue_for_profit_task,
    load_expense_other_for_profit_task,
    refresh_offset_by_month_task,
    merge_profit_data_task,
    calculate_profit_indicators_task,
    save_profit_table_task,
    load_bus_profit_data_task,
    calculate_bus_profit_indicators_task,
    save_bus_profit_table_task,
)


@flow(name="profit_refresh_flow", log_prints=True)
def profit_refresh_flow(date_range: Optional[pd.DatetimeIndex] = None) -> None:
    """
    利润表刷新流程
    处理所有已计算的月份数据，生成 fact_profit 和 fact_bus_profit 表
    
    Args:
        date_range: 日期范围（所有已计算的月份）。如果留空，默认计算年初至上个自然月末。
    """
    if date_range is None:
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        today = datetime.now()
        start = datetime(today.year, 1, 1)
        end = datetime(today.year, today.month, 1) - relativedelta(days=1)
        date_range = pd.date_range(start=start, end=end)
        print(f"未传入 date_range 参数，自动按默认规则计算范围：{start.strftime('%Y-%m-%d')} 到 {end.strftime('%Y-%m-%d')}")
        
    print(f"开始利润表刷新流程，日期范围: {date_range.min()} 到 {date_range.max()}")
    
    # ========== 普通利润表刷新 ==========
    print("--- 开始刷新普通利润表 ---")
    
    # 加载数据
    df_revenue = load_revenue_for_profit_task(date_range)
    df_expense_other = load_expense_other_for_profit_task(date_range)
    # 先从 fact_offset 重新计算月度数并刷新 fact_offset_by_month，再返回当期数据
    df_offset = refresh_offset_by_month_task(date_range)
    
    # 合并数据
    df_profit = merge_profit_data_task(df_revenue, df_expense_other, df_offset)
    
    # 计算利润指标
    df_profit_final = calculate_profit_indicators_task(df_profit)
    
    # 保存到数据库（只删除计算月份的数据）
    save_profit_table_task(df_profit_final, date_range)
    print("--- 普通利润表刷新完成 ---")
    
    # ========== 业务线利润表刷新 ==========
    print("--- 开始刷新业务线利润表 ---")
    
    # 加载数据
    df_bus_profit = load_bus_profit_data_task(date_range)
    
    # 计算利润指标
    df_bus_profit_final = calculate_bus_profit_indicators_task(df_bus_profit)
    
    # 保存到数据库（只删除计算月份的数据）
    save_bus_profit_table_task(df_bus_profit_final, date_range)
    print("--- 业务线利润表刷新完成 ---")
    
    print("利润表刷新流程全部完成")
