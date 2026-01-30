"""业务线损益计算流程 - 业务线数据计算和利润表刷新"""
from prefect import flow
from typing import List, Optional
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from utils.date_utils import get_date_range_by_month, get_date_range_by_months
# 从同模块导入
from .revenue_expense_profit_flow import revenue_expense_profit_flow
from .asset_detail_flow import asset_detail_flow
# 从 profit_refresh 模块导入
from modules.profit_refresh.flows.profit_refresh_flow import profit_refresh_flow


@flow(name="business_line_profit_flow", log_prints=True)
def business_line_profit_flow(
    year: int,
    month: Optional[int] = None,
    months: Optional[List[int]] = None
) -> None:
    """
    业务线损益计算流程
    
    负责：
    1. 业务线数据计算（收入、费用、利润、资产明细）
    2. 利润表刷新
    
    Args:
        year: 年份
        month: 单个月份（1-12），如果提供则只处理该月
        months: 月份列表（1-12），如果提供则按月循环处理多个月份，例如 [10, 11, 12]
    
    Examples:
        # 处理单个月份（只处理 12 月）
        business_line_profit_flow(year=2025, month=12)
        
        # 批量处理多个月份（按月循环执行，避免内存溢出）
        business_line_profit_flow(year=2025, months=[10, 11, 12])
    """
    print(f"开始执行业务线损益计算流程，年份: {year}")

    # 确定要处理的月份列表
    if months is not None:
        # 批量处理多个月份（按月循环执行）
        # 将月份数字列表转换为 (year, month) 元组列表
        month_list = [(year, m) for m in months]
        print(f"批量处理模式，月份数: {len(months)}，将按月循环执行以避免内存溢出")
        print(f"处理月份: {', '.join([f'{year}年{m}月' for m in months])}")
    elif month is not None:
        # 只处理单个月份
        print(f"处理模式：只处理 {year}年{month}月")
        month_list = [(year, month)]
    else:
        raise ValueError("必须提供 month 或 months 参数")

    # 按月循环执行
    for idx, (process_year, process_month) in enumerate(month_list, 1):
        print(f"\n{'='*60}")
        print(
            f"开始处理第 {idx}/{len(month_list)} 个月：{process_year}年{process_month}月")
        print(f"{'='*60}")

        # 获取单个月份的日期范围
        date_range = get_date_range_by_month(process_year, process_month)
        print(f"日期范围: {date_range.min()} 到 {date_range.max()}")

        try:
            # 收入、费用、利润明细生成流程（内部会自己获取数据）
            revenue_expense_profit_flow(date_range)

            # 资产明细生成流程
            asset_detail_flow(date_range)

            print(f"✓ {process_year}年{process_month}月 处理完成")
        except Exception as e:
            print(f"✗ {process_year}年{process_month}月 处理失败: {str(e)}")
            raise  # 如果某个月失败，停止后续处理

    print(f"\n{'='*60}")
    print(f"业务线数据计算流程全部完成，共处理 {len(month_list)} 个月")
    print(f"{'='*60}")

    # ========== 利润表刷新流程 ==========
    # 在所有月份数据计算完成后，刷新利润表
    print(f"\n{'='*60}")
    print("开始利润表刷新流程（处理所有已计算的月份数据）")
    print(f"{'='*60}")

    # 合并所有月份的日期范围
    all_date_range = get_date_range_by_months(month_list)
    print(f"利润表刷新日期范围: {all_date_range.min()} 到 {all_date_range.max()}")

    try:
        profit_refresh_flow(all_date_range)
        print("✓ 利润表刷新完成")
    except Exception as e:
        print(f"✗ 利润表刷新失败: {str(e)}")
        raise

    print(f"\n{'='*60}")
    print("业务线损益计算流程全部完成")
    print(f"{'='*60}")
