"""综合比例计算流程"""
from prefect import flow
import pandas as pd
import sys
import os
from typing import Optional, List
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.date_utils import get_date_range_by_month, get_date_range_by_months
from ..tasks.shared_rate_tasks import (
    load_bus_profit_for_shared_rate_task,
    load_personnel_for_shared_rate_task,
    load_human_cost_for_shared_rate_task,
    calculate_personnel_allocation_task,
    calculate_comprehensive_rate_task,
    save_shared_rate_task,
)


@flow(name="calculate_shared_rate_flow", log_prints=True)
def calculate_shared_rate_flow(
    year: int,
    month: Optional[int] = None,
    months: Optional[List[int]] = None
) -> None:
    """
    综合比例计算流程
    支持按月循环处理，每个月份单独计算综合比例

    Args:
        year: 年份
        month: 单个月份（1-12），如果提供则只处理该月
        months: 月份列表（1-12），如果提供则按月循环处理多个月份，例如 [10, 11, 12]

    Examples:
        # 处理单个月份（只处理 12 月）
        calculate_shared_rate_flow(year=2025, month=12)

        # 批量处理多个月份（按月循环执行，避免内存溢出）
        calculate_shared_rate_flow(year=2025, months=[10, 11, 12])
    """
    print(f"开始综合比例计算流程，年份: {year}")

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
            # 1. 加载业务线利润数据
            print("--- 开始加载业务线利润数据 ---")
            df_profit = load_bus_profit_for_shared_rate_task(date_range)

            # 2. 加载人数数据
            print("--- 开始加载人数数据 ---")
            df_personnel = load_personnel_for_shared_rate_task(date_range)

            # 3. 加载人力费用比例数据
            print("--- 开始加载人力费用比例数据 ---")
            df_human_cost = load_human_cost_for_shared_rate_task(date_range)

            # 4. 计算人数业务线分配
            print("--- 开始计算人数业务线分配 ---")
            df_personnel_allocation = calculate_personnel_allocation_task(
                df_personnel, df_human_cost
            )

            # 5. 计算综合比例
            print("--- 开始计算综合比例 ---")
            df_shared_rate = calculate_comprehensive_rate_task(
                df_profit, df_personnel_allocation
            )

            # 6. 保存结果
            print("--- 开始保存综合比例 ---")
            save_shared_rate_task(df_shared_rate, date_range)

            print(f"✓ {process_year}年{process_month}月 综合比例计算完成")
        except Exception as e:
            print(f"✗ {process_year}年{process_month}月 综合比例计算失败: {str(e)}")
            raise  # 如果某个月份计算失败，停止整个流程

    print(f"\n{'='*60}")
    print(f"综合比例计算流程全部完成，共处理 {len(month_list)} 个月")
    print(f"{'='*60}")
