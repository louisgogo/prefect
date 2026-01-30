"""收入、费用、利润明细生成流程"""
from prefect import flow
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from .prepare_data_flow import prepare_data_flow
from ..tasks.revenue_tasks import (
    load_revenue_data_task,
    process_manual_revenue_task,
    process_auto_revenue_task,
    merge_revenue_data_task,
    pivot_revenue_data_task,
    update_energy_hardware_task,
    validate_revenue_rate_task,
    apply_shared_rate_to_revenue_task,
    save_revenue_detail_task,
)
from ..tasks.expense_tasks import (
    load_expense_data_task,
    process_gap_expense_task,
    process_manual_expense_task,
    process_auto_expense_task,
    merge_expense_data_task,
    update_energy_hardware_expense_task,
    validate_expense_rate_task,
    apply_shared_rate_to_expense_task,
    save_expense_detail_task,
)
from ..tasks.profit_tasks import (
    load_profit_data_task,
    process_manual_profit_task,
    process_auto_profit_task,
    convert_expense_to_profit_task,
    convert_revenue_to_profit_task,
    load_offset_data_task,
    merge_profit_data_task,
    validate_profit_rate_task,
    save_profit_detail_task,
    process_shared_profit_task,
)


@flow(name="revenue_expense_profit_flow", log_prints=True)
def revenue_expense_profit_flow(
    date_range: pd.DatetimeIndex
) -> None:
    """
    收入、费用、利润明细生成流程
    
    Args:
        date_range: 日期范围
    """
    print("开始收入、费用、利润明细生成流程...")
    
    # 在 flow 内部获取数据（避免 DataFrame 序列化问题）
    df_upload_merge_all, df_org = prepare_data_flow()
    
    # ========== 收入明细生成 ==========
    print("--- 开始生成收入明细 ---")
    df_revenue = load_revenue_data_task(date_range)
    df_revenue_bus_hand = process_manual_revenue_task(df_revenue, df_upload_merge_all)
    df_revenue_bus_auto = process_auto_revenue_task(df_revenue, df_upload_merge_all, df_org)
    df_revenue_bus_all = merge_revenue_data_task(df_revenue_bus_hand, df_revenue_bus_auto)
    df_revenue_bus_all_to_profit = pivot_revenue_data_task(df_revenue_bus_all)
    df_revenue_bus_all_to_profit = update_energy_hardware_task(df_revenue_bus_all_to_profit)
    validate_revenue_rate_task(df_revenue_bus_all_to_profit)
    df_revenue_final = apply_shared_rate_to_revenue_task(df_revenue_bus_all_to_profit, date_range)
    save_revenue_detail_task(df_revenue_final, date_range)
    print("--- 收入明细生成完成 ---")
    
    # ========== 费用明细生成 ==========
    print("--- 开始生成费用明细 ---")
    df_expense = load_expense_data_task(date_range)
    df_expense_bus_gap = process_gap_expense_task(df_expense, df_upload_merge_all)
    df_expense_bus_hand = process_manual_expense_task(df_expense, df_upload_merge_all)
    df_expense_bus_auto = process_auto_expense_task(df_expense, df_upload_merge_all, df_org)
    df_expense_bus_all = merge_expense_data_task(
        df_expense_bus_hand, df_expense_bus_auto, df_expense_bus_gap
    )
    df_expense_bus_all = update_energy_hardware_expense_task(df_expense_bus_all)
    validate_expense_rate_task(df_expense_bus_all)
    df_expense_final = apply_shared_rate_to_expense_task(df_expense_bus_all, date_range)
    save_expense_detail_task(df_expense_final, date_range)
    print("--- 费用明细生成完成 ---")
    
    # ========== 利润明细生成 ==========
    print("--- 开始生成利润明细 ---")
    df_profit = load_profit_data_task(date_range)
    df_profit_bus_hand = process_manual_profit_task(df_profit, df_upload_merge_all)
    df_profit_bus_auto = process_auto_profit_task(df_profit, df_upload_merge_all, df_org)
    
    # 转换费用表和收入表为利润表格式
    df_expense_bus_all_to_profit = convert_expense_to_profit_task(df_expense_final)
    df_revenue_bus_all_to_profit = convert_revenue_to_profit_task(df_revenue_bus_all_to_profit)
    
    # 加载抵销数数据
    df_offset = load_offset_data_task(date_range)
    
    # 合并利润数据
    df_profit_bus_all = merge_profit_data_task(
        df_revenue_bus_all_to_profit,
        df_expense_bus_all_to_profit,
        pd.concat([df_profit_bus_hand, df_profit_bus_auto], ignore_index=True),
        df_offset
    )
    validate_profit_rate_task(df_profit_bus_all)
    save_profit_detail_task(df_profit_bus_all, date_range)
    print("--- 利润明细生成完成 ---")
    
    # ========== 处理公摊利润 ==========
    print("--- 开始处理公摊利润 ---")
    process_shared_profit_task(date_range)
    print("--- 公摊利润处理完成 ---")
    
    print("收入、费用、利润明细生成流程完成")
