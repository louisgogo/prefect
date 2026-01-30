"""资产明细生成流程（应收、存货、在途存货）"""
from prefect import flow
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from mypackage.utilities import connect_to_db
from ..tasks.asset_tasks import (
    load_receivable_data_task,
    process_receivable_task,
    validate_receivable_rate_task,
    save_receivable_detail_task,
    load_inventory_data_task,
    process_inventory_task,
    validate_inventory_rate_task,
    save_inventory_detail_task,
    load_inventory_on_way_data_task,
    process_inventory_on_way_task,
    validate_inventory_on_way_rate_task,
    save_inventory_on_way_detail_task,
)


@flow(name="asset_detail_flow", log_prints=True)
def asset_detail_flow(
    date_range: pd.DatetimeIndex
) -> None:
    """
    资产明细生成流程
    
    Args:
        date_range: 日期范围
    """
    print("开始资产明细生成流程...")
    
    # 获取组织架构和业务线比例数据
    conn, cur = connect_to_db()
    
    # 获取组织架构
    cur.execute("SELECT * FROM dim_org_struc")
    df_org = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    
    # 获取业务线比例数据
    cur.execute("SELECT * FROM fact_bus_line")
    df_bus_line = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    df_bus_line = df_bus_line.drop(['id'], axis=1)
    df_bus_line['category'] = df_bus_line['category'].replace(
        {'中台': '间接归属', '后台': '间接归属', '前台': '直接归属'}
    )
    
    cur.close()
    conn.close()
    
    # ========== 应收明细生成 ==========
    print("--- 开始生成应收明细 ---")
    df_ar = load_receivable_data_task(date_range)
    df_ar_bus_all = process_receivable_task(df_ar, df_bus_line, df_org)
    validate_receivable_rate_task(df_ar_bus_all)
    save_receivable_detail_task(df_ar_bus_all, date_range)
    print("--- 应收明细生成完成 ---")
    
    # ========== 存货明细生成 ==========
    print("--- 开始生成存货明细 ---")
    df_inv = load_inventory_data_task(date_range)
    df_inv_bus_all = process_inventory_task(df_inv, df_bus_line, df_org)
    validate_inventory_rate_task(df_inv_bus_all)
    save_inventory_detail_task(df_inv_bus_all, date_range)
    print("--- 存货明细生成完成 ---")
    
    # ========== 在途存货明细生成 ==========
    print("--- 开始生成在途存货明细 ---")
    df_inv_on = load_inventory_on_way_data_task(date_range)
    df_inv_on_bus_all = process_inventory_on_way_task(df_inv_on, df_bus_line, df_org)
    validate_inventory_on_way_rate_task(df_inv_on_bus_all)
    save_inventory_on_way_detail_task(df_inv_on_bus_all, date_range)
    print("--- 在途存货明细生成完成 ---")
    
    print("资产明细生成流程完成")
