from prefect import task
from mypackage.utilities import connect_to_db
from mypackage.mapping import reverse_combined_column_mapping
from ..utils import insert_to_staging_table
import pandas as pd
import numpy as np
import os
import sys

# 导入本地配置
from ..config import get_bus_lines, groups_frontend

@task(name="4-存货应收拆分", retries=1, log_prints=True)
def run_inv_ar_split_task(date_range):
    print("开始执行: 4-存货、应收账款数据拆分(入库中间表)")
    
    bus_lines = get_bus_lines()
    conn, cur = connect_to_db()
    
    try:
        # 获取dim_org_struc
        cur.execute("SELECT * FROM dim_org_struc WHERE unique_lvl not like '%无归属%'")
        df_org = pd.DataFrame(cur.fetchall(), columns=[reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description])
        levels = df_org['唯一层级'].drop_duplicates().tolist()
        
        # 1. 存货数据
        # fact_inventory 列名: ref_amt(参考金额), acct_period(会计期间), mat_name(物料名称)
        # 注意: fact_inventory 无 year 列，无 计价货币金额 列
        print("正在获取存货数据...")
        cur.execute("SELECT * FROM fact_inventory")
        df_inv = pd.DataFrame(cur.fetchall(), columns=[reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description])
        df_inv = df_inv[df_inv['会计期间'].isin(date_range)]
        df_inv = df_inv[df_inv['唯一层级'].isin(levels)]
        if not df_inv.empty:
            df_inv = df_inv.drop(['一级组织', '二级组织', '三级组织'], axis=1, errors='ignore')
            df_inv[['一级组织', '二级组织', '三级组织']] = df_inv['唯一层级'].str.split('-', n=2, expand=True)
            df_inv = df_inv[['来源编号', '一级组织', '二级组织', '三级组织', '会计期间', '物料名称', '参考金额', '唯一层级']]
            # fact_inventory 无 year 列，从 acct_period 派生
            df_inv['年份'] = pd.to_datetime(df_inv['会计期间']).dt.year
            df_inv[bus_lines] = np.nan
            # 保留 '参考金额' 列名，与 fact_inventory.ref_amt 对齐

        # 2. 应收账款数据
        # fact_receivable 列名: ar_balance(应收账款余额), acct_period(会计期间)
        # 注意: fact_receivable 无 year 列，无 客户名称/物料名称 列
        print("正在获取应收账款数据...")
        cur.execute("SELECT * FROM fact_receivable")
        df_ar = pd.DataFrame(cur.fetchall(), columns=[reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description])
        df_ar = df_ar[df_ar['会计期间'].isin(date_range)]
        df_ar = df_ar[df_ar['唯一层级'].isin(levels)]
        if not df_ar.empty:
            df_ar = df_ar.drop(['一级组织', '二级组织', '三级组织'], axis=1, errors='ignore')
            df_ar[['一级组织', '二级组织', '三级组织']] = df_ar['唯一层级'].str.split('-', n=2, expand=True)
            df_ar = df_ar[['来源编号', '一级组织', '二级组织', '三级组织', '会计期间', '应收账款余额', '唯一层级']]
            # fact_receivable 无 year 列，从 acct_period 派生
            df_ar['年份'] = pd.to_datetime(df_ar['会计期间']).dt.year
            df_ar[bus_lines] = np.nan
            # 保留 '应收账款余额' 列名，与 fact_receivable.ar_balance 对齐

        # 3. 在途存货数据
        # 实际表名: fact_inventory_on_way (非 fact_in_transit_inventory)
        # 列名: order_amount(订单金额), acct_period(会计期间), mat_name(物料名称)
        print("正在获取在途存货数据...")
        cur.execute("SELECT * FROM fact_inventory_on_way")
        df_transit = pd.DataFrame(cur.fetchall(), columns=[reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description])
        df_transit = df_transit[df_transit['会计期间'].isin(date_range)]
        df_transit = df_transit[df_transit['唯一层级'].isin(levels)]
        if not df_transit.empty:
            df_transit = df_transit.drop(['一级组织', '二级组织', '三级组织'], axis=1, errors='ignore')
            df_transit[['一级组织', '二级组织', '三级组织']] = df_transit['唯一层级'].str.split('-', n=2, expand=True)
            df_transit = df_transit[['来源编号', '一级组织', '二级组织', '三级组织', '会计期间', '物料名称', '订单金额', '唯一层级']]
            # fact_inventory_on_way 无 year 列，从 acct_period 派生
            df_transit['年份'] = pd.to_datetime(df_transit['会计期间']).dt.year
            df_transit[bus_lines] = np.nan
            # 保留 '订单金额' 列名，与 fact_inventory_on_way.order_amount 对齐

        # 组织映射
        cur.execute("SELECT distinct unique_lvl, prim_org, sec_org, short_name, category FROM dim_org_struc")
        df_path = pd.DataFrame(cur.fetchall(), columns=['unique_lvl', 'prim_org', 'sec_org', 'short_name', 'category'])
        
        if not df_inv.empty:
            insert_to_staging_table(df=df_inv, df_org=df_path, groups=groups_frontend, date_range=date_range, date_column='会计期间', table_name='staging_bus_inventory', bus_lines=bus_lines, is_split_others=False, is_by_df=True)
        if not df_ar.empty:
            insert_to_staging_table(df=df_ar, df_org=df_path, groups=groups_frontend, date_range=date_range, date_column='会计期间', table_name='staging_bus_receivable', bus_lines=bus_lines, is_split_others=False, is_by_df=True)
        if not df_transit.empty:
            insert_to_staging_table(df=df_transit, df_org=df_path, groups=groups_frontend, date_range=date_range, date_column='会计期间', table_name='staging_bus_in_transit_inventory', bus_lines=bus_lines, is_split_others=False, is_by_df=True)
            
        print("✅ 4-存货、应收、在途存货拆分 入库完成！")
    finally:
        cur.close()
        conn.close()
