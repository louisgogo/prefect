from prefect import task
from mypackage.utilities import connect_to_db
from mypackage.mapping import reverse_combined_column_mapping
from ..utils import insert_to_staging_table
import pandas as pd
import numpy as np
import os
import sys

# 导入本地配置
from ..config import get_bus_lines

@task(name="3-无归属业务线拆分", retries=1, log_prints=True)
def run_unassigned_split_task(date_range):
    print("开始执行: 3-无归属业务线数据拆分(入库中间表)")
    
    bus_lines = get_bus_lines()
    conn, cur = connect_to_db()
    
    try:
        # 获取无归属、公共层级的数据
        print("正在获取收入数据...")
        cur.execute("SELECT * FROM fact_revenue WHERE unique_lvl like '%无归属%'")
        df_revenue = pd.DataFrame(cur.fetchall(), columns=[reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description])
        df_revenue = df_revenue[df_revenue['会计期间'].isin(date_range)]
        if not df_revenue.empty:
            df_revenue = df_revenue.drop(['一级组织', '二级组织', '三级组织'], axis=1, errors='ignore')
            df_revenue[['一级组织', '二级组织', '三级组织']] = df_revenue['唯一层级'].str.split('-', n=2, expand=True)
            df_revenue = df_revenue[['来源编号', '一级组织', '二级组织', '三级组织', '会计期间', '收入大类', '产品大类', '物料名称', '不含税金额本位币', '成本金额', '运费成本', '关税成本', '软件成本', '唯一层级', '年份']]
            df_revenue[bus_lines] = np.nan
            # 保留 '不含税金额本位币' 列名，与 fact_revenue.amt_tax_exc_loc 对齐
            df_revenue['audit_status'] = 'PENDING'

        # 获取费用数据
        print("正在获取费用数据...")
        cur.execute("SELECT * FROM fact_expense WHERE unique_lvl like '%无归属%'")
        df_expense = pd.DataFrame(cur.fetchall(), columns=[reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description])
        df_expense = df_expense[df_expense['会计期间'].isin(date_range)]
        if not df_expense.empty:
            df_expense = df_expense.drop(['一级组织', '二级组织', '三级组织'], axis=1, errors='ignore')
            df_expense[['一级组织', '二级组织', '三级组织']] = df_expense['唯一层级'].str.split('-', n=2, expand=True)
            # fact_expense 列: 费用金额(exp_amt), 无 数据来源 列
            df_expense = df_expense[['来源编号', '一级组织', '二级组织', '三级组织', '唯一层级', '单据编号', '报销人', '摘要', '会计期间', '费用性质', '费用大类', '核算项目-费控', '研发项目', '项目编码', '费用金额', '年份', '分摊业务线']]
            df_expense = df_expense.dropna(subset=['费用金额'])
            df_expense = df_expense[df_expense['费用金额'] != 0]
            df_expense[bus_lines] = np.nan
            df_expense['audit_status'] = 'PENDING'

        # 组织映射数据用于 insert_to_staging_table
        cur.execute("SELECT distinct unique_lvl, short_name FROM dim_org_struc")
        df_org = pd.DataFrame(cur.fetchall(), columns=['unique_lvl', 'short_name'])

        if not df_revenue.empty:
            insert_to_staging_table(df=df_revenue, df_org=df_org, groups=[], date_range=date_range, date_column='会计期间', table_name='staging_bus_revenue', bus_lines=bus_lines, is_split_others=True, is_by_df=True)
        if not df_expense.empty:
            insert_to_staging_table(df=df_expense, df_org=df_org, groups=[], date_range=date_range, date_column='会计期间', table_name='staging_bus_expense', bus_lines=bus_lines, is_split_others=True, is_by_df=True)
            
        print("✅ 3-无归属业务线拆分 数据入库完成！")
    finally:
        cur.close()
        conn.close()
