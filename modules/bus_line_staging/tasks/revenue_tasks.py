import os
import sys

import numpy as np
import pandas as pd
from mypackage.mapping import reverse_combined_column_mapping
from mypackage.utilities import connect_to_db

from prefect import task

# 导入本地配置
from ..config import get_bus_lines, groups_frontend
from ..utils import insert_to_staging_table


@task(name="2-特定部门收入及其他拆分", retries=1, log_prints=True)
def run_revenue_other_split_task(date_range):
    print("开始执行: 2-特定部门的收入、其他数据拆分(入库中间表)")

    bus_lines = get_bus_lines()
    conn, cur = connect_to_db()

    try:
        date_list = ",".join([f"'{d}'" for d in date_range])

        # 获取全部层级
        cur.execute("SELECT * FROM dim_org_struc WHERE unique_lvl not like '%无归属%'")
        df_org = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        levels = df_org["唯一层级"].drop_duplicates().tolist()
        levels_str = ",".join([f"'{x}'" for x in levels])

        # 1. 收入数据（直接在SQL中过滤日期和层级）
        print("正在获取收入数据...")
        cur.execute(
            f"""SELECT * FROM fact_revenue
            WHERE acct_period IN ({date_list})
            AND unique_lvl IN ({levels_str})"""
        )
        df_revenue = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        if not df_revenue.empty:
            df_revenue = df_revenue.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
            df_revenue[["一级组织", "二级组织", "三级组织"]] = df_revenue["唯一层级"].str.split(
                "-", n=2, expand=True
            )
            df_revenue = df_revenue[
                [
                    "来源编号",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "会计期间",
                    "收入大类",
                    "产品大类",
                    "物料名称",
                    "不含税金额本位币",
                    "成本金额",
                    "运费成本",
                    "关税成本",
                    "软件成本",
                    "唯一层级",
                    "年份",
                ]
            ]
            df_revenue[bus_lines] = np.nan
            # 保留 '不含税金额本位币' 列名，与 fact_revenue.amt_tax_exc_loc 对齐

        # 2. 其他项目数据（直接在SQL中过滤日期、层级和科目）
        print("正在获取其他项目数据...")
        acct_list = [
            "信用减值损失",
            "公允价值变动收益",
            "其他收益",
            "所得税费用",
            "投资收益",
            "税金及附加",
            "营业外支出",
            "营业外收入",
            "资产减值损失",
            "资产处置收益",
        ]
        acct_str = ",".join([f"'{x}'" for x in acct_list])
        cur.execute(
            f"""SELECT * FROM fact_profit_bd
            WHERE date IN ({date_list})
            AND unique_lvl IN ({levels_str})
            AND prim_subj IN ({acct_str})"""
        )
        df_profit = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )

        if not df_profit.empty:
            df_profit["年份"] = pd.to_datetime(df_profit["日期"]).dt.year.astype(str)
            df_profit = df_profit.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
            df_profit[["一级组织", "二级组织", "三级组织"]] = df_profit["唯一层级"].str.split("-", n=2, expand=True)
            df_profit = df_profit[
                ["来源编号", "财报合并", "一级组织", "二级组织", "三级组织", "日期", "一级科目", "本月金额", "唯一层级", "年份"]
            ]
            df_profit[bus_lines] = np.nan
            df_profit = df_profit.dropna(subset=["本月金额"])
            df_profit = df_profit[df_profit["本月金额"] != 0]
            # 保留 '本月金额' 列名，与 fact_profit_bd.mo_amt 对齐
        # 组织映射
        cur.execute(
            "SELECT distinct unique_lvl, prim_org, sec_org, short_name, category FROM dim_org_struc"
        )
        df_path = pd.DataFrame(
            cur.fetchall(), columns=["unique_lvl", "prim_org", "sec_org", "short_name", "category"]
        )

        if not df_revenue.empty:
            insert_to_staging_table(
                df=df_revenue,
                df_org=df_path,
                groups=groups_frontend,
                date_range=date_range,
                date_column="会计期间",
                table_name="staging_bus_revenue",
                bus_lines=bus_lines,
                is_split_others=False,
                is_by_df=True,
            )
        if not df_profit.empty:
            insert_to_staging_table(
                df=df_profit,
                df_org=df_path,
                groups=groups_frontend,
                date_range=date_range,
                date_column="日期",
                table_name="staging_bus_profit_bd",
                bus_lines=bus_lines,
                is_split_others=False,
                is_by_df=True,
            )

        print("✅ 2-特定部门的收入、其他数据拆分 入库完成！")
    finally:
        cur.close()
        conn.close()
