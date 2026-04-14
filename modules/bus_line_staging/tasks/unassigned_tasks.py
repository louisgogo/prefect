import os
import sys

import numpy as np
import pandas as pd
from mypackage.mapping import reverse_combined_column_mapping
from mypackage.utilities import connect_to_db

from prefect import task

# 导入本地配置
from ..config import get_bus_lines, groups_frontend, groups_middle
from ..utils import insert_to_staging_table


@task(name="3-无归属业务线拆分", retries=1, log_prints=True)
def run_unassigned_split_task(date_range):
    print("开始执行: 3-无归属业务线数据拆分(入库中间表)")

    bus_lines = get_bus_lines()
    conn, cur = connect_to_db()

    try:
        date_list = ",".join([f"'{d}'" for d in date_range])

        # === 3.1 获取全部部门中非业务线的收入明细（SQL中过滤）===
        print("正在获取非业务线的收入数据...")
        cur.execute(
            "SELECT * FROM dim_org_struc WHERE bus_line='无' AND unique_lvl not like '%无归属%'"
        )
        df_org = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        # 不包含合伙人营销中心和审核能力中心
        df_org = df_org[~df_org["分组简称"].isin(groups_frontend)]
        levels = df_org["唯一层级"].drop_duplicates().tolist()
        levels_str = ",".join([f"'{x}'" for x in levels])

        cur.execute(
            f"""SELECT * FROM fact_revenue
            WHERE acct_period IN ({date_list})
            AND unique_lvl IN ({levels_str})"""
        )
        df = cur.fetchall()
        columns = [
            reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
        ]
        df_revenue = pd.DataFrame(df, columns=columns)
        if not df_revenue.empty:
            df_revenue = df_revenue.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
            df_revenue[["一级组织", "二级组织", "三级组织"]] = df_revenue["唯一层级"].str.split(
                "-", n=2, expand=True
            )
            df_revenue = df_revenue[
                [
                    "来源编号",
                    "唯一层级",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "会计期间",
                    "财报合并",
                    "收入大类",
                    "产品大类",
                    "物料名称",
                    "不含税金额本位币",
                    "成本金额",
                    "运费成本",
                    "关税成本",
                    "软件成本",
                ]
            ]
            df_revenue[bus_lines] = np.nan

        # === 3.2 获取前中台部门中无法归属业务线的费用数据（SQL中过滤）===
        print("正在获取非业务线的费用数据...")
        cur.execute(
            "SELECT * FROM dim_org_struc WHERE bus_line='无' AND (category='前台' or category='中台') AND unique_lvl not like '%无归属%'"
        )
        df = cur.fetchall()
        columns = [
            reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
        ]
        df_org_expense = pd.DataFrame(df, columns=columns)
        # 不包含合伙人营销中心和审核能力中心、中台
        df_org_expense = df_org_expense[
            (~df_org_expense["分组简称"].isin(groups_frontend))
            & (~df_org_expense["分组简称"].isin(groups_middle))
        ]
        levels_expense = df_org_expense["唯一层级"].drop_duplicates().tolist()
        levels_expense_str = ",".join([f"'{x}'" for x in levels_expense])

        cur.execute(
            f"""SELECT * FROM fact_expense
            WHERE acct_period IN ({date_list})
            AND unique_lvl IN ({levels_expense_str})"""
        )
        df = cur.fetchall()
        columns = [
            reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
        ]
        df_expense = pd.DataFrame(df, columns=columns)
        if not df_expense.empty:
            df_expense = df_expense.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
            df_expense[["一级组织", "二级组织", "三级组织"]] = df_expense["唯一层级"].str.split(
                "-", n=2, expand=True
            )
            df_expense = df_expense[
                [
                    "来源编号",
                    "唯一层级",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "会计期间",
                    "财报合并",
                    "费用大类",
                    "摘要",
                    "费用金额",
                    "分摊业务线",
                ]
            ]
            df_expense[bus_lines] = np.nan
            # 去掉零值
            df_expense = df_expense.dropna(subset=["费用金额"])
            df_expense = df_expense[df_expense["费用金额"] != 0]

        # === 3.3 获取其他利润表项目无法归属业务线的明细（SQL中过滤）===
        print("正在获取非业务线的其他利润表数据...")
        cur.execute(
            "SELECT * FROM dim_org_struc WHERE bus_line='无' AND unique_lvl not like '%无归属%'"
        )
        df = cur.fetchall()
        columns = [
            reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
        ]
        df_org_profit = pd.DataFrame(df, columns=columns)
        # 不包含合伙人营销中心和审核能力中心
        df_org_profit = df_org_profit[~df_org_profit["分组简称"].isin(groups_frontend)]
        levels_profit = df_org_profit["唯一层级"].drop_duplicates().tolist()
        levels_profit_str = ",".join([f"'{x}'" for x in levels_profit])

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
            AND unique_lvl IN ({levels_profit_str})
            AND prim_subj IN ({acct_str})"""
        )
        df = cur.fetchall()
        columns = [
            reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
        ]
        df_profit = pd.DataFrame(df, columns=columns)
        if not df_profit.empty:
            df_profit = df_profit.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
            df_profit[["一级组织", "二级组织", "三级组织"]] = df_profit["唯一层级"].str.split("-", n=2, expand=True)
            df_profit = df_profit[
                ["来源编号", "唯一层级", "一级组织", "二级组织", "三级组织", "日期", "财报合并", "一级科目", "本月金额"]
            ]
            df_profit[bus_lines] = np.nan
            # 去掉零值
            df_profit = df_profit.dropna(subset=["本月金额"])
            df_profit = df_profit[df_profit["本月金额"] != 0]

        # === 3.4 获取无归属、公共层级的数据（兜底数据，SQL中过滤）===
        print("正在获取无归属层级的收入数据...")
        cur.execute(
            f"""SELECT * FROM fact_revenue
            WHERE unique_lvl LIKE '%无归属%'
            AND acct_period IN ({date_list})"""
        )
        df_revenue_unassigned = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        if not df_revenue_unassigned.empty:
            df_revenue_unassigned = df_revenue_unassigned.drop(
                ["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore"
            )
            df_revenue_unassigned[["一级组织", "二级组织", "三级组织"]] = df_revenue_unassigned[
                "唯一层级"
            ].str.split("-", n=2, expand=True)
            df_revenue_unassigned = df_revenue_unassigned[
                [
                    "来源编号",
                    "唯一层级",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "会计期间",
                    "收入大类",
                    "产品大类",
                    "物料名称",
                    "不含税金额本位币",
                    "成本金额",
                    "关税成本",
                    "软件成本",
                    "年份",
                ]
            ]
            df_revenue_unassigned[bus_lines] = np.nan

        print("正在获取无归属层级的费用数据...")
        cur.execute(
            f"""SELECT * FROM fact_expense
            WHERE unique_lvl LIKE '%无归属%'
            AND acct_period IN ({date_list})"""
        )
        df_expense_unassigned = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        if not df_expense_unassigned.empty:
            df_expense_unassigned = df_expense_unassigned.drop(
                ["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore"
            )
            df_expense_unassigned[["一级组织", "二级组织", "三级组织"]] = df_expense_unassigned[
                "唯一层级"
            ].str.split("-", n=2, expand=True)
            df_expense_unassigned = df_expense_unassigned[
                [
                    "来源编号",
                    "唯一层级",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "单据编号",
                    "报销人",
                    "摘要",
                    "会计期间",
                    "费用性质",
                    "费用大类",
                    "核算项目-费控",
                    "研发项目",
                    "项目编码",
                    "费用金额",
                    "年份",
                    "分摊业务线",
                ]
            ]
            df_expense_unassigned = df_expense_unassigned.dropna(subset=["费用金额"])
            df_expense_unassigned = df_expense_unassigned[df_expense_unassigned["费用金额"] != 0]
            df_expense_unassigned[bus_lines] = np.nan

        # 组织映射数据用于 insert_to_staging_table
        cur.execute("SELECT distinct unique_lvl, short_name FROM dim_org_struc")
        df_org_mapping = pd.DataFrame(cur.fetchall(), columns=["unique_lvl", "short_name"])

        # 合并收入数据（非业务线收入 + 无归属收入）→ staging_bus_revenue
        revenue_list = []
        if not df_revenue.empty:
            df_revenue["数据来源"] = "非业务线收入"
            revenue_list.append(df_revenue)
        if not df_revenue_unassigned.empty:
            df_revenue_unassigned["数据来源"] = "无归属收入"
            revenue_list.append(df_revenue_unassigned)
        if revenue_list:
            df_revenue_all = pd.concat(revenue_list, ignore_index=True)
            insert_to_staging_table(
                df=df_revenue_all,
                df_org=df_org_mapping,
                groups=[],
                date_range=date_range,
                date_column="会计期间",
                table_name="staging_bus_revenue",
                bus_lines=bus_lines,
                is_split_others=True,
                is_by_df=True,
            )

        # 合并费用数据（非业务线费用 + 无归属费用）→ staging_bus_expense
        expense_list = []
        if not df_expense.empty:
            df_expense["数据来源"] = "非业务线费用"
            expense_list.append(df_expense)
        if not df_expense_unassigned.empty:
            df_expense_unassigned["数据来源"] = "无归属费用"
            expense_list.append(df_expense_unassigned)
        if expense_list:
            df_expense_all = pd.concat(expense_list, ignore_index=True)
            insert_to_staging_table(
                df=df_expense_all,
                df_org=df_org_mapping,
                groups=[],
                date_range=date_range,
                date_column="会计期间",
                table_name="staging_bus_expense",
                bus_lines=bus_lines,
                is_split_others=True,
                is_by_df=True,
            )

        # 合并其他利润表数据（非业务线其他 + 无归属其他）→ staging_bus_profit_bd
        profit_list = []
        if not df_profit.empty:
            df_profit["数据来源"] = "非业务线其他"
            profit_list.append(df_profit)
        if profit_list:
            df_profit_all = pd.concat(profit_list, ignore_index=True)
            insert_to_staging_table(
                df=df_profit_all,
                df_org=df_org_mapping,
                groups=[],
                date_range=date_range,
                date_column="日期",
                table_name="staging_bus_profit_bd",
                bus_lines=bus_lines,
                is_split_others=True,
                is_by_df=True,
            )

        print("✅ 3-无归属业务线拆分 数据入库完成！")
    finally:
        cur.close()
        conn.close()
