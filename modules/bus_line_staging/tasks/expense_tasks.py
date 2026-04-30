import os
import sys

import numpy as np
import pandas as pd
from mypackage.mapping import reverse_combined_column_mapping
from mypackage.utilities import connect_to_db, read_data_bysql

from prefect import task

# 导入本地配置
from ..config import get_bus_lines, groups_backend, groups_frontend, groups_middle
from ..utils import insert_to_staging_table


@task(name="1-前中后台费用数据拆分", retries=1, log_prints=True)
def run_expense_split_to_staging_task(date_range):
    print("开始执行业务线费用数据库拆分入库作业...")

    bus_lines = get_bus_lines()
    conn, cur = connect_to_db()

    # 费用科目定义
    expense_categorys_1 = [
        "FYXM0012",
        "FYXM0014",
        "FYXM0016",
        "FYXM0024",
        "FYXM0025",
        "FYXM0026",
        "FYXM0027",
        "FYXM0028",
        "FYXM0029",
        "FYXM0031",
        "FYXM0072",
        "FYXM0073",
    ]
    expense_categorys_text_1 = ",".join([f"'{item}'" for item in expense_categorys_1])
    expense_categorys_2 = ["FYXM0012", "FYXM0014", "FYXM0016"]
    expense_categorys_text_2 = ",".join([f"'{item}'" for item in expense_categorys_2])

    # 获取原始费用数据（直接在SQL中过滤日期，减少数据传输）
    date_list = ",".join([f"'{d}'" for d in date_range])
    cur.execute(
        f"""SELECT * FROM fact_expense
        WHERE acct_period IN ({date_list})
        AND dist_bus_line IS NULL
        AND ((exp_item_code IN ({expense_categorys_text_1}) AND unique_lvl LIKE '%行政中心%')
             OR (exp_item_code IN ({expense_categorys_text_2}) AND unique_lvl LIKE '%人力资源中心%'))"""
    )
    columns = [reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description]
    df_expense = pd.DataFrame(cur.fetchall(), columns=columns)

    if not df_expense.empty:
        df_expense = df_expense[~df_expense["分摊业务线"].isin(bus_lines)]
        id_exclude = df_expense["来源编号"].drop_duplicates().tolist()
    else:
        id_exclude = []

    # 获取组织架构
    cur.execute(
        "SELECT distinct unique_lvl, prim_org, sec_org, third_org, short_name, category FROM dim_org_struc"
    )
    df_org_all = pd.DataFrame(
        cur.fetchall(),
        columns=["unique_lvl", "prim_org", "sec_org", "third_org", "short_name", "category"],
    )

    # 计算人数比例（直接在SQL中过滤日期）
    cur.execute(
        f"""
        SELECT * FROM fact_personnel
        WHERE date IN ({date_list})
        AND (unique_lvl NOT LIKE '%分公司%' OR third_org in ('增值业务产品部','分公司运营支持部','通道合作部','银行合作部'))
        AND unique_lvl NOT LIKE '%AGI%'
        AND (unique_lvl NOT LIKE '%审核能力中心%' OR unique_lvl LIKE '%审核能力中心-运营中心%')
        AND class ='发薪人数'
    """
    )
    columns = [reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description]
    df_rate = pd.DataFrame(cur.fetchall(), columns=columns)

    if not df_rate.empty:
        df_rate_group = df_rate.groupby(["唯一层级", "日期"]).agg({"人数": "sum"}).reset_index()
        df_rate_group["月总人数"] = df_rate_group.groupby("日期")["人数"].transform("sum")
        df_rate_group["比重"] = df_rate_group["人数"] / df_rate_group["月总人数"]
        df_rate_group.rename(columns={"日期": "会计期间"}, inplace=True)
        df_rate_group = df_rate_group[["会计期间", "唯一层级", "比重"]]
    else:
        df_rate_group = pd.DataFrame(columns=["会计期间", "唯一层级", "比重"])

    # 分摊逻辑
    if not df_expense.empty and not df_rate_group.empty:
        df_merged = df_expense.merge(df_rate_group, on=["会计期间"], how="left")
        df_merged["费用金额"] = df_merged["费用金额"].astype(float) * df_merged["比重"].astype(float)
        df_merged = df_merged.drop(["比重"], axis=1).rename(
            columns={"唯一层级_y": "唯一层级", "唯一层级_x": "数据来源"}
        )
    else:
        df_merged = pd.DataFrame()

    # 获取主费用数据（直接在SQL中过滤日期和无归属）
    id_exclude_str = ",".join([f"'{x}'" for x in id_exclude]) if id_exclude else "''"
    cur.execute(
        f"""SELECT * FROM fact_expense
        WHERE acct_period IN ({date_list})
        AND unique_lvl NOT LIKE '%无归属%'
        AND source_no NOT IN ({id_exclude_str})"""
    )
    columns = [reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=columns)

    if not df.empty:
        df["数据来源"] = df["唯一层级"]

    df_template = pd.concat([df, df_merged]) if not df_merged.empty else df
    if df_template.empty:
        print("没有符合维度的数据，跳过费用入库。")
        conn.close()
        return

    df_template = df_template.reset_index()[
        [
            "来源编号",
            "唯一层级",
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
            "数据来源",
            "分摊业务线",
        ]
    ]
    df_template[["一级组织", "二级组织", "三级组织"]] = (
        df_template["唯一层级"].astype(str).str.split("-", n=2, expand=True)
    )
    df_template["会计期间"] = pd.to_datetime(df_template["会计期间"])
    df_template["年份"] = df_template["会计期间"].dt.year
    df_template["会计期间"] = df_template["会计期间"].dt.date
    df_template = df_template[
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
            "数据来源",
            "分摊业务线",
        ]
    ]

    for col in bus_lines:
        df_template[col] = np.nan

    df_template = df_template.dropna(subset=["费用金额"])
    df_template = df_template[df_template["费用金额"] != 0]
    # 保留 '费用金额' 列名，与 fact_expense.exp_amt 对齐（通过 combined_column_mapping 映射）
    # 将 '数据来源' 显式重命名为 sec_dist_lvl（该字段不在 combined_column_mapping 中）
    df_template.rename(columns={"数据来源": "sec_dist_lvl"}, inplace=True)
    # 数据库表实际存在 "数据来源" 列，需要保留该列的值
    df_template["数据来源"] = df_template["sec_dist_lvl"]

    for col in bus_lines:
        df_template[col] = np.where(df_template["分摊业务线"] == col, 1, np.nan)

    date_column = "会计期间"
    table_name = "staging_bus_expense"

    # 分步入库
    print("开始处理 中台自身数据拆分...")
    df_middle = df_org_all[df_org_all.category.isin(["中台"])]
    insert_to_staging_table(
        df=df_template,
        df_org=df_middle,
        groups=groups_middle,
        date_range=date_range,
        date_column=date_column,
        table_name=table_name,
        bus_lines=bus_lines,
        is_split_others=False,
        is_by_df=False,
    )

    print("开始处理 后台自身数据拆分（不含人力等分摊费用）...")
    df_backend_org = df_org_all[df_org_all.category.isin(["后台"])]
    # 后台非人力费用：排除人力费用和已分摊的费用（通过id_exclude识别）
    df_backend = df_template[
        (df_template["费用性质"] != "人力费用") & (~df_template["来源编号"].isin(id_exclude))
    ].copy()
    if not df_backend.empty:
        # 添加集团列用于区分
        df_backend.loc[:, "集团"] = np.nan
        insert_to_staging_table(
            df=df_backend,
            df_org=df_backend_org,
            groups=groups_backend,
            date_range=date_range,
            date_column=date_column,
            table_name=table_name,
            bus_lines=bus_lines,
            is_split_others=True,
            is_by_df=False,
        )

    print("开始处理 后台自身数据拆分（含人力和分摊部分）...")
    sql = "SELECT unique_lvl, bus_line, rate, date FROM fact_bus_wage_rate"
    df_labor = read_data_bysql(sql)
    if not df_labor.empty:
        df_labor["日期"] = pd.to_datetime(df_labor["日期"]).dt.date
        df_labor = df_labor[df_labor["日期"].isin(date_range)]
        df_labor.rename(
            columns={"日期": "会计期间", "bus_line": "业务线", "unique_lvl": "唯一层级", "rate": "比例"},
            inplace=True,
        )

        # 筛选人力费用和已分摊的费用
        df_labor_source = df_template[
            (df_template["费用性质"] == "人力费用") | (df_template["来源编号"].isin(id_exclude))
        ].copy()
        if not df_labor_source.empty:
            # melt打竖：将业务线列转换为行
            id_vars = [
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
                "sec_dist_lvl",
                "分摊业务线",
            ]
            value_vars = [col for col in df_labor_source.columns if col in bus_lines]

            df_1 = df_labor_source.melt(id_vars=id_vars, var_name="业务线", value_name="比例").drop(
                "比例", axis=1
            )

            # 合并预算人力比例
            df_a = pd.merge(df_1, df_labor, how="left", on=["唯一层级", "业务线", "会计期间"])

            if not df_a.empty:
                # pivot打横：将业务线行转换回列
                idx_cols = [
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
                    "sec_dist_lvl",
                    "分摊业务线",
                ]
                df_labor_backend = df_a.pivot(
                    columns="业务线", values="比例", index=idx_cols
                ).reset_index()

                # 补充缺失的列（只补充df_template中有但df_labor_backend中没有的列）
                missing_cols = set(df_template.columns) - set(df_labor_backend.columns)
                for c in missing_cols:
                    df_labor_backend[c] = np.nan
                # 保留df_labor_backend中的所有列（包括业务线比例列），同时确保df_template中的列都存在
                # 按df_template的列顺序排列，但保留df_labor_backend额外的列（业务线比例）
                all_cols = list(df_template.columns) + [
                    c
                    for c in df_labor_backend.columns
                    if c not in df_template.columns and c in bus_lines
                ]
                df_labor_backend = df_labor_backend[
                    [c for c in all_cols if c in df_labor_backend.columns]
                ]

                # 填充分摊业务线的值（如果是分摊的费用，则对应业务线比例为1）
                for col in bus_lines:
                    df_labor_backend[col] = np.where(
                        df_labor_backend["分摊业务线"] == col, 1, df_labor_backend[col]
                    )

                # 后台人力费用统一处理，不分组（groups=[]），全部放入others
                insert_to_staging_table(
                    df=df_labor_backend,
                    df_org=df_backend_org,
                    groups=[],
                    date_range=date_range,
                    date_column=date_column,
                    table_name=table_name,
                    bus_lines=bus_lines,
                    is_split_others=True,
                    is_by_df=False,
                )

    print("开始处理 前台中需要单独拆分的业务线...")
    df_frontend = df_org_all[df_org_all.category.isin(["前台"])]
    insert_to_staging_table(
        df=df_template,
        df_org=df_frontend,
        groups=groups_frontend,
        date_range=date_range,
        date_column=date_column,
        table_name=table_name,
        bus_lines=bus_lines,
        is_split_others=False,
        is_by_df=True,
    )

    conn.close()
    print("=== 业务线费用据拆分入库完成. ===")
