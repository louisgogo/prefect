#!/usr/bin/env python
"""
修复脚本：补充 staging_bus_expense 中缺失的后台人力费用及行政/人力分摊费用。

用法:
    cd /root/prefect
    source venv/bin/activate
    python scripts/fix_staging_expense_labor.py [YYYY-MM-DD] [YYYY-MM-DD]

如果不传参数，默认处理上个月。
只插入 staging_bus_expense 中不存在的记录，不会覆盖或删除已有数据。
"""

import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# 把项目根目录加入路径，以便导入 modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mypackage.mapping import reverse_combined_column_mapping
from mypackage.utilities import connect_to_db, read_data_bysql

from modules.bus_line_staging.config import get_bus_lines, groups_backend
from modules.bus_line_staging.utils import copy_data_to_postgres

# ---------- 配置：和 expense_tasks.py 保持一致 ----------
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
expense_categorys_2 = ["FYXM0012", "FYXM0014", "FYXM0016"]


def get_date_range(start_date=None, end_date=None):
    if start_date and end_date:
        return pd.date_range(start=start_date, end=end_date, freq="D").date
    current_date = datetime.now().date()
    first_day = current_date.replace(day=1)
    last_day = first_day - timedelta(days=1)
    first_day = last_day.replace(day=1)
    return pd.date_range(start=first_day, end=last_day, freq="D").date


def build_df_template(date_range, conn, cur):
    """复现 expense_tasks.py 中生成 df_template 的前半段逻辑。"""

    date_list = ",".join([f"'{d}'" for d in date_range])
    exp_text_1 = ",".join([f"'{c}'" for c in expense_categorys_1])
    exp_text_2 = ",".join([f"'{c}'" for c in expense_categorys_2])

    # 1) 行政/人力中心需要按人数比例分摊的费用
    cur.execute(
        f"""SELECT * FROM fact_expense
        WHERE acct_period IN ({date_list})
        AND dist_bus_line IS NULL
        AND ((exp_item_code IN ({exp_text_1}) AND unique_lvl LIKE '%行政中心%')
             OR (exp_item_code IN ({exp_text_2}) AND unique_lvl LIKE '%人力资源中心%'))"""
    )
    cols = [reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description]
    df_expense = pd.DataFrame(cur.fetchall(), columns=cols)

    bus_lines = get_bus_lines()
    if not df_expense.empty:
        df_expense = df_expense[~df_expense["分摊业务线"].isin(bus_lines)]
        id_exclude = df_expense["来源编号"].drop_duplicates().tolist()
    else:
        id_exclude = []

    # 2) 计算人数比例
    cur.execute(
        f"""
        SELECT * FROM fact_personnel
        WHERE date IN ({date_list})
        AND (unique_lvl NOT LIKE '%分公司%' OR third_org IN ('增值业务产品部','分公司运营支持部','通道合作部','银行合作部'))
        AND unique_lvl NOT LIKE '%AGI%'
        AND (unique_lvl NOT LIKE '%审核能力中心%' OR unique_lvl LIKE '%审核能力中心-运营中心%')
        AND class ='发薪人数'
        """
    )
    cols = [reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description]
    df_rate = pd.DataFrame(cur.fetchall(), columns=cols)

    if not df_rate.empty:
        df_rate_group = df_rate.groupby(["唯一层级", "日期"]).agg({"人数": "sum"}).reset_index()
        df_rate_group["月总人数"] = df_rate_group.groupby("日期")["人数"].transform("sum")
        df_rate_group["比重"] = df_rate_group["人数"] / df_rate_group["月总人数"]
        df_rate_group.rename(columns={"日期": "会计期间"}, inplace=True)
        df_rate_group = df_rate_group[["会计期间", "唯一层级", "比重"]]
    else:
        df_rate_group = pd.DataFrame(columns=["会计期间", "唯一层级", "比重"])

    # 3) 分摊逻辑 -> df_merged
    if not df_expense.empty and not df_rate_group.empty:
        df_merged = df_expense.merge(df_rate_group, on=["会计期间"], how="left")
        df_merged["费用金额"] = df_merged["费用金额"].astype(float) * df_merged["比重"].astype(float)
        df_merged = df_merged.drop(["比重"], axis=1).rename(
            columns={"唯一层级_y": "唯一层级", "唯一层级_x": "数据来源"}
        )
    else:
        df_merged = pd.DataFrame()

    # 4) 主费用数据（排除无归属和已分摊）
    id_exclude_str = ",".join([f"'{x}'" for x in id_exclude]) if id_exclude else "''"
    cur.execute(
        f"""SELECT * FROM fact_expense
        WHERE acct_period IN ({date_list})
        AND unique_lvl NOT LIKE '%无归属%'
        AND source_no NOT IN ({id_exclude_str})"""
    )
    cols = [reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    if not df.empty:
        df["数据来源"] = df["唯一层级"]

    # 5) 合并为 df_template
    df_template = pd.concat([df, df_merged], ignore_index=True) if not df_merged.empty else df
    if df_template.empty:
        return pd.DataFrame(), bus_lines, id_exclude

    # 保留必要列
    keep_cols = [
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
    df_template = df_template[[c for c in keep_cols if c in df_template.columns]].copy()

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
    df_template.rename(columns={"数据来源": "sec_dist_lvl"}, inplace=True)
    df_template["数据来源"] = df_template["sec_dist_lvl"]

    for col in bus_lines:
        df_template[col] = np.where(df_template["分摊业务线"] == col, 1, np.nan)

    return df_template, bus_lines, id_exclude


def build_df_labor_backend(df_template, bus_lines, id_exclude, date_range):
    """复现 expense_tasks.py 中 222~328 行的核心逻辑，生成 df_labor_backend。"""

    # 筛选人力费用和已分摊费用
    df_labor_source = df_template[
        (df_template["费用性质"] == "人力费用") | (df_template["来源编号"].isin(id_exclude))
    ].copy()

    if df_labor_source.empty:
        print("没有找到需要修复的人力费用或已分摊费用。")
        return pd.DataFrame()

    # 读取预算工资比例
    sql = "SELECT unique_lvl, bus_line, rate, date FROM fact_bus_wage_rate"
    df_labor = read_data_bysql(sql)
    if df_labor.empty:
        print("ERROR: fact_bus_wage_rate 表仍然没有数据，无法修复。请先更新该表。")
        return pd.DataFrame()

    df_labor["日期"] = pd.to_datetime(df_labor["日期"]).dt.date
    df_labor = df_labor[df_labor["日期"].isin(date_range)]
    if df_labor.empty:
        print("ERROR: fact_bus_wage_rate 表中没有目标月份的数据，无法修复。请先更新该表。")
        return pd.DataFrame()

    df_labor.rename(
        columns={"日期": "会计期间", "bus_line": "业务线", "unique_lvl": "唯一层级", "rate": "比例"},
        inplace=True,
    )

    # melt 打竖
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
        "数据来源",
        "分摊业务线",
    ]
    value_vars = [col for col in df_labor_source.columns if col in bus_lines]
    df_1 = df_labor_source.melt(
        id_vars=[c for c in id_vars if c in df_labor_source.columns],
        var_name="业务线",
        value_name="比例",
    ).drop("比例", axis=1)

    # merge 预算比例
    df_a = pd.merge(df_1, df_labor, how="left", on=["唯一层级", "业务线", "会计期间"])
    if df_a.empty:
        print("merge 后无数据。")
        return pd.DataFrame()

    # pivot 打横
    idx_cols = [c for c in id_vars if c in df_a.columns]
    df_labor_backend = df_a.pivot(columns="业务线", values="比例", index=idx_cols).reset_index()

    # 补充缺失列
    for c in set(df_template.columns) - set(df_labor_backend.columns):
        df_labor_backend[c] = np.nan

    # 填充分摊业务线
    for col in bus_lines:
        if col in df_labor_backend.columns:
            df_labor_backend[col] = np.where(
                df_labor_backend["分摊业务线"] == col, 1, df_labor_backend[col]
            )

    return df_labor_backend


def get_existing_keys(date_range, conn, cur):
    """查询 staging_bus_expense 当月已存在的 (来源编号, 唯一层级, 会计期间)。"""
    date_list = ",".join([f"'{d}'" for d in date_range])
    cur.execute(
        f"""
        SELECT DISTINCT 来源编号, 唯一层级, 会计期间::text
        FROM staging_bus_expense
        WHERE 会计期间 IN ({date_list})
        """
    )
    rows = cur.fetchall()
    # 转为 set 方便去重
    return set((str(r[0]), str(r[1]), str(r[2])) for r in rows)


def insert_missing(df, bus_lines, date_range, conn, cur):
    """去重后插入缺失记录。"""
    if df.empty:
        print("没有需要插入的新数据。")
        return 0

    # 只保留目标月份
    df = df[df["会计期间"].isin(date_range)].copy()
    if df.empty:
        print("目标月份无数据。")
        return 0

    # 获取表的所有列
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'staging_bus_expense'"
    )
    table_columns = [r[0] for r in cur.fetchall()]

    # 确保业务线列存在
    for col in bus_lines:
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # 添加审核状态
    df["审核状态"] = "PENDING"

    # 只保留表实际存在的列
    existing_cols = [col for col in table_columns if col in df.columns]
    df = df[existing_cols]

    # 金额列保留 3 位小数
    for col in existing_cols:
        if pd.api.types.is_float_dtype(df[col]) and col not in bus_lines:
            df[col] = df[col].round(3)

    # 归一化补偿（只对非空比例行）
    bus_line_cols = [c for c in existing_cols if c in bus_lines]
    if bus_line_cols:
        row_sums = df[bus_line_cols].sum(axis=1, skipna=True)
        has_bus_line = df[bus_line_cols].notna().any(axis=1)
        diffs = (1.0 - row_sums).round(2)
        mask = has_bus_line & (diffs != 0)
        if mask.any():
            max_cols = df.loc[mask, bus_line_cols].idxmax(axis=1)
            for idx in df[mask].index:
                col = max_cols.loc[idx]
                if pd.notna(col):
                    df.at[idx, col] = round(df.at[idx, col] + diffs.loc[idx], 2)

    copy_data_to_postgres(df, "staging_bus_expense", existing_cols, conn, cur)
    return len(df)


def main():
    if len(sys.argv) >= 3:
        start_date, end_date = sys.argv[1], sys.argv[2]
        date_range = get_date_range(start_date, end_date)
    else:
        date_range = get_date_range()

    print(f"目标日期范围: {date_range[0]} 到 {date_range[-1]}")

    conn, cur = connect_to_db()
    try:
        # 1) 构建 df_template
        df_template, bus_lines, id_exclude = build_df_template(date_range, conn, cur)
        if df_template.empty:
            print("没有需要处理的基础费用数据。")
            return

        # 2) 构建 df_labor_backend（只含人力+已分摊部分）
        df_labor_backend = build_df_labor_backend(df_template, bus_lines, id_exclude, date_range)
        if df_labor_backend.empty:
            print("没有生成需要修复的后台人力/分摊数据。")
            return

        print(f"根据 fact_bus_wage_rate 计算得到 {len(df_labor_backend)} 条待插入记录。")

        # 3) 查询已有记录并去重
        existing_keys = get_existing_keys(date_range, conn, cur)
        print(f"staging_bus_expense 中已存在 {len(existing_keys)} 条记录。")

        # 构造 key 用于去重
        df_labor_backend["_key"] = df_labor_backend.apply(
            lambda r: (str(r["来源编号"]), str(r["唯一层级"]), str(r["会计期间"])),
            axis=1,
        )
        df_to_insert = df_labor_backend[~df_labor_backend["_key"].isin(existing_keys)].copy()
        df_to_insert.drop(columns=["_key"], inplace=True)

        duplicate_count = len(df_labor_backend) - len(df_to_insert)
        if duplicate_count:
            print(f"其中 {duplicate_count} 条已存在于 staging_bus_expense 中，已跳过。")

        # 4) 插入缺失记录
        inserted = insert_missing(df_to_insert, bus_lines, date_range, conn, cur)
        print(f"成功插入 {inserted} 条缺失记录到 staging_bus_expense。")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
