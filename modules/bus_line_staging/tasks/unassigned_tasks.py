from collections.abc import Sequence

import numpy as np
import pandas as pd
from mypackage.mapping import reverse_combined_column_mapping
from mypackage.utilities import connect_to_db
from prefect import task

from ..config import get_bus_lines, groups_frontend, groups_middle
from ..fact_assignments import legacy_fact_filter
from ..utils import insert_to_staging_table
from .expense_tasks import _set_expense_source_metadata


def _mapped_frame(cur) -> pd.DataFrame:
    return pd.DataFrame(
        cur.fetchall(),
        columns=[reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description],
    )


def _quoted_values(values) -> str:
    return ",".join([f"'{value}'" for value in values])


def _load_unassigned_revenue(cur, date_list: str, bus_lines: Sequence[str]) -> pd.DataFrame:
    print("正在获取非业务线的收入数据...")
    cur.execute("SELECT * FROM dim_org_struc WHERE bus_line='无' AND unique_lvl not like '%无归属%'")
    df_org = _mapped_frame(cur)
    df_org = df_org[~df_org["分组简称"].isin(groups_frontend)]
    levels_str = _quoted_values(df_org["唯一层级"].drop_duplicates().tolist())
    cur.execute(
        f"""SELECT * FROM fact_revenue
        WHERE acct_period IN ({date_list})
        AND unique_lvl IN ({levels_str})
        AND {legacy_fact_filter()}"""
    )
    frame = _mapped_frame(cur)
    if frame.empty:
        return frame
    frame = frame.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
    frame[["一级组织", "二级组织", "三级组织"]] = frame["唯一层级"].str.split("-", n=2, expand=True)
    frame = frame[
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
    frame[list(bus_lines)] = np.nan
    return frame


def _load_unassigned_expense(cur, date_list: str, bus_lines: Sequence[str]) -> pd.DataFrame:
    print("正在获取非业务线的费用数据...")
    cur.execute(
        "SELECT * FROM dim_org_struc WHERE bus_line='无' "
        "AND (category='前台' or category='中台') AND unique_lvl not like '%无归属%'"
    )
    df_org = _mapped_frame(cur)
    df_org = df_org[(~df_org["分组简称"].isin(groups_frontend)) & (~df_org["分组简称"].isin(groups_middle))]
    levels_str = _quoted_values(df_org["唯一层级"].drop_duplicates().tolist())
    cur.execute(
        f"""SELECT * FROM fact_expense
        WHERE acct_period IN ({date_list})
        AND unique_lvl IN ({levels_str})
        AND {legacy_fact_filter()}
        AND NULLIF(BTRIM(dist_bus_line), '') IS NULL"""
    )
    frame = _mapped_frame(cur)
    if frame.empty:
        return frame
    frame = frame.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
    frame[["一级组织", "二级组织", "三级组织"]] = frame["唯一层级"].str.split("-", n=2, expand=True)
    frame = frame[
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
    frame[list(bus_lines)] = np.nan
    frame = frame.dropna(subset=["费用金额"])
    return frame[frame["费用金额"] != 0]


def _load_unassigned_profit(cur, date_list: str, bus_lines: Sequence[str]) -> pd.DataFrame:
    print("正在获取非业务线的其他利润表数据...")
    cur.execute("SELECT * FROM dim_org_struc WHERE bus_line='无' AND unique_lvl not like '%无归属%'")
    df_org = _mapped_frame(cur)
    df_org = df_org[~df_org["分组简称"].isin(groups_frontend)]
    levels_str = _quoted_values(df_org["唯一层级"].drop_duplicates().tolist())
    acct_str = _quoted_values(
        [
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
    )
    cur.execute(
        f"""SELECT * FROM fact_profit_bd
        WHERE date IN ({date_list})
        AND unique_lvl IN ({levels_str})
        AND prim_subj IN ({acct_str})
        AND {legacy_fact_filter()}"""
    )
    frame = _mapped_frame(cur)
    if frame.empty:
        return frame
    frame = frame.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
    frame[["一级组织", "二级组织", "三级组织"]] = frame["唯一层级"].str.split("-", n=2, expand=True)
    frame = frame[
        [
            "来源编号",
            "唯一层级",
            "一级组织",
            "二级组织",
            "三级组织",
            "日期",
            "财报合并",
            "一级科目",
            "本月金额",
        ]
    ]
    frame[list(bus_lines)] = np.nan
    frame = frame.dropna(subset=["本月金额"])
    return frame[frame["本月金额"] != 0]


@task(name="3-无归属业务线拆分", log_prints=True)
def run_unassigned_split_task(date_range, batch_id, modules: Sequence[str] | None = None):
    print("开始执行: 3-无归属业务线数据拆分(入库中间表)")
    selected = {"expense", "revenue", "profit_other"} if modules is None else set(modules)
    unknown = selected.difference({"expense", "revenue", "profit_other"})
    if unknown:
        raise ValueError(f"无归属拆分收到不支持的模块: {', '.join(sorted(unknown))}")

    bus_lines = get_bus_lines()
    conn, cur = connect_to_db()
    try:
        date_list = _quoted_values(date_range)
        frames = {}
        if "revenue" in selected:
            frames["revenue"] = _load_unassigned_revenue(cur, date_list, bus_lines)
        if "expense" in selected:
            frames["expense"] = _load_unassigned_expense(cur, date_list, bus_lines)
        if "profit_other" in selected:
            frames["profit_other"] = _load_unassigned_profit(cur, date_list, bus_lines)

        print("按用户要求，跳过无归属层级数据的获取...")
        cur.execute("SELECT distinct unique_lvl, short_name FROM dim_org_struc")
        df_org_mapping = pd.DataFrame(cur.fetchall(), columns=["unique_lvl", "short_name"])

        if "revenue" in frames and not frames["revenue"].empty:
            frames["revenue"]["数据来源"] = frames["revenue"]["唯一层级"]
            insert_to_staging_table(
                df=frames["revenue"],
                df_org=df_org_mapping,
                groups=[],
                date_range=date_range,
                date_column="会计期间",
                table_name="staging_bus_revenue",
                bus_lines=bus_lines,
                batch_id=batch_id,
                is_split_others=True,
                is_by_df=True,
            )
        if "expense" in frames and not frames["expense"].empty:
            frames["expense"] = _set_expense_source_metadata(frames["expense"], "唯一层级")
            insert_to_staging_table(
                df=frames["expense"],
                df_org=df_org_mapping,
                groups=[],
                date_range=date_range,
                date_column="会计期间",
                table_name="staging_bus_expense",
                bus_lines=bus_lines,
                batch_id=batch_id,
                is_split_others=True,
                is_by_df=True,
            )
        if "profit_other" in frames and not frames["profit_other"].empty:
            frames["profit_other"]["数据来源"] = frames["profit_other"]["唯一层级"]
            insert_to_staging_table(
                df=frames["profit_other"],
                df_org=df_org_mapping,
                groups=[],
                date_range=date_range,
                date_column="日期",
                table_name="staging_bus_profit_bd",
                bus_lines=bus_lines,
                batch_id=batch_id,
                is_split_others=True,
                is_by_df=True,
            )
        print("✅ 3-无归属业务线拆分 数据入库完成！")
    finally:
        cur.close()
        conn.close()
