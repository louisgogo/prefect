import os
import sys
from collections.abc import Sequence

import numpy as np
import pandas as pd
from mypackage.mapping import reverse_combined_column_mapping
from mypackage.utilities import connect_to_db
from prefect import task

from ..config import get_bus_lines, groups_frontend
from ..fact_assignments import legacy_fact_filter
from ..utils import insert_to_staging_table


def select_in_transit_inventory_rows(df: pd.DataFrame) -> pd.DataFrame:
    unreceived = pd.to_numeric(df["未入库数量"], errors="coerce").fillna(0)
    return df[(df["业务线"].isin(["无", "小POS"])) & unreceived.ne(0)].copy()


def calculate_in_transit_order_amount(df: pd.DataFrame) -> pd.Series:
    unreceived = pd.to_numeric(df["未入库数量"], errors="coerce")
    unit_price = pd.to_numeric(df["单价"], errors="coerce")
    exchange_rate = pd.to_numeric(df["汇率"], errors="coerce").fillna(1)
    return unreceived * unit_price * exchange_rate


@task(name="4-存货应收拆分", log_prints=True)
def run_inv_ar_split_task(date_range, batch_id, modules: Sequence[str] | None = None):
    print("开始执行: 4-存货、应收账款数据拆分(入库中间表)")

    selected = (
        {"inventory", "receivable", "in_transit_inventory"} if modules is None else set(modules)
    )
    unknown = selected.difference({"inventory", "receivable", "in_transit_inventory"})
    if unknown:
        raise ValueError(f"资产拆分收到不支持的模块: {', '.join(sorted(unknown))}")

    bus_lines = get_bus_lines()
    conn, cur = connect_to_db()
    date_list = ",".join([f"'{d}'" for d in date_range])

    try:
        # 获取dim_org_struc - 包含业务线信息用于筛选
        cur.execute("SELECT * FROM dim_org_struc WHERE unique_lvl not like '%无归属%'")
        df_org = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        levels = df_org["唯一层级"].drop_duplicates().tolist()
        levels_str = ",".join([f"'{x}'" for x in levels])

        # 1. 存货数据
        # 根据示例：筛选业务线为'无'、'小POS'或'审核业务'的数据（SQL中过滤日期和层级）
        print("正在获取存货数据...")
        cur.execute(
            f"""SELECT * FROM fact_inventory
            WHERE unique_lvl NOT LIKE '%无归属%'
            AND acct_period IN ({date_list})
            AND unique_lvl IN ({levels_str})
            AND {legacy_fact_filter()}"""
        )
        df_inv = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        # 根据唯一层级匹配业务线，筛选需要拆分的存货数据
        df_inv_org = pd.merge(df_inv, df_org[["唯一层级", "业务线", "分组简称"]], how="left", on="唯一层级")
        # 筛选业务线为'无'、'小POS'或'审核业务'的数据
        df_inv_matched = df_inv_org[df_inv_org["业务线"].isin(["无", "小POS", "审核业务"])]

        # Get groups before dropping the column
        inv_groups = []
        if not df_inv_matched.empty:
            inv_groups = df_inv_matched[df_inv_matched["分组简称"].notna()]["分组简称"].unique()
            inv_groups = [g for g in inv_groups if g in groups_frontend]

        if not df_inv_matched.empty:
            df_inv_matched = df_inv_matched.drop(["业务线", "分组简称"], axis=1, errors="ignore")
            df_inv_matched = df_inv_matched.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
            df_inv_matched[["一级组织", "二级组织", "三级组织"]] = df_inv_matched["唯一层级"].str.split(
                "-", n=2, expand=True
            )
            # 选择特定列，参考示例文件中的列选择
            df_inv = df_inv_matched[
                [
                    "来源编号",
                    "财报合并",
                    "财报单体",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "物料编码",
                    "物料名称",
                    "存货类别",
                    "客户类别",
                    "客户编码",
                    "客户名称",
                    "仓库",
                    "是否为备货物料",
                    "数量(库存)",
                    "参考价(基本)",
                    "参考金额",
                    "6个月以内数量",
                    "6个月以内金额",
                    "6-9个月数量",
                    "6-9个月金额",
                    "9个月-1年数量",
                    "9个月-1年金额",
                    "1-2年数量",
                    "1-2年金额",
                    "2-3年数量",
                    "2-3年金额",
                    "3年以上数量",
                    "3年以上金额",
                    "会计期间",
                    "唯一层级",
                ]
            ]
            df_inv["年份"] = pd.to_datetime(df_inv["会计期间"]).dt.year
            df_inv[bus_lines] = np.nan
            df_inv["数据来源"] = df_inv["唯一层级"]
        else:
            df_inv = pd.DataFrame()

        # 2. 应收账款数据
        # 根据示例：筛选业务线为'无'、'小POS'或'审核业务'的数据，删除内部关联方及余额为0的数据（SQL中过滤）
        print("正在获取应收账款数据...")
        cur.execute(
            f"""SELECT * FROM fact_receivable
            WHERE unique_lvl NOT LIKE '%无归属%'
            AND acct_period IN ({date_list})
            AND unique_lvl IN ({levels_str})
            AND txn_nature != '内部关联方往来'
            AND ar_balance != 0
            AND {legacy_fact_filter()}"""
        )
        df_ar = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        # 根据唯一层级匹配业务线
        df_ar_org = pd.merge(df_ar, df_org[["唯一层级", "业务线", "分组简称"]], how="left", on="唯一层级")
        # 筛选业务线为'无'、'小POS'或'审核业务'的数据，删除内部关联方及余额为0的数据
        df_ar_matched = df_ar_org[
            (df_ar_org["业务线"].isin(["无", "小POS", "审核业务"]))
            & (df_ar_org["往来性质"] != "内部关联方往来")
            & (df_ar_org["应收账款余额"] != 0)
        ]

        # Get groups before dropping the column
        ar_groups = []
        if not df_ar_matched.empty:
            ar_groups = df_ar_matched[df_ar_matched["分组简称"].notna()]["分组简称"].unique()
            ar_groups = [g for g in ar_groups if g in groups_frontend]

        if not df_ar_matched.empty:
            df_ar_matched = df_ar_matched.drop(["业务线", "分组简称"], axis=1, errors="ignore")
            df_ar_matched = df_ar_matched.drop(["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore")
            df_ar_matched[["一级组织", "二级组织", "三级组织"]] = df_ar_matched["唯一层级"].str.split(
                "-", n=2, expand=True
            )
            # 选择特定列，参考示例文件中的列选择
            df_ar = df_ar_matched[
                [
                    "来源编号",
                    "财报合并",
                    "财报单体",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "一级科目",
                    "客户编码",
                    "往来单位",
                    "往来性质",
                    "客户类型",
                    "销售部门",
                    "销售区域",
                    "赊销未核金额",
                    "预收未核金额",
                    "分期未核金额",
                    "应收账款余额",
                    "逾期金额",
                    "未到期金额",
                    "逾期30天以内金额",
                    "逾期30天到90天金额",
                    "逾期90天到180天金额",
                    "逾期180天到360天金额",
                    "逾期360天以上金额",
                    "账龄3个月以内",
                    "账龄3-6个月",
                    "账龄6-9个月",
                    "账龄9-12个月",
                    "账龄1-2年",
                    "账龄2-3年",
                    "账龄3年以上",
                    "本年借方发生额",
                    "本年贷方发生额",
                    "销售模块",
                    "上个月逾期金额",
                    "逾期变动",
                    "本年回款金额",
                    "会计期间",
                    "唯一层级",
                    "业务大类",
                    "业务小类",
                    "应收状态",
                    "备注",
                ]
            ]
            df_ar["年份"] = pd.to_datetime(df_ar["会计期间"]).dt.year
            df_ar[bus_lines] = np.nan
            df_ar["数据来源"] = df_ar["唯一层级"]
        else:
            df_ar = pd.DataFrame()

        # 3. 在途存货数据
        # 只保留仍有未入库数量的在途记录；订单金额不再作为源字段或筛选条件。
        print("正在获取在途存货数据...")
        cur.execute(
            f"""SELECT * FROM fact_inventory_on_way
            WHERE unique_lvl NOT LIKE '%无归属%'
            AND acct_period IN ({date_list})
            AND unique_lvl IN ({levels_str})
            AND COALESCE(unreceived_inventory, 0) <> 0
            AND {legacy_fact_filter()}"""
        )
        df_transit = pd.DataFrame(
            cur.fetchall(),
            columns=[
                reverse_combined_column_mapping.get(desc[0], desc[0]) for desc in cur.description
            ],
        )
        # 根据唯一层级匹配业务线
        df_transit_org = pd.merge(
            df_transit, df_org[["唯一层级", "业务线", "分组简称"]], how="left", on="唯一层级"
        )
        # 筛选业务线为“无”或“小POS”且仍有未入库数量的数据。
        df_transit_matched = select_in_transit_inventory_rows(df_transit_org)

        # Get groups before dropping the column
        transit_groups = []
        if not df_transit_matched.empty:
            transit_groups = df_transit_matched[df_transit_matched["分组简称"].notna()]["分组简称"].unique()
            transit_groups = [g for g in transit_groups if g in groups_frontend]

        if not df_transit_matched.empty:
            df_transit_matched = df_transit_matched.drop(["业务线", "分组简称"], axis=1, errors="ignore")
            df_transit_matched = df_transit_matched.drop(
                ["一级组织", "二级组织", "三级组织"], axis=1, errors="ignore"
            )
            df_transit_matched[["一级组织", "二级组织", "三级组织"]] = df_transit_matched["唯一层级"].str.split(
                "-", n=2, expand=True
            )
            # 计算在途订单金额：未入库数量 * 单价 * 汇率（汇率空白视为1；单价空白为赠品，结果为空）
            df_transit_matched["在途订单金额"] = calculate_in_transit_order_amount(df_transit_matched)
            # 选择特定列，参考示例文件中的列选择
            df_transit = df_transit_matched[
                [
                    "来源编号",
                    "财报合并",
                    "财报单体",
                    "一级组织",
                    "二级组织",
                    "三级组织",
                    "订单号",
                    "订单日期",
                    "供应商编码",
                    "供应商名称",
                    "存货类别",
                    "物料编码",
                    "物料名称",
                    "在途订单金额",
                    "订单数量",
                    "未入库数量",
                    "交货日期",
                    "会计期间",
                    "唯一层级",
                ]
            ]
            df_transit["年份"] = pd.to_datetime(df_transit["会计期间"]).dt.year
            df_transit[bus_lines] = np.nan
            df_transit["数据来源"] = df_transit["唯一层级"]
        else:
            df_transit = pd.DataFrame()

        # 组织映射 - 只包含有所需业务线数据的中心
        cur.execute(
            "SELECT distinct unique_lvl, prim_org, sec_org, short_name, category FROM dim_org_struc"
        )
        df_path = pd.DataFrame(
            cur.fetchall(), columns=["unique_lvl", "prim_org", "sec_org", "short_name", "category"]
        )

        # 插入存货数据
        if "inventory" in selected and not df_inv.empty:
            if len(inv_groups) > 0:
                insert_to_staging_table(
                    df=df_inv,
                    df_org=df_path,
                    groups=list(inv_groups),
                    date_range=date_range,
                    date_column="会计期间",
                    table_name="staging_bus_inventory",
                    bus_lines=bus_lines,
                    batch_id=batch_id,
                    is_split_others=True,
                    is_by_df=True,
                )
            else:
                # 如果没有匹配的分组，全部放入others
                insert_to_staging_table(
                    df=df_inv,
                    df_org=df_path,
                    groups=[],
                    date_range=date_range,
                    date_column="会计期间",
                    table_name="staging_bus_inventory",
                    bus_lines=bus_lines,
                    batch_id=batch_id,
                    is_split_others=True,
                    is_by_df=True,
                )

        # 插入应收账款数据
        if "receivable" in selected and not df_ar.empty:
            if len(ar_groups) > 0:
                insert_to_staging_table(
                    df=df_ar,
                    df_org=df_path,
                    groups=list(ar_groups),
                    date_range=date_range,
                    date_column="会计期间",
                    table_name="staging_bus_receivable",
                    bus_lines=bus_lines,
                    batch_id=batch_id,
                    is_split_others=True,
                    is_by_df=True,
                )
            else:
                insert_to_staging_table(
                    df=df_ar,
                    df_org=df_path,
                    groups=[],
                    date_range=date_range,
                    date_column="会计期间",
                    table_name="staging_bus_receivable",
                    bus_lines=bus_lines,
                    batch_id=batch_id,
                    is_split_others=True,
                    is_by_df=True,
                )

        # 插入在途存货数据
        if "in_transit_inventory" in selected and not df_transit.empty:
            if len(transit_groups) > 0:
                insert_to_staging_table(
                    df=df_transit,
                    df_org=df_path,
                    groups=list(transit_groups),
                    date_range=date_range,
                    date_column="会计期间",
                    table_name="staging_bus_in_transit_inventory",
                    bus_lines=bus_lines,
                    batch_id=batch_id,
                    is_split_others=True,
                    is_by_df=True,
                )
            else:
                insert_to_staging_table(
                    df=df_transit,
                    df_org=df_path,
                    groups=[],
                    date_range=date_range,
                    date_column="会计期间",
                    table_name="staging_bus_in_transit_inventory",
                    bus_lines=bus_lines,
                    batch_id=batch_id,
                    is_split_others=True,
                    is_by_df=True,
                )

        print("✅ 4-存货、应收、在途存货拆分 入库完成！")
    finally:
        cur.close()
        conn.close()
