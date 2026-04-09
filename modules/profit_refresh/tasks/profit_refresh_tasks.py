"""利润表刷新相关 Tasks"""
import os
import sys

import pandas as pd

from prefect import task

# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from mypackage.utilities import connect_to_db


@task(name="load_revenue_for_profit", log_prints=True)
def load_revenue_for_profit_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从 fact_revenue 读取收入数据并转换格式

    Args:
        date_range: 日期范围

    Returns:
        收入数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute("SELECT * FROM fact_revenue")
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        # 筛选日期范围
        df["acct_period"] = pd.to_datetime(df["acct_period"])
        df = df[df["acct_period"].isin(date_range)]

        # 收入表转换
        cols_rm = ["amt_tax_exc_loc", "cost_amt", "freight_cost", "soft_cost", "tariff_cost"]
        cols_kp = [col for col in df.columns if col not in cols_rm]
        df = pd.melt(df, id_vars=cols_kp, var_name="prim_subj", value_name="mo_amt")
        df = df[
            ["source_no", "fin_con", "fin_ind", "unique_lvl", "acct_period", "prim_subj", "mo_amt"]
        ]
        df["prim_subj"] = df["prim_subj"].replace(
            {
                "amt_tax_exc_loc": "营业收入",
                "cost_amt": "营业成本",
                "freight_cost": "营业成本",
                "soft_cost": "营业成本",
                "tariff_cost": "营业成本",
            }
        )
        df = df.rename(columns={"mo_amt": "amt"})
        df["class"] = "收入"

        print(f"加载收入数据完成，共 {len(df)} 条记录")
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"加载收入数据时发生错误: {str(e)}")
        raise


@task(name="load_expense_other_for_profit", log_prints=True)
def load_expense_other_for_profit_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从 fact_expense 和 fact_profit_bd 读取费用和其他数据

    Args:
        date_range: 日期范围

    Returns:
        费用和其他数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute(
            """
            SELECT source_no, fin_con, fin_ind, unique_lvl, acct_period, prim_subj, exp_amt as amt, '费用' as class
            FROM fact_expense
            WHERE acct_period >= %s AND acct_period <= %s
            UNION ALL
            SELECT source_no, fin_con, fin_ind, unique_lvl, date as acct_period, prim_subj, mo_amt as amt, '其他' as class
            FROM fact_profit_bd
            WHERE prim_subj NOT IN ('营业收入','营业成本','管理费用','销售费用','财务费用','研发费用','营业利润','净利润','利润总额','政府补贴','分摊税费','分摊收益','退税收入')
            AND date >= %s AND date <= %s
        """,
            (date_range.min(), date_range.max(), date_range.min(), date_range.max()),
        )
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        # 筛选日期范围
        df["acct_period"] = pd.to_datetime(df["acct_period"])
        df = df[df["acct_period"].isin(date_range)]

        print(f"加载费用和其他数据完成，共 {len(df)} 条记录")
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"加载费用和其他数据时发生错误: {str(e)}")
        raise


@task(name="refresh_offset_by_month", log_prints=True)
def refresh_offset_by_month_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从 fact_offset 重新计算月度抵销数，更新 fact_offset_by_month，并返回当期数据。

    Notebook 原始逻辑：
      1. 全量读取 fact_offset（累计数）
      2. 按 prim_subj / acct_period / unique_lvl 分组求和
      3. 用 diff() 将累计数转为当月数，1 月份直接取累计值
      4. 写入 fact_offset_by_month（全量覆盖）
      5. 筛选 date_range 范围内的记录返回，供后续合并

    Args:
        date_range: 日期范围

    Returns:
        当期月度抵销数 DataFrame（列：source_no, fin_con, fin_ind, unique_lvl,
                                        acct_period, prim_subj, amt, class）
    """
    try:
        from mypackage.utilities import delete_data_add_data

        conn, cur = connect_to_db()
        # 全量读取累计抵销数，排除汇总科目（与 Notebook 保持一致）
        cur.execute(
            """
            SELECT * FROM fact_offset
            WHERE subj_name NOT IN ('营业利润', '净利润', '利润总额')
        """
        )
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()

        if df.empty:
            print("fact_offset 无数据，跳过抵销数计算")
            return pd.DataFrame(
                columns=[
                    "source_no",
                    "fin_con",
                    "fin_ind",
                    "unique_lvl",
                    "acct_period",
                    "prim_subj",
                    "amt",
                    "class",
                ]
            )

        # 列重命名，统一字段名
        df["date"] = pd.to_datetime(df["date"])
        df = df[["source_no", "unique_lvl", "date", "subj_name", "offset_num"]].rename(
            columns={"offset_num": "amt", "date": "acct_period", "subj_name": "prim_subj"}
        )

        # 按科目 + 期间 + 层级分组求和（累计数）
        df = df.groupby(["prim_subj", "acct_period", "unique_lvl"])["amt"].sum().reset_index()
        df = df.sort_values(["prim_subj", "unique_lvl", "acct_period"]).reset_index(drop=True)

        # 累计数 → 月度数：同 prim_subj + unique_lvl 分组内做 diff()
        df["mo_amt"] = df.groupby(["prim_subj", "unique_lvl"])["amt"].diff()
        # 每年 1 月没有上期，直接取累计值
        df.loc[df["acct_period"].dt.month == 1, "mo_amt"] = df.loc[
            df["acct_period"].dt.month == 1, "amt"
        ]

        df = df.drop(columns=["amt"]).rename(columns={"mo_amt": "amt"})
        df["class"] = "抵销"
        df["fin_con"] = "抵销数"
        df["fin_ind"] = "抵销数"
        df["source_no"] = "O" + df.index.astype(str)

        # 全量覆盖 fact_offset_by_month（与 Notebook 一致）
        delete_data_add_data("fact_offset_by_month", df)
        print(f"fact_offset_by_month 刷新完成，共写入 {len(df)} 条记录")

        # 返回 date_range 范围内的月度数据
        df_filtered = df[df["acct_period"].isin(date_range)].copy()
        print(f"筛选当期抵销数完成，共 {len(df_filtered)} 条记录")
        return df_filtered

    except Exception as e:
        print(f"刷新抵销数时发生错误: {str(e)}")
        raise


@task(name="merge_profit_data", log_prints=True)
def merge_profit_data_task(
    df_revenue: pd.DataFrame, df_expense_other: pd.DataFrame, df_offset: pd.DataFrame
) -> pd.DataFrame:
    """
    合并收入、费用、抵销数数据

    Args:
        df_revenue: 收入数据
        df_expense_other: 费用和其他数据
        df_offset: 抵销数数据

    Returns:
        合并后的数据 DataFrame
    """
    try:
        df_t = pd.concat([df_revenue, df_expense_other, df_offset], axis=0, ignore_index=True)
        df_t["acct_period"] = pd.to_datetime(df_t["acct_period"])
        df_t = df_t[(df_t["amt"].notna()) & (df_t["amt"].notnull()) & (df_t["amt"] != 0)]

        print(f"合并利润数据完成，共 {len(df_t)} 条记录")
        return df_t
    except Exception as e:
        print(f"合并利润数据时发生错误: {str(e)}")
        raise


@task(name="calculate_profit_indicators", log_prints=True)
def calculate_profit_indicators_task(df_profit: pd.DataFrame) -> pd.DataFrame:
    """
    数据透视、计算利润指标（毛利润、营业利润、净利润）

    Args:
        df_profit: 合并后的利润数据

    Returns:
        包含利润指标的数据 DataFrame
    """
    try:
        # 数据聚合
        df_index = df_profit.columns.tolist()[1:-1]  # 排除第一列和最后一列
        df_profit_agg = df_profit[df_index].groupby(df_index[:-1]).sum().reset_index()

        # 数据透视
        df_profit_pivot = (
            df_profit.pivot_table(
                index=["fin_con", "fin_ind", "unique_lvl", "acct_period"],
                columns="prim_subj",
                values="amt",
                aggfunc="sum",
            )
            .fillna(0)
            .reset_index()
        )

        # 补全科目
        account_list = [
            "营业收入",
            "营业成本",
            "税金及附加",
            "销售费用",
            "管理费用",
            "研发费用",
            "财务费用",
            "信用减值损失",
            "资产减值损失",
            "资产处置收益",
            "公允价值变动收益",
            "其他收益",
            "投资收益",
            "营业外收入",
            "营业外支出",
            "所得税费用",
        ]
        for col in account_list:
            if col not in df_profit_pivot.columns:
                print(f"新增列：{col}")
                df_profit_pivot[col] = 0

        # 计算利润指标
        df_profit_pivot["毛利润"] = df_profit_pivot["营业收入"] - df_profit_pivot["营业成本"]
        df_profit_pivot["营业利润"] = (
            df_profit_pivot["营业收入"]
            - (
                df_profit_pivot["税金及附加"]
                + df_profit_pivot["营业成本"]
                + df_profit_pivot["销售费用"]
                + df_profit_pivot["管理费用"]
                + df_profit_pivot["研发费用"]
                + df_profit_pivot["财务费用"]
            )
            + (
                df_profit_pivot["信用减值损失"]
                + df_profit_pivot["资产减值损失"]
                + df_profit_pivot["资产处置收益"]
                + df_profit_pivot["公允价值变动收益"]
                + df_profit_pivot["其他收益"]
                + df_profit_pivot["投资收益"]
            )
        )
        df_profit_pivot["净利润"] = (
            df_profit_pivot["营业收入"]
            - (
                df_profit_pivot["税金及附加"]
                + df_profit_pivot["营业成本"]
                + df_profit_pivot["销售费用"]
                + df_profit_pivot["管理费用"]
                + df_profit_pivot["研发费用"]
                + df_profit_pivot["财务费用"]
            )
            + (
                df_profit_pivot["信用减值损失"]
                + df_profit_pivot["资产减值损失"]
                + df_profit_pivot["资产处置收益"]
                + df_profit_pivot["公允价值变动收益"]
                + df_profit_pivot["其他收益"]
                + df_profit_pivot["投资收益"]
            )
            + df_profit_pivot["营业外收入"]
            - df_profit_pivot["营业外支出"]
            - df_profit_pivot["所得税费用"]
        )

        # 数据逆透视
        df_profit_melt = pd.melt(
            df_profit_pivot,
            id_vars=["fin_con", "fin_ind", "unique_lvl", "acct_period"],
            value_vars=["毛利润", "营业利润", "净利润"],
            var_name="prim_subj",
            value_name="amt",
        )
        df_profit_melt = (
            df_profit_melt.groupby(["fin_con", "fin_ind", "unique_lvl", "acct_period", "prim_subj"])
            .sum()
            .reset_index()
        )
        df_profit_melt["source_no"] = "C" + df_profit_melt.index.astype(str)

        # 合并原始数据和计算出的利润指标
        df_upload = pd.concat([df_profit, df_profit_melt], axis=0, ignore_index=True)

        print(f"计算利润指标完成，共 {len(df_upload)} 条记录")
        return df_upload
    except Exception as e:
        print(f"计算利润指标时发生错误: {str(e)}")
        raise


@task(name="save_profit_table", log_prints=True)
def save_profit_table_task(df_profit: pd.DataFrame, date_range: pd.DatetimeIndex) -> None:
    """
    保存到 fact_profit 表

    Args:
        df_profit: 利润数据
        date_range: 日期范围（用于删除指定月份的数据）
    """
    try:
        from mypackage.utilities import delete_data_add_data_by_DateRange

        # 使用 delete_data_add_data_by_DateRange，只删除计算月份的数据
        delete_data_add_data_by_DateRange(
            table_name="fact_profit",
            date_column="acct_period",
            df=df_profit,
            df_date_column="acct_period",
            date_range=date_range,
        )
        print(f"保存利润表完成，共 {len(df_profit)} 条记录")
    except Exception as e:
        print(f"保存利润表时发生错误: {str(e)}")
        raise


@task(name="load_bus_profit_data", log_prints=True)
def load_bus_profit_data_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从 fact_bus_profit_bd 读取业务线利润数据

    Args:
        date_range: 日期范围

    Returns:
        业务线利润数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute(
            """
            SELECT source_no, fin_con, fin_ind, unique_lvl, date as acct_period, prim_subj, mo_amt as amt, bus_line
            FROM fact_bus_profit_bd
            WHERE prim_subj NOT IN ('营业利润','净利润','利润总额','政府补贴','分摊税费','分摊收益','退税收入')
            AND date >= %s AND date <= %s
        """,
            (date_range.min(), date_range.max()),
        )
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        # 筛选日期范围
        df["acct_period"] = pd.to_datetime(df["acct_period"])
        df = df[df["acct_period"].isin(date_range)]
        df = df[(df["amt"].notna()) & (df["amt"].notnull()) & (df["amt"] != 0)]

        print(f"加载业务线利润数据完成，共 {len(df)} 条记录")
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"加载业务线利润数据时发生错误: {str(e)}")
        raise


@task(name="calculate_bus_profit_indicators", log_prints=True)
def calculate_bus_profit_indicators_task(df_bus_profit: pd.DataFrame) -> pd.DataFrame:
    """
    计算业务线利润指标

    Args:
        df_bus_profit: 业务线利润数据

    Returns:
        包含利润指标的业务线利润数据 DataFrame
    """
    try:
        # 数据聚合
        df_index = df_bus_profit.columns.tolist()[1:-1]  # 排除第一列和最后一列
        df_profit_agg = df_bus_profit[df_index].groupby(df_index[:-1]).sum().reset_index()

        # 数据透视
        df_profit_pivot = (
            df_bus_profit.pivot_table(
                index=["fin_con", "fin_ind", "unique_lvl", "acct_period", "bus_line"],
                columns="prim_subj",
                values="amt",
                aggfunc="sum",
            )
            .fillna(0)
            .reset_index()
        )

        # 补全科目
        account_list = [
            "营业收入",
            "营业成本",
            "税金及附加",
            "销售费用",
            "管理费用",
            "研发费用",
            "财务费用",
            "信用减值损失",
            "资产减值损失",
            "资产处置收益",
            "公允价值变动收益",
            "其他收益",
            "投资收益",
            "营业外收入",
            "营业外支出",
            "所得税费用",
        ]
        for col in account_list:
            if col in df_profit_pivot.columns:
                df_profit_pivot[col] = df_profit_pivot[col].astype(float)
            if col not in df_profit_pivot.columns:
                print(f"新增列：{col}")
                df_profit_pivot[col] = 0.0

        # 计算利润指标
        df_profit_pivot["毛利润"] = df_profit_pivot["营业收入"] - df_profit_pivot["营业成本"]
        df_profit_pivot["营业利润"] = (
            df_profit_pivot["营业收入"]
            - (
                df_profit_pivot["税金及附加"]
                + df_profit_pivot["营业成本"]
                + df_profit_pivot["销售费用"]
                + df_profit_pivot["管理费用"]
                + df_profit_pivot["研发费用"]
                + df_profit_pivot["财务费用"]
            )
            + (
                df_profit_pivot["信用减值损失"]
                + df_profit_pivot["资产减值损失"]
                + df_profit_pivot["资产处置收益"]
                + df_profit_pivot["公允价值变动收益"]
                + df_profit_pivot["其他收益"]
                + df_profit_pivot["投资收益"]
            )
        )
        df_profit_pivot["净利润"] = (
            df_profit_pivot["营业收入"]
            - (
                df_profit_pivot["税金及附加"]
                + df_profit_pivot["营业成本"]
                + df_profit_pivot["销售费用"]
                + df_profit_pivot["管理费用"]
                + df_profit_pivot["研发费用"]
                + df_profit_pivot["财务费用"]
            )
            + (
                df_profit_pivot["信用减值损失"]
                + df_profit_pivot["资产减值损失"]
                + df_profit_pivot["资产处置收益"]
                + df_profit_pivot["公允价值变动收益"]
                + df_profit_pivot["其他收益"]
                + df_profit_pivot["投资收益"]
            )
            + df_profit_pivot["营业外收入"]
            - df_profit_pivot["营业外支出"]
            - df_profit_pivot["所得税费用"]
        )

        # 数据逆透视
        df_profit_melt = pd.melt(
            df_profit_pivot,
            id_vars=["fin_con", "fin_ind", "unique_lvl", "acct_period", "bus_line"],
            value_vars=["毛利润", "营业利润", "净利润"],
            var_name="prim_subj",
            value_name="amt",
        )
        df_profit_melt = (
            df_profit_melt.groupby(
                ["fin_con", "fin_ind", "unique_lvl", "acct_period", "prim_subj", "bus_line"]
            )
            .sum()
            .reset_index()
        )
        df_profit_melt["source_no"] = "C" + df_profit_melt.index.astype(str)

        # 合并原始数据和计算出的利润指标
        df_upload = pd.concat([df_bus_profit, df_profit_melt], axis=0, ignore_index=True)

        print(f"计算业务线利润指标完成，共 {len(df_upload)} 条记录")
        return df_upload
    except Exception as e:
        print(f"计算业务线利润指标时发生错误: {str(e)}")
        raise


@task(name="save_bus_profit_table", log_prints=True)
def save_bus_profit_table_task(df_bus_profit: pd.DataFrame, date_range: pd.DatetimeIndex) -> None:
    """
    保存到 fact_bus_profit 表

    Args:
        df_bus_profit: 业务线利润数据
        date_range: 日期范围（用于删除指定月份的数据）
    """
    try:
        from mypackage.utilities import delete_data_add_data_by_DateRange

        # 使用 delete_data_add_data_by_DateRange，只删除计算月份的数据
        delete_data_add_data_by_DateRange(
            table_name="fact_bus_profit",
            date_column="acct_period",
            df=df_bus_profit,
            df_date_column="acct_period",
            date_range=date_range,
        )
        print(f"保存业务线利润表完成，共 {len(df_bus_profit)} 条记录")
    except Exception as e:
        print(f"保存业务线利润表时发生错误: {str(e)}")
        raise
