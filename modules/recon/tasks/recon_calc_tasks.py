"""往来对账 - 对账计算 Tasks

阶段2：从 PostgreSQL excel_account_recon 读取数据，
加载映射配置表，执行往来/销售/现金流三类核对，保存结果。
移植自 D:\mac\ExcelToPython\generated_script.py。
"""
import os
import platform
import sys
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from prefect import task

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _get_mapping_path() -> str:
    """根据操作系统返回映射配置表路径"""
    if platform.system() == "Windows":
        return r"Z:\10-内部往来对账\4-配置参数\映射配置表.xlsx"
    else:
        return r"/mnt/xgd_share/10-内部往来对账/4-配置参数/映射配置表.xlsx"


# ──────────────────────────────────────────────
# Task 1：加载映射配置表
# ──────────────────────────────────────────────


@task(name="load_mapping_config", log_prints=True)
def load_mapping_config_task() -> Tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """
    从共享盘加载映射配置表.xlsx 中的各配置子表。

    Returns:
        (df_params, df_unit_map, df_yebao_unit_map,
         df_diff_wanglai, df_diff_xiaoshou, df_diff_xianjinliu)
    """
    mapping_excel = _get_mapping_path()
    print(f"--> 加载映射配置表: {mapping_excel}")

    try:
        df_params = pd.read_excel(mapping_excel, sheet_name="参数表", usecols=["项目", "统一名称"]).dropna(
            how="all"
        )
        df_params["统一名称"] = df_params["统一名称"].fillna("")

        df_units = pd.read_excel(
            mapping_excel, sheet_name="单位简称", usecols=["单位编号", "单位全称", "单位简称", "合并名称", "业报合并名称"]
        ).dropna(how="all")
        df_unit_map = df_units[["合并名称", "单位简称"]].rename(columns={"单位简称": "本单位"}).drop_duplicates()
        df_yebao_unit_map = (
            df_units[["业报合并名称", "单位简称"]].rename(columns={"单位简称": "本单位"}).drop_duplicates()
        )

        # 往来差异说明
        try:
            df_diff_wanglai = pd.read_excel(mapping_excel, sheet_name="往来差异说明")
            if not df_diff_wanglai.empty:
                df_diff_wanglai["统一日期"] = pd.to_datetime(df_diff_wanglai["统一日期"], errors="coerce")
                for col in ["金额", "往来核对-应付.金额", "差异"]:
                    if col in df_diff_wanglai.columns:
                        df_diff_wanglai[col] = pd.to_numeric(
                            df_diff_wanglai[col], errors="coerce"
                        ).round(2)
        except Exception as e:
            print(f"[WARN] 往来差异说明加载失败: {e}")
            df_diff_wanglai = pd.DataFrame(
                columns=["唯一名称", "金额", "往来核对-应付.唯一名称", "往来核对-应付.金额", "差异", "统一日期", "差异原因"]
            )

        # 销售差异说明
        try:
            df_diff_xiaoshou = pd.read_excel(mapping_excel, sheet_name="销售差异说明")
            if not df_diff_xiaoshou.empty:
                df_diff_xiaoshou["唯一日期"] = pd.to_datetime(df_diff_xiaoshou["唯一日期"], errors="coerce")
                for col in ["金额", "采购核对.金额", "计算差异"]:
                    if col in df_diff_xiaoshou.columns:
                        df_diff_xiaoshou[col] = pd.to_numeric(
                            df_diff_xiaoshou[col], errors="coerce"
                        ).round(2)
        except Exception as e:
            print(f"[WARN] 销售差异说明加载失败: {e}")
            df_diff_xiaoshou = pd.DataFrame(
                columns=[
                    "公司简称",
                    "对方简称",
                    "金额",
                    "采购核对.公司简称",
                    "采购核对.对方简称",
                    "采购核对.金额",
                    "计算差异",
                    "唯一日期",
                    "差异原因",
                ]
            )

        # 现金流差异说明
        try:
            df_diff_xianjinliu = pd.read_excel(mapping_excel, sheet_name="现金流差异说明")
            if not df_diff_xianjinliu.empty:
                df_diff_xianjinliu["唯一日期"] = pd.to_datetime(
                    df_diff_xianjinliu["唯一日期"], errors="coerce"
                )
                for col in ["金额", "现金流量-支付.金额", "差额"]:
                    if col in df_diff_xianjinliu.columns:
                        df_diff_xianjinliu[col] = pd.to_numeric(
                            df_diff_xianjinliu[col], errors="coerce"
                        ).round(2)
        except Exception as e:
            print(f"[WARN] 现金流差异说明加载失败: {e}")
            df_diff_xianjinliu = pd.DataFrame(
                columns=["唯一名称", "金额", "现金流量-支付.唯一名称", "现金流量-支付.金额", "差额", "唯一日期", "差异原因"]
            )

        print(
            f"--> 映射配置表加载完成：参数表 {len(df_params)} 行，差异说明(往来{len(df_diff_wanglai)}/销售{len(df_diff_xiaoshou)}/现金流{len(df_diff_xianjinliu)})"
        )
        return (
            df_params,
            df_unit_map,
            df_yebao_unit_map,
            df_diff_wanglai,
            df_diff_xiaoshou,
            df_diff_xianjinliu,
        )

    except Exception as e:
        print(f"[ERROR] 加载映射配置表失败: {e}，使用空表占位")
        empty_params = pd.DataFrame(columns=["项目", "统一名称"])
        empty_unit = pd.DataFrame(columns=["合并名称", "本单位"])
        empty_yebao = pd.DataFrame(columns=["业报合并名称", "本单位"])
        empty_wanglai = pd.DataFrame(
            columns=["唯一名称", "金额", "往来核对-应付.唯一名称", "往来核对-应付.金额", "差异", "统一日期", "差异原因"]
        )
        empty_xiaoshou = pd.DataFrame(
            columns=[
                "公司简称",
                "对方简称",
                "金额",
                "采购核对.公司简称",
                "采购核对.对方简称",
                "采购核对.金额",
                "计算差异",
                "唯一日期",
                "差异原因",
            ]
        )
        empty_xianjinliu = pd.DataFrame(
            columns=["唯一名称", "金额", "现金流量-支付.唯一名称", "现金流量-支付.金额", "差额", "唯一日期", "差异原因"]
        )
        return (
            empty_params,
            empty_unit,
            empty_yebao,
            empty_wanglai,
            empty_xiaoshou,
            empty_xianjinliu,
        )


# ──────────────────────────────────────────────
# Task 2：从 PostgreSQL 读取原始数据
# ──────────────────────────────────────────────


@task(name="load_recon_raw_data", log_prints=True)
def load_recon_raw_task(target_date: Optional[str] = None) -> pd.DataFrame:
    """
    从 PostgreSQL excel_account_recon 表读取目标月份数据，并重命名为中文列名。

    Args:
        target_date: 格式 YYYY-MM-DD；None 时取上个自然月。

    Returns:
        中文列名的原始数据 DataFrame。
    """
    from datetime import date, timedelta

    from mypackage.utilities import engine_to_db

    if target_date:
        try:
            t = pd.to_datetime(target_date).date()
        except Exception:
            today = date.today()
            t = today.replace(day=1) - timedelta(days=1)
    else:
        today = date.today()
        t = today.replace(day=1) - timedelta(days=1)

    lastmonth = date(t.year, t.month, 1)
    # 计算本月1日，用于过滤
    if t.month == 12:
        current_month_start = date(t.year + 1, 1, 1)
    else:
        current_month_start = date(t.year, t.month + 1, 1)

    rename_mapping = {
        "co_abbr": "公司简称",
        "prim_subj": "科目名称",
        "class": "类别",
        "cp_abbr": "对方简称",
        "content": "具体内容",
        "amt": "金额",
        "date": "日期",
        "remarks": "备注",
        "resp_person": "责任人",
        "major_cat": "大类",
        "note_cat": "附注分类",
    }

    try:
        engine = engine_to_db()
        df_db = pd.read_sql("SELECT * FROM public.excel_account_recon", con=engine)
        print(f"--> 从 PostgreSQL 读取 excel_account_recon 共 {len(df_db)} 条")
    except Exception as e:
        print(f"[ERROR] 读取 excel_account_recon 失败: {e}")
        raise

    df_raw = df_db.rename(columns=rename_mapping)

    ordered_cols = [
        "id",
        "公司简称",
        "科目名称",
        "类别",
        "附注分类",
        "对方简称",
        "具体内容",
        "金额",
        "日期",
        "备注",
        "责任人",
        "大类",
    ]
    for col in ordered_cols:
        if col not in df_raw.columns:
            df_raw[col] = None
    df_raw = df_raw[ordered_cols].copy()

    df_raw["金额"] = pd.to_numeric(df_raw["金额"], errors="coerce")
    df_raw["日期"] = pd.to_datetime(df_raw["日期"], errors="coerce").dt.normalize()

    # 剔除 "大类" 为空的无效脏数据（如 Excel 末尾备注/空行被多读进来的情况）
    df_raw = df_raw.dropna(subset=["大类"]).copy()
    df_raw = df_raw[df_raw["大类"].astype(str).str.strip().astype(bool)].copy()

    # 过滤目标月份
    prev_month_start = pd.Timestamp(lastmonth)
    cur_month_start = pd.Timestamp(current_month_start)
    df_raw = df_raw[(df_raw["日期"] >= prev_month_start) & (df_raw["日期"] < cur_month_start)].copy()

    print(f"--> 过滤至 {lastmonth} 月，共 {len(df_raw)} 条记录")
    return df_raw


# ──────────────────────────────────────────────
# Task 3：往来对账核对
# ──────────────────────────────────────────────


@task(name="reconcile_wanglai", log_prints=True)
def reconcile_wanglai_task(
    df_raw: pd.DataFrame,
    df_params: pd.DataFrame,
    df_diff_wanglai: pd.DataFrame,
) -> pd.DataFrame:
    """往来余额三向核对（应收 vs 应付），输出差异明细并匹配差异原因。"""
    df_wanglai = df_raw[df_raw["大类"] == "往来余额"].copy()

    # 应付
    df_ap = df_wanglai[df_wanglai["科目名称"].str.contains("付", na=False)].copy()
    df_ap = pd.merge(df_ap, df_params, left_on="科目名称", right_on="项目", how="left")
    df_ap["唯一名称"] = df_ap["对方简称"] + "-" + df_ap["公司简称"] + "-" + df_ap["统一名称"].fillna("")
    df_ap_grouped = df_ap.groupby(["唯一名称", "日期"], as_index=False, dropna=False)["金额"].sum()

    # 应收
    df_ar = df_wanglai[df_wanglai["科目名称"].str.contains("收", na=False)].copy()
    df_ar = pd.merge(df_ar, df_params, left_on="科目名称", right_on="项目", how="left")
    df_ar["唯一名称"] = df_ar["公司简称"] + "-" + df_ar["对方简称"] + "-" + df_ar["统一名称"].fillna("")
    df_ar_grouped = df_ar.groupby(["唯一名称", "日期"], dropna=False)["金额"].sum().reset_index()

    # 合并
    df_ar_grouped = df_ar_grouped.rename(columns={"唯一名称": "唯一名称_AR", "日期": "日期_AR"})
    df_ap_grouped = df_ap_grouped.rename(columns={"唯一名称": "唯一名称_AP", "日期": "日期_AP"})

    df_merged = pd.merge(
        df_ar_grouped,
        df_ap_grouped,
        left_on=["唯一名称_AR", "日期_AR"],
        right_on=["唯一名称_AP", "日期_AP"],
        how="outer",
        suffixes=("", "_应付"),
    )

    df_merged["金额"] = df_merged["金额"].fillna(0).round(2)
    df_merged["往来核对-应付.金额"] = df_merged["金额_应付"].fillna(0).round(2)
    df_merged["差异"] = (df_merged["金额"] - df_merged["往来核对-应付.金额"]).round(2)
    df_merged["统一日期"] = df_merged["日期_AR"].combine_first(df_merged["日期_AP"])

    df_result = df_merged[(df_merged["差异"] >= 0.01) | (df_merged["差异"] <= -0.01)].copy()
    df_result["唯一名称"] = df_result["唯一名称_AR"]
    df_result["往来核对-应付.唯一名称"] = df_result["唯一名称_AP"]
    out_cols = ["唯一名称", "金额", "往来核对-应付.唯一名称", "往来核对-应付.金额", "差异", "统一日期"]
    df_result = df_result[out_cols].copy()

    # 匹配差异原因
    if not df_diff_wanglai.empty:
        df_result["差异"] = df_result["差异"].round(2)
        join_cols = [c for c in out_cols if c in df_diff_wanglai.columns]
        for col in ["唯一名称", "往来核对-应付.唯一名称"]:
            if col in join_cols:
                df_result[col] = df_result[col].fillna("").astype(str).str.strip()
                df_diff_wanglai[col] = df_diff_wanglai[col].fillna("").astype(str).str.strip()
        df_final = pd.merge(df_result, df_diff_wanglai, on=join_cols, how="left")
    else:
        df_final = df_result
        df_final["差异原因"] = None

    if not df_final.empty:
        # 清除完全空的行或者没有唯一名称及差异的空行
        df_final = df_final[
            df_final["唯一名称"].astype(str).str.strip().astype(bool)
            | df_final["往来核对-应付.唯一名称"].astype(str).str.strip().astype(bool)
        ]
        # 使用 sort_values 按差异金额绝对值降序排序，同时重置索引
        df_final = df_final.sort_values(by="差异", key=abs, ascending=False).reset_index(drop=True)

    print(f"--> 往来核对完成，差异 {len(df_final)} 条")
    return df_final


# ──────────────────────────────────────────────
# Task 4：销售/采购对账核对
# ──────────────────────────────────────────────


@task(name="reconcile_sales_purchases", log_prints=True)
def process_sales_purchases_task(
    df_raw: pd.DataFrame,
    df_diff_xiaoshou: pd.DataFrame,
) -> pd.DataFrame:
    """销售 vs 采购发生额核对，输出差异明细并匹配差异原因。"""
    df_sales = df_raw[df_raw["大类"] == "销售发生额"].copy()
    df_sales_grouped = (
        df_sales.groupby(["公司简称", "对方简称", "日期"], dropna=False)["金额"].sum().reset_index()
    )

    df_purchases = df_raw[df_raw["大类"] == "采购发生额"].copy()
    df_purchases_grouped = (
        df_purchases.groupby(["公司简称", "对方简称", "日期"], dropna=False)["金额"].sum().reset_index()
    )

    df_merged = pd.merge(
        df_sales_grouped,
        df_purchases_grouped,
        left_on=["对方简称", "公司简称", "日期"],
        right_on=["公司简称", "对方简称", "日期"],
        how="outer",
        suffixes=("", "_采购"),
    )

    df_merged["金额"] = df_merged["金额"].fillna(0).round(2)
    df_merged["采购核对.金额"] = df_merged["金额_采购"].fillna(0).round(2)
    df_merged["计算差异"] = (df_merged["金额"] - df_merged["采购核对.金额"]).round(2)

    if "日期_x" in df_merged.columns and "日期_y" in df_merged.columns:
        df_merged["唯一日期"] = df_merged["日期_x"].combine_first(df_merged["日期_y"])
    else:
        df_merged["唯一日期"] = df_merged["日期"]

    df_result = df_merged[(df_merged["计算差异"] >= 0.05) | (df_merged["计算差异"] <= -0.05)].copy()

    df_result["采购核对.公司简称"] = df_result["公司简称_采购"]
    df_result["采购核对.对方简称"] = df_result["对方简称_采购"]
    out_cols = ["公司简称", "对方简称", "金额", "采购核对.公司简称", "采购核对.对方简称", "采购核对.金额", "计算差异", "唯一日期"]
    for c in out_cols:
        if c not in df_result.columns:
            df_result[c] = None
    df_result = df_result[out_cols].copy()

    if not df_diff_xiaoshou.empty:
        df_result["计算差异"] = df_result["计算差异"].round(2)
        join_cols = [c for c in out_cols if c in df_diff_xiaoshou.columns]
        for col in ["公司简称", "对方简称", "采购核对.公司简称", "采购核对.对方简称"]:
            if col in join_cols:
                df_result[col] = df_result[col].fillna("").astype(str).str.strip()
                df_diff_xiaoshou[col] = df_diff_xiaoshou[col].fillna("").astype(str).str.strip()
        df_final = pd.merge(df_result, df_diff_xiaoshou, on=join_cols, how="left")
    else:
        df_final = df_result
        df_final["差异原因"] = None

    if not df_final.empty:
        # 清除公司简称和对方简称全空的无效合并行
        # 注意：要同时检查销售侧和采购侧的简称，避免单边记录被误过滤
        has_sales_co = df_final["公司简称"].astype(str).str.strip().astype(bool)
        has_sales_cp = df_final["对方简称"].astype(str).str.strip().astype(bool)
        has_purch_co = df_final["采购核对.公司简称"].astype(str).str.strip().astype(bool)
        has_purch_cp = df_final["采购核对.对方简称"].astype(str).str.strip().astype(bool)
        df_final = df_final[has_sales_co | has_sales_cp | has_purch_co | has_purch_cp]
        # 使用 sort_values 按绝对差异降序排序
        df_final = df_final.sort_values(by="计算差异", key=abs, ascending=False).reset_index(drop=True)

    print(f"--> 销售/采购核对完成，差异 {len(df_final)} 条")
    return df_final


# ──────────────────────────────────────────────
# Task 5：现金流对账核对
# ──────────────────────────────────────────────


@task(name="reconcile_cashflow", log_prints=True)
def process_cashflow_task(
    df_raw: pd.DataFrame,
    df_params: pd.DataFrame,
    df_diff_xianjinliu: pd.DataFrame,
) -> pd.DataFrame:
    """现金流量收入 vs 支付核对，输出差异明细并匹配差异原因。"""
    df_cash = df_raw[df_raw["大类"] == "现金流量"].copy()

    pay_subjects = [
        "分配股利、利润或偿付利息支付的现金",
        "投资支付的现金",
        "支付其他与投资活动有关的现金",
        "支付其他与筹资活动有关的现金",
        "支付其他与经营活动有关的现金",
        "支付的与投资有关的现金",
        "购买商品、接受劳务支付的现金",
        "购建固定资产、无形资产和其他长期资产支付的现金",
    ]
    df_pay = df_cash[df_cash["科目名称"].isin(pay_subjects)].copy()
    df_pay = pd.merge(df_pay, df_params, left_on="科目名称", right_on="项目", how="left")
    df_pay["唯一名称"] = df_pay["对方简称"] + "-" + df_pay["公司简称"] + "-" + df_pay["统一名称"].fillna("")
    df_pay_grouped = df_pay.groupby(["唯一名称", "日期"], dropna=False)["金额"].sum().reset_index()

    income_subjects = [
        "取得投资收益收到的现金",
        "吸收投资收到的现金",
        "处置固定资产、无形资产和其他长期资产收回的现金净额",
        "收到其他与筹资活动有关的现金",
        "收到其他与经营活动有关的现金",
        "收回投资收到的现金",
        "销售商品、提供劳务收到的现金",
        "处置子公司及其他营业单位收到的现金净额",
        "收到其他与投资活动有关的现金",
    ]
    df_income = df_cash[df_cash["科目名称"].isin(income_subjects)].copy()
    df_income = pd.merge(df_income, df_params, left_on="科目名称", right_on="项目", how="left")
    df_income = df_income[df_income["统一名称"].notna() & (df_income["统一名称"] != "")].copy()
    df_income["唯一名称"] = (
        df_income["公司简称"] + "-" + df_income["对方简称"] + "-" + df_income["统一名称"].fillna("")
    )
    df_income_grouped = df_income.groupby(["唯一名称", "日期"], dropna=False)["金额"].sum().reset_index()

    df_income_grouped = df_income_grouped.rename(columns={"唯一名称": "唯一名称_I", "日期": "日期_I"})
    df_pay_grouped = df_pay_grouped.rename(columns={"唯一名称": "唯一名称_P", "日期": "日期_P"})

    df_merged = pd.merge(
        df_income_grouped,
        df_pay_grouped,
        left_on=["唯一名称_I", "日期_I"],
        right_on=["唯一名称_P", "日期_P"],
        how="outer",
        suffixes=("", "_支付"),
    )
    df_merged["金额"] = df_merged["金额"].fillna(0).round(2)
    df_merged["现金流量-支付.金额"] = df_merged["金额_支付"].fillna(0).round(2)
    df_merged["差额"] = (df_merged["金额"] - df_merged["现金流量-支付.金额"]).round(2)
    df_merged["唯一日期"] = df_merged["日期_I"].combine_first(df_merged["日期_P"])

    df_result = df_merged[(df_merged["差额"] >= 0.01) | (df_merged["差额"] <= -0.01)].copy()
    df_result["唯一名称"] = df_result["唯一名称_I"]
    df_result["现金流量-支付.唯一名称"] = df_result["唯一名称_P"]
    out_cols = ["唯一名称", "金额", "现金流量-支付.唯一名称", "现金流量-支付.金额", "差额", "唯一日期"]
    df_result = df_result[out_cols].copy()

    if not df_diff_xianjinliu.empty:
        df_result["差额"] = df_result["差额"].round(2)
        join_cols = [c for c in out_cols if c in df_diff_xianjinliu.columns]
        for col in ["唯一名称", "现金流量-支付.唯一名称"]:
            if col in join_cols:
                df_result[col] = df_result[col].fillna("").astype(str).str.strip()
                df_diff_xianjinliu[col] = df_diff_xianjinliu[col].fillna("").astype(str).str.strip()
        df_final = pd.merge(df_result, df_diff_xianjinliu, on=join_cols, how="left")
    else:
        df_final = df_result
        df_final["差异原因"] = None

    if not df_final.empty:
        # 清除名字完全为空的行
        df_final = df_final[
            df_final["唯一名称"].astype(str).str.strip().astype(bool)
            | df_final["现金流量-支付.唯一名称"].astype(str).str.strip().astype(bool)
        ]
        # 按绝对值降序排序
        df_final = df_final.sort_values(by="差额", key=abs, ascending=False).reset_index(drop=True)

    print(f"--> 现金流核对完成，差异 {len(df_final)} 条")
    return df_final


# ──────────────────────────────────────────────
# Task 6：保存对账结果
# ──────────────────────────────────────────────


def _format_dates(df: pd.DataFrame) -> pd.DataFrame:
    """统一格式化所有日期列为 YYYY-MM-DD 字符串"""
    for col in df.columns:
        if "日期" in col or pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


@task(name="save_recon_results", log_prints=True)
def save_recon_results_task(
    res_wanglai: pd.DataFrame,
    res_transaction: pd.DataFrame,
    res_cashflow: pd.DataFrame,
    target_date: Optional[str] = None,
) -> str:
    """
    将三类对账结果写入 PostgreSQL（三张结果表）并同时导出备份 Excel。

    目标表：
      - recon_result_wanglai   往来差异
      - recon_result_sales     销售/采购差异
      - recon_result_cashflow  现金流差异

    Args:
        res_wanglai: 往来核对结果
        res_transaction: 销售/采购核对结果
        res_cashflow: 现金流核对结果
        target_date: 目标月份（用于生成文件名）

    Returns:
        Excel 导出路径字符串
    """
    import datetime

    from mypackage.utilities import engine_to_db
    from sqlalchemy import text

    # 日期格式化
    res_wanglai = _format_dates(res_wanglai.copy())
    res_transaction = _format_dates(res_transaction.copy())
    res_cashflow = _format_dates(res_cashflow.copy())

    # 确定月份标签（用于 Excel 文件名）
    if target_date:
        try:
            t = pd.to_datetime(target_date)
            month_label = t.strftime("%Y%m")
        except Exception:
            month_label = datetime.datetime.now().strftime("%Y%m")
    else:
        from datetime import date, timedelta

        today = date.today()
        last = today.replace(day=1) - timedelta(days=1)
        month_label = last.strftime("%Y%m")

    # 写入 PostgreSQL（读取旧表 → 剔除目标月 → 合并新结果 → 整体回写）
    engine = engine_to_db()
    year_month_prefix = f"{month_label[:4]}-{month_label[4:]}"  # e.g. "2026-02"

    result_tables = [
        ("recon_result_wanglai", res_wanglai),
        ("recon_result_sales", res_transaction),
        ("recon_result_cashflow", res_cashflow),
    ]
    for table_name, df_new in result_tables:
        try:
            # 找日期列（用于过滤旧数据）
            date_cols = [c for c in df_new.columns if "日期" in c]

            # 尝试读取已有数据（表不存在则直接跳过）
            try:
                df_existing = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
                # 剔除本月旧数据
                if date_cols and not df_existing.empty:
                    dc = date_cols[0]
                    df_existing[dc] = df_existing[dc].astype(str)
                    df_existing = df_existing[~df_existing[dc].str.startswith(year_month_prefix)]
                # 合并旧数据（其他月份）+ 本月新数据
                df_write = pd.concat([df_existing, df_new], ignore_index=True)
            except Exception:
                # 表不存在，直接写新数据
                df_write = df_new.copy()

            df_write.to_sql(table_name, con=engine, if_exists="replace", index=False)
            print(f"--> 写入 {table_name} 完成（本月新数据 {len(df_new)} 条，合计 {len(df_write)} 条）")
        except Exception as e:
            print(f"[WARN] 写入 {table_name} 失败: {e}，继续输出 Excel")

    # 导出 Excel 备份
    if platform.system() == "Windows":
        output_dir = r"Z:\10-内部往来对账\5-对账结果"
    else:
        output_dir = r"/mnt/xgd_share/10-内部往来对账/5-对账结果"

    output_file = os.path.join(output_dir, f"核对结果_{month_label}.xlsx")
    try:
        os.makedirs(output_dir, exist_ok=True)
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            res_wanglai.to_excel(writer, sheet_name="往来差异核对", index=False)
            res_transaction.to_excel(writer, sheet_name="交易差异核对", index=False)
            res_cashflow.to_excel(writer, sheet_name="现金流核对", index=False)
        print(f"--> Excel 已导出至: {output_file}")
    except Exception as e:
        print(f"[WARN] Excel 导出失败: {e}")
        output_file = "(Excel 导出失败)"

    return output_file
