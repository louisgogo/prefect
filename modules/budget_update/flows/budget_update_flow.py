"""预算更新流程：从 FONE 拉取预算、严格映射检查、写库"""

import calendar
import json
import os
import sys
from datetime import datetime
from typing import Optional

import pandas as pd

from prefect import flow

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
)

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..tasks.budget_update_tasks import (
    fetch_fone_budget_data_task,
    process_cash_budget_task,
    process_expense_budget_task,
    process_income_budget_task,
    process_personnel_budget_task,
    process_profit_budget_task,
    process_shared_rate_budget_task,
    write_budget_to_db_task,
)


def _get_actual_through_month() -> Optional[int]:
    """
    年中预算默认实际数取到上个月。
    例如 6 月执行时 6 月实际数尚未出具，取到 5 月；7 月及以后取到 6 月。
    年初预算不拼接实际数，返回 None。
    """
    now = datetime.now()
    m = now.month
    if m in (4, 5, 6, 7):
        return m - 1
    return None


def _get_budget_defaults_by_date() -> dict:
    """
    按当前日期返回预算参数的默认值。
    上年11月～2月 → 年初预算（11/12 月用下年度-01-01，1/2 月用当年度-01-01）；
    本年4月～7月 → 年中预算（当年度-07-01）；
    其他月份 → 年初预算、当年度-01-01。
    """
    now = datetime.now()
    y, m = now.year, now.month
    if m in (11, 12):
        # 上年11月、12月：做下一年年初预算
        return {
            "budget_type": "年初预算",
            "budget_year": str(y + 1),
            "fone_version": "Version1",
            "save_previous_version": False,
        }
    if m in (1, 2):
        # 1月、2月：做当年年初预算
        return {
            "budget_type": "年初预算",
            "budget_year": str(y),
            "fone_version": "Version1",
            "save_previous_version": False,
        }
    if m in (4, 5, 6, 7):
        # 4月～7月：做当年年中预算
        return {
            "budget_type": "年中预算",
            "budget_year": str(y),
            "fone_version": "AdjustVersion1",
            "save_previous_version": False,
        }
    # 3月、8月、9月、10月：默认年初预算、当年度-01-01
    return {
        "budget_type": "年初预算",
        "budget_year": str(y),
        "fone_version": "Version1",
        "save_previous_version": False,
    }


def _empty(s: Optional[str]) -> bool:
    return s is None or (isinstance(s, str) and s.strip() == "")


def _get_official_budget_version(budget_year: str, budget_type: str) -> str:
    """根据预算年度和类型生成下游统一使用的每月 1 日正式版。"""
    try:
        year = int(budget_year)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"budget_year 必须是有效年份，当前值: {budget_year}") from exc

    if budget_type == "年初预算":
        month = 1
    elif budget_type == "年中预算":
        month = 7
    else:
        raise ValueError("budget_type 必须为「年初预算」或「年中预算」")
    return f"{year:04d}-{month:02d}-01"


# 模块加载时计算默认值，供 Run Deployment 表单预填；用户可修改后再运行
_FLOW_DEFAULTS = _get_budget_defaults_by_date()
_FLOW_DEFAULTS["actual_through_month"] = _get_actual_through_month()


@flow(name="budget_update_flow", log_prints=True)
def budget_update_flow(
    budget_year: Optional[str] = _FLOW_DEFAULTS["budget_year"],
    fone_version: Optional[str] = _FLOW_DEFAULTS["fone_version"],
    budget_type: Optional[str] = _FLOW_DEFAULTS["budget_type"],
    save_previous_version: bool = _FLOW_DEFAULTS["save_previous_version"],
    actual_through_month: Optional[int] = _FLOW_DEFAULTS["actual_through_month"],
    refresh_ai_data_etl: bool = True,
    output_dir: Optional[str] = None,
) -> None:
    """
    预算更新流程：参数校验 → FONE 取数 → 费用/收入/人数/流水/利润/综合比例 清洗与严格映射检查 → 写库。

    任一映射检查点存在未映射则先导出 CSV 再中断执行。

    Run Deployment 弹窗会预填默认值（按当前月份规则），可手工修改后运行；留空的参数在运行时会再按当前日期补默认值。

    Args:
        budget_year: 预算年度，如 '2026'。不填则按当前月份规则推导
        fone_version: FONE 源系统里的版本编码。不填则按 budget_type 推导（年初 Version1，年中 AdjustVersion1）
        budget_type: 预算类型，'年初预算' 或 '年中预算'。不填则按当前月份规则推导
        save_previous_version: 是否保存当前正式版，默认 False。True 时，先把当前每月 1 日正式版
            自动归档到同月第一个未使用日期（2 日、3 日……），再写入新正式版；False 时直接覆盖正式版
        actual_through_month: 年中预算时，实际数取到第几个月（1-11）。不填则按执行月份自动推导：
            4月→3、5月→4、6月→5、7月→6。年初预算忽略此参数。
        refresh_ai_data_etl: 预算基础表写库成功后，是否自动刷新 AI 数据 ETL/预算利润表。
            默认开启，确保 ai_bud_profit 使用最新 bud_income/bud_expense/bud_profit 重算。
        output_dir: 未映射数据 CSV/JSON 导出目录，默认 `/root/prefect/check/budget_unmapped`

    预算版本规则：
        - 正式版日期无需填写：年初预算固定为 budget_year-01-01，年中预算固定为 budget_year-07-01。
        - save_previous_version=False（默认）：不保留当前稿，快速执行会直接覆盖同月 1 日正式版。
        - save_previous_version=True：当前正式版自动归档到同月 2 日、3 日等首个空闲日期。
        - 新数据始终写入同月 1 日，因此下游 AI 预算利润和综合比例流程继续读取最新正式版。
    """
    if output_dir is None:
        output_dir = "/root/prefect/check/budget_unmapped"
    os.makedirs(output_dir, exist_ok=True)

    # 未填写的年度、类型和 FONE 源版本按当前日期补默认值
    defaults = _get_budget_defaults_by_date()
    if _empty(budget_type):
        budget_type = defaults["budget_type"]
    if _empty(budget_year):
        budget_year = defaults["budget_year"]
    if _empty(fone_version):
        fone_version = defaults["fone_version"]
    if actual_through_month is None:
        actual_through_month = _get_actual_through_month()

    # 参数校验
    if not budget_year or not fone_version or not budget_type:
        raise ValueError("必填参数不能为空: budget_year, fone_version, budget_type")
    if budget_type not in ("年初预算", "年中预算"):
        raise ValueError("budget_type 必须为「年初预算」或「年中预算」")

    budget_version = _get_official_budget_version(budget_year, budget_type)
    budget_version_ts = pd.to_datetime(budget_version)
    if budget_type == "年中预算":
        if actual_through_month is None or not (1 <= actual_through_month <= 11):
            raise ValueError("年中预算必须指定 actual_through_month（1-11）")
        psql_end_day = calendar.monthrange(int(budget_year), actual_through_month)[1]
        fone_start_month = actual_through_month + 1
        date_range_psql = pd.date_range(
            start=f"{budget_year}-01-01",
            end=f"{budget_year}-{actual_through_month:02d}-{psql_end_day}",
            freq="D",
        )
        date_range_fone = pd.date_range(
            start=f"{budget_year}-{fone_start_month:02d}-01",
            end=f"{budget_year}-12-31",
            freq="D",
        )
    else:
        # 年初预算不拼接实际数，保留默认值即可（写库分支不会使用）
        date_range_psql = pd.date_range(
            start=f"{budget_year}-01-01", end=f"{budget_year}-06-30", freq="D"
        )
        date_range_fone = pd.date_range(
            start=f"{budget_year}-07-01", end=f"{budget_year}-12-31", freq="D"
        )

    print("=" * 60)
    print("开始预算更新流程")
    print("=" * 60)
    print(f"  budget_year: {budget_year}")
    print(f"  fone_version: {fone_version}")
    print(f"  budget_type: {budget_type}")
    print(f"  budget_version（自动正式版）: {budget_version}")
    print(f"  save_previous_version: {save_previous_version}")
    print(f"  actual_through_month: {actual_through_month}")
    print(f"  refresh_ai_data_etl: {refresh_ai_data_etl}")
    print(f"  output_dir: {output_dir}")
    if save_previous_version:
        print("  版本保存方式: 当前正式版自动归档到同月首个空闲日期，新数据写回 1 日")
    else:
        print("  版本保存方式: 不保留当前稿，直接覆盖 1 日正式版")
    print("=" * 60)

    notify_hermes_task(
        event="started",
        flow_name="预算更新",
        payload={
            "budget_year": budget_year,
            "fone_version": fone_version,
            "budget_type": budget_type,
            "budget_version": budget_version,
            "save_previous_version": save_previous_version,
            "actual_through_month": actual_through_month,
            "refresh_ai_data_etl": refresh_ai_data_etl,
        },
    )

    archived_version: Optional[str] = None
    try:
        # 1. 从 FONE 拉取预算数据
        data = fetch_fone_budget_data_task(
            budget_year=budget_year,
            fone_version=fone_version,
            output_dir=output_dir,
        )

        # 2. 各预算类型清洗 + 严格映射检查（任一步未通过即终止）
        df_exp = process_expense_budget_task(
            data["fone_exp"], version=budget_version, output_dir=output_dir
        )
        df_inc = process_income_budget_task(
            data["fone_biz"], version=budget_version, output_dir=output_dir
        )
        df_emp = process_personnel_budget_task(
            data["fone_emp"], version=budget_version, output_dir=output_dir
        )
        df_cash = process_cash_budget_task(
            data["fone_amo"], version=budget_version, output_dir=output_dir
        )
        df_pro = process_profit_budget_task(
            data["fone_pro"], version=budget_version, output_dir=output_dir
        )
        df_shared_rate = process_shared_rate_budget_task(
            data["fone_shared_rate"], version=budget_version
        )

        # 3. 写库（年初 / 年中分支在 task 内）
        archived_version = write_budget_to_db_task(
            budget_type=budget_type,
            budget_version=budget_version_ts,
            save_previous_version=save_previous_version,
            date_range_psql=date_range_psql,
            date_range_fone=date_range_fone,
            df_exp=df_exp,
            df_inc=df_inc,
            df_emp=df_emp,
            df_cash=df_cash,
            df_pro=df_pro,
            df_shared_rate=df_shared_rate,
        )

        # 4. 预算基础表更新后，重算 AI 预算利润表，避免 ai_bud_profit 留在旧版本计算结果。
        if refresh_ai_data_etl:
            print("=" * 60)
            print("开始刷新 AI 数据 ETL/预算利润表")
            print("=" * 60)
            from ...ai_data_etl.flows.ai_data_etl_flow import ai_data_etl_flow

            ai_data_etl_flow(
                data_type="业务线数据",
                calc_budget_profit=True,
                year=int(budget_year),
                month=None,
                budget_version=budget_version,
            )
            print("=" * 60)
            print("AI 数据 ETL/预算利润表刷新完成")
            print("=" * 60)

        print("=" * 60)
        print("预算更新流程完成")
        print("=" * 60)

        notify_hermes_task(
            event="completed",
            flow_name="预算更新",
            payload={
                "budget_year": budget_year,
                "fone_version": fone_version,
                "budget_type": budget_type,
                "budget_version": budget_version,
                "save_previous_version": save_previous_version,
                "archived_version": archived_version,
                "actual_through_month": actual_through_month,
                "refresh_ai_data_etl": refresh_ai_data_etl,
                "output_dir": output_dir,
                "summary": (
                    f"{budget_year} {budget_type} 预算更新成功，正式版={budget_version}，"
                    f"上一稿={'归档为 ' + archived_version if archived_version else '未归档'}，"
                    f"实际数取到 {actual_through_month} 月，"
                    f"AI数据ETL={'已刷新' if refresh_ai_data_etl else '未刷新'}"
                ),
            },
        )
    except Exception as e:
        error_msg = f"预算更新流程失败: {str(e)}"
        print(f"\n{error_msg}")
        import traceback

        print(traceback.format_exc())

        # 如有未映射汇总文件，将其路径和摘要一并通知 Hermes
        unmapped_summary_path = os.path.join(output_dir, "unmapped_summary.json")
        unmapped_payload = {
            "error": str(e),
            "error_type": type(e).__name__,
            "budget_year": budget_year,
            "budget_type": budget_type,
            "budget_version": budget_version,
            "save_previous_version": save_previous_version,
            "archived_version": archived_version,
        }
        if os.path.exists(unmapped_summary_path):
            unmapped_payload["unmapped_summary_path"] = os.path.abspath(unmapped_summary_path)
            unmapped_payload["output_dir"] = os.path.abspath(output_dir)
            try:
                with open(unmapped_summary_path, "r", encoding="utf-8") as f:
                    unmapped_payload["unmapped_summary"] = json.load(f)
            except Exception:
                pass

        notify_hermes_task(
            event="failed",
            flow_name="预算更新",
            payload=unmapped_payload,
        )
        raise Exception(error_msg) from e
