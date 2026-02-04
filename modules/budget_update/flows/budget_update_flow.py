"""预算更新流程：从 FONE 拉取预算、严格映射检查、写库"""
from prefect import flow
from typing import Optional
import pandas as pd
import sys
import os
from datetime import datetime

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
)

from ..tasks.budget_update_tasks import (
    fetch_fone_budget_data_task,
    process_expense_budget_task,
    process_income_budget_task,
    process_personnel_budget_task,
    process_cash_budget_task,
    process_profit_budget_task,
    process_shared_rate_budget_task,
    write_budget_to_db_task,
)


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
            "version": f"{y + 1}-01-01",
            "report_date": f"{y + 1}-01-01",
        }
    if m in (1, 2):
        # 1月、2月：做当年年初预算
        return {
            "budget_type": "年初预算",
            "budget_year": str(y),
            "fone_version": "Version1",
            "version": f"{y}-01-01",
            "report_date": f"{y}-01-01",
        }
    if m in (4, 5, 6, 7):
        # 4月～7月：做当年年中预算
        return {
            "budget_type": "年中预算",
            "budget_year": str(y),
            "fone_version": "AdjustVersion1",
            "version": f"{y}-07-01",
            "report_date": f"{y}-07-01",
        }
    # 3月、8月、9月、10月：默认年初预算、当年度-01-01
    return {
        "budget_type": "年初预算",
        "budget_year": str(y),
        "fone_version": "Version1",
        "version": f"{y}-01-01",
        "report_date": f"{y}-01-01",
    }


def _empty(s: Optional[str]) -> bool:
    return s is None or (isinstance(s, str) and s.strip() == "")


# 模块加载时计算默认值，供 Run Deployment 表单预填；用户可修改后再运行
_FLOW_DEFAULTS = _get_budget_defaults_by_date()


@flow(name="budget_update_flow", log_prints=True)
def budget_update_flow(
    budget_year: Optional[str] = _FLOW_DEFAULTS["budget_year"],
    fone_version: Optional[str] = _FLOW_DEFAULTS["fone_version"],
    version: Optional[str] = _FLOW_DEFAULTS["version"],
    budget_type: Optional[str] = _FLOW_DEFAULTS["budget_type"],
    report_date: Optional[str] = _FLOW_DEFAULTS["report_date"],
    output_dir: Optional[str] = None,
) -> None:
    """
    预算更新流程：参数校验 → FONE 取数 → 费用/收入/人数/流水/利润/综合比例 清洗与严格映射检查 → 写库。

    任一映射检查点存在未映射则先导出 CSV 再中断执行。

    Run Deployment 弹窗会预填默认值（按当前月份规则），可手工修改后运行；留空的参数在运行时会再按当前日期补默认值。

    Args:
        budget_year: 预算年度，如 '2026'。不填则按当前月份规则推导
        fone_version: FONE 源系统里的版本编码。不填则按 budget_type 推导（年初 Version1，年中 AdjustVersion1）
        version: 填报日期（字符串），本批数据的版本标签。不填则按当前月份规则推导
        budget_type: 预算类型，'年初预算' 或 '年中预算'。不填则按当前月份规则推导
        report_date: 报告日期（字符串），写库时按此日期定位要替换的那一批。不填则按当前月份规则推导
        output_dir: 未映射数据 CSV 导出目录，默认当前工作目录

    易混点（version 与 report_date）：
        - report_date：决定「替换哪一批历史数」。库里 report_date 等于该日期的行会被删掉，再插入本批。
        - version：决定「本批新增数打什么标签」。插入的每一行会带 填报日期=version。
    """
    if output_dir is None:
        output_dir = os.getcwd()

    # 未填写的参数按当前日期补默认值（预算版本/填报日期等随月份动态调整）
    defaults = _get_budget_defaults_by_date()
    if _empty(budget_type):
        budget_type = defaults["budget_type"]
    if _empty(budget_year):
        budget_year = defaults["budget_year"]
    if _empty(fone_version):
        fone_version = defaults["fone_version"]
    if _empty(version):
        version = defaults["version"]
    if _empty(report_date):
        report_date = defaults["report_date"]

    # 参数校验
    if not budget_year or not fone_version or not version or not budget_type or not report_date:
        raise ValueError(
            "必填参数不能为空: budget_year, fone_version, version, budget_type, report_date"
        )
    if budget_type not in ("年初预算", "年中预算"):
        raise ValueError("budget_type 必须为「年初预算」或「年中预算」")

    report_date_ts = pd.to_datetime(report_date)
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
    print(f"  version: {version}")
    print(f"  budget_type: {budget_type}")
    print(f"  report_date: {report_date}")
    print(f"  output_dir: {output_dir}")
    print("=" * 60)

    try:
        # 1. 从 FONE 拉取预算数据
        data = fetch_fone_budget_data_task(
            budget_year=budget_year,
            fone_version=fone_version,
            output_dir=output_dir,
        )

        # 2. 各预算类型清洗 + 严格映射检查（任一步未通过即终止）
        df_exp = process_expense_budget_task(
            data["fone_exp"], version=version, output_dir=output_dir
        )
        df_inc = process_income_budget_task(
            data["fone_biz"], version=version, output_dir=output_dir
        )
        df_emp = process_personnel_budget_task(
            data["fone_emp"], version=version, output_dir=output_dir
        )
        df_cash = process_cash_budget_task(
            data["fone_amo"], version=version, output_dir=output_dir
        )
        df_pro = process_profit_budget_task(
            data["fone_pro"], version=version, output_dir=output_dir
        )
        df_shared_rate = process_shared_rate_budget_task(
            data["fone_shared_rate"], version=version
        )

        # 3. 写库（年初 / 年中分支在 task 内）
        write_budget_to_db_task(
            budget_type=budget_type,
            report_date=report_date_ts,
            version=version,
            date_range_psql=date_range_psql,
            date_range_fone=date_range_fone,
            df_exp=df_exp,
            df_inc=df_inc,
            df_emp=df_emp,
            df_cash=df_cash,
            df_pro=df_pro,
            df_shared_rate=df_shared_rate,
        )

        print("=" * 60)
        print("预算更新流程完成")
        print("=" * 60)
    except Exception as e:
        error_msg = f"预算更新流程失败: {str(e)}"
        print(f"\n{error_msg}")
        import traceback
        print(traceback.format_exc())
        raise Exception(error_msg) from e
