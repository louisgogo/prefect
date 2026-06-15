"""从 FONE 获取往来数据子流程

通过 FONE API 触发 ERP 科目余额表获取及 BI 内部关联方数据推送脚本。
默认处理"上个自然月"数据，也可通过 year/month 参数指定。
"""

import calendar
import os
import sys
from datetime import datetime
from typing import Optional

from prefect import flow

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..tasks.fone_recon_tasks import execute_fone_recon_script_task, get_fone_token_task


def _get_last_month():
    """获取上个月的年份和月份。"""
    now = datetime.now()
    if now.month == 1:
        return now.year - 1, 12
    return now.year, now.month - 1


def _month_end(year: int, month: int) -> str:
    """返回指定年月最后一天，格式 YYYY-MM-DD。"""
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-{last_day:02d}"


@flow(name="fone_recon_flow", log_prints=True)
def fone_recon_flow(
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> None:
    """
    从 FONE 获取往来数据流程：调用 FONE API 执行 0501 脚本。

    Args:
        year: 目标年份，不填则使用上个自然年（若上个月为12月）或当前年。
        month: 目标月份（1-12），不填则使用上个自然月。

    流程说明：
      1. 计算目标月份的起止日期（1号 ~ 月末）
      2. 调用 FONE 登录接口获取 ticket（使用 /api/login/test）
      3. 调用执行脚本接口，触发 0501-获取ERP科目余额表-WebApi
      4. 校验脚本执行结果，失败则抛出异常
    """
    print("=" * 60)
    print("从 FONE 获取往来数据流程启动")
    print("=" * 60)

    notify_hermes_task(
        event="started",
        flow_name="从FONE获取往来数据",
        payload={"year": year, "month": month},
    )

    try:
        # 计算默认年月
        if year is None or month is None:
            default_year, default_month = _get_last_month()
            year = year if year is not None else default_year
            month = month if month is not None else default_month

        start_date = f"{year}-{month:02d}-01"
        end_date = _month_end(year, month)

        print(f"目标月份: {year}年{month}月")
        print(f"日期范围: {start_date} ~ {end_date}")

        # Step 1: 获取 token
        print("\n【步骤1】获取 FONE 认证凭证...")
        login_result = get_fone_token_task()
        ticket = login_result["ticket"]

        # Step 2: 执行脚本
        print("\n【步骤2】执行从 FONE 获取往来数据脚本...")
        result = execute_fone_recon_script_task(
            ticket=ticket,
            start_date=start_date,
            end_date=end_date,
        )

        print("\n" + "=" * 60)
        print("从 FONE 获取往来数据流程完成！")
        print(f"  脚本状态: {result['script_status']}")
        print(f"  控制台日志: {len(result['console_logs'])} 条")
        print("=" * 60)

        notify_hermes_task(
            event="completed",
            flow_name="从FONE获取往来数据",
            payload={
                "year": year,
                "month": month,
                "start_date": start_date,
                "end_date": end_date,
                "script_status": result.get("script_status"),
                "summary": f"从FONE获取往来数据完成，{year}年{month}月，脚本状态: {result.get('script_status')}",
            },
        )
    except Exception as e:
        error_msg = f"从 FONE 获取往来数据流程失败: {str(e)}"
        print(f"\n{error_msg}")
        notify_hermes_task(
            event="failed",
            flow_name="从FONE获取往来数据",
            payload={"error": str(e), "error_type": type(e).__name__, "year": year, "month": month},
        )
        raise Exception(error_msg) from e
