"""往来对账 - 数据自动填充 Tasks

检测 recon_name 表是否包含目标月份的数据，如果没有则从已有最新日期的数据复制.
"""
import os
import sys
from datetime import date, timedelta
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import text

from prefect import task

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _calc_target_month(target_date: Optional[str] = None) -> date:
    """计算目标月份."""
    if target_date:
        try:
            target = pd.to_datetime(target_date).date()
        except Exception:
            today = date.today()
            target = today.replace(day=1) - timedelta(days=1)
    else:
        today = date.today()
        target = today.replace(day=1) - timedelta(days=1)

    return date(target.year, target.month, 1)


def _get_month_end(month: date) -> date:
    """获取某月的结束日期（下月1日）."""
    if month.month == 12:
        return date(month.year + 1, 1, 1)
    return date(month.year, month.month + 1, 1)


@task(name="check_and_fill_recon_data", log_prints=True)
def check_and_fill_recon_data_task(target_date: Optional[str] = None) -> Dict[str, Any]:
    """
    检测 recon_name 表是否包含目标月份的数据.

    如果没有目标月数据，则从表中已有最新日期的数据复制，日期修改为目标月份.
    如果已经存在目标月数据，则不做任何处理.

    Args:
        target_date: 目标日期，格式 YYYY-MM-DD，None 时取上个自然月.

    Returns:
        {
            'success': bool,
            'action': str,  # 'filled', 'skipped', 'error'
            'message': str,
            'filled_count': int,  # 填充的记录数
            'source_month': str,  # 数据来源月份
            'target_month': str,  # 目标月份
        }
    """
    from mypackage.utilities import engine_to_db

    target_month = _calc_target_month(target_date)

    print(f"--> 检测 recon_name 表数据状态")
    print(f"    目标月份: {target_month}")

    try:
        engine = engine_to_db()

        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # 1. 检查目标月数据是否存在
                target_month_end = _get_month_end(target_month)
                result = conn.execute(
                    text('SELECT COUNT(*) FROM recon_name WHERE "日期" >= :start AND "日期" < :end'),
                    {"start": target_month, "end": target_month_end},
                )
                target_count = result.scalar()

                print(f"--> {target_month} 月已有数据: {target_count} 条")

                if target_count > 0:
                    trans.commit()
                    return {
                        "success": True,
                        "action": "skipped",
                        "message": f"{target_month} 月已有 {target_count} 条数据，跳过自动填充",
                        "filled_count": 0,
                        "source_month": "",
                        "target_month": str(target_month),
                    }

                # 2. 查找表中已有最新日期的数据
                print(f"--> {target_month} 月无数据，查找表中最新数据...")

                latest_date_result = conn.execute(
                    text('SELECT MAX("日期") FROM recon_name WHERE "日期" < :target'),
                    {"target": target_month},
                )
                latest_date = latest_date_result.scalar()

                if latest_date is None:
                    trans.commit()
                    return {
                        "success": True,
                        "action": "skipped",
                        "message": "表中没有历史数据可供复制",
                        "filled_count": 0,
                        "source_month": "",
                        "target_month": str(target_month),
                    }

                print(f"--> 找到最新数据日期: {latest_date}")

                # 3. 查询该日期的数据
                latest_month = date(latest_date.year, latest_date.month, 1)
                latest_month_end = _get_month_end(latest_month)

                df_source = pd.read_sql(
                    text(
                        """
                    SELECT "单位简称", "嘉联用公司简称", "合并名称", "业报合并名称", "日期"
                    FROM recon_name
                    WHERE "日期" >= :start AND "日期" < :end
                    """
                    ),
                    con=conn,
                    params={"start": latest_month, "end": latest_month_end},
                )

                if df_source.empty:
                    print(f"--> {latest_month} 月无数据可供复制")
                    trans.commit()
                    return {
                        "success": True,
                        "action": "skipped",
                        "message": f"{latest_month} 月无数据，无需填充",
                        "filled_count": 0,
                        "source_month": str(latest_month),
                        "target_month": str(target_month),
                    }

                print(f"--> 找到 {len(df_source)} 条 {latest_month} 月数据，准备复制到 {target_month} 月")

                # 修改日期为目标月份
                df_source["日期"] = target_month

                # 写入数据
                df_source.to_sql("recon_name", con=conn, if_exists="append", index=False)

                filled_count = len(df_source)
                trans.commit()

                print(f"--> 成功复制 {filled_count} 条数据到 {target_month} 月")

                return {
                    "success": True,
                    "action": "filled",
                    "message": f"成功从 {latest_month} 月复制 {filled_count} 条数据到 {target_month} 月",
                    "filled_count": filled_count,
                    "source_month": str(latest_month),
                    "target_month": str(target_month),
                }

            except Exception as e:
                trans.rollback()
                raise RuntimeError(f"复制数据失败: {e}")

    except Exception as e:
        error_msg = f"检测和填充 recon_name 数据失败: {e}"
        print(f"[ERROR] {error_msg}")
        return {
            "success": False,
            "action": "error",
            "message": error_msg,
            "filled_count": 0,
            "source_month": "",
            "target_month": str(target_month),
        }
