"""研发项目收益计算流程。"""

from datetime import date
from typing import Dict, Optional

from modules.common.tasks.notify_hermes_task import notify_hermes_task
from prefect import flow

from ..tasks.rd_project_profitability_tasks import (
    calculate_rd_project_profitability_task,
    export_rd_project_profitability_excel_task,
    get_default_rd_project_period,
    load_rd_project_profitability_sources_task,
    replace_rd_project_profitability_snapshot_task,
    resolve_rd_project_period,
    validate_rd_project_profitability_task,
)


def _get_rd_project_profitability_defaults_by_date(
    reference_date: Optional[date] = None,
) -> Dict[str, object]:
    """返回部署和 Prefect UI 使用的最近已结月年初至今默认参数。"""
    start_date, end_date = get_default_rd_project_period(reference_date)
    return {
        "start_date": start_date.date().isoformat(),
        "end_date": end_date.date().isoformat(),
        "write_to_db": False,
        "target_table": "fact_rd_project_profitability",
        "output_dir": None,
        "download_base_url": None,
        "notify_frontend": True,
        "callback_url": None,
        "tolerance": 0.01,
    }


@flow(name="rd_project_profitability_flow", log_prints=True)
def rd_project_profitability_flow(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    write_to_db: bool = False,
    target_table: str = "fact_rd_project_profitability",
    output_dir: Optional[str] = None,
    download_base_url: Optional[str] = None,
    notify_frontend: bool = True,
    callback_url: Optional[str] = None,
    tolerance: float = 0.01,
) -> Dict[str, object]:
    """按显式期间计算研发项目收入、成本、费用和剩余收益。"""
    period_start, period_end = resolve_rd_project_period(start_date, end_date)
    print(
        "开始研发项目收益计算："
        f"period_start={period_start.date()}, period_end={period_end.date()}, "
        f"write_to_db={write_to_db}, target_table={target_table}"
    )
    if notify_frontend:
        notify_hermes_task(
            event="started",
            flow_name="研发项目收益分析",
            callback_url=callback_url,
            include_logs=False,
            payload={
                "period_start": period_start.date().isoformat(),
                "period_end": period_end.date().isoformat(),
            },
        )

    try:
        sources = load_rd_project_profitability_sources_task(period_start, period_end)
        result = calculate_rd_project_profitability_task(
            sources=sources,
            period_start=period_start,
            period_end=period_end,
        )
        validation = validate_rd_project_profitability_task(
            result=result,
            tolerance=tolerance,
        )
        report = export_rd_project_profitability_excel_task(
            result=result,
            validation=validation,
            output_dir=output_dir,
            download_base_url=download_base_url,
        )

        write_metrics = None
        if write_to_db:
            write_metrics = replace_rd_project_profitability_snapshot_task(
                result=result,
                target_table=target_table,
            )

        response = {
            "status": "completed",
            "period_start": period_start.date().isoformat(),
            "period_end": period_end.date().isoformat(),
            "report": report,
            "calculation_metrics": result["metrics"],
            "validation": validation,
            "write_metrics": write_metrics,
        }
        if notify_frontend:
            response["callback"] = notify_hermes_task(
                event="completed",
                flow_name="研发项目收益分析",
                callback_url=callback_url,
                payload={
                    "period_start": response["period_start"],
                    "period_end": response["period_end"],
                    "output_path": report["output_path"],
                    "download_url": report["download_url"],
                    "file_name": report["file_name"],
                    "row_count": report["row_count"],
                    "backup_row_count": report["backup_row_count"],
                    "remaining_profit": validation["remaining_profit_total"],
                    "summary": (
                        f"生成研发项目收益 Excel，共 {report['row_count']} 行；"
                        f"收入成本备查 {report['backup_row_count']} 行；"
                        f"剩余收益 {validation['remaining_profit_total']:.2f} 元"
                    ),
                },
            )

        print(
            "研发项目收益计算完成："
            f"rows={len(result['detail'])}, "
            f"backup_rows={report['backup_row_count']}, "
            f"remaining_profit={validation['remaining_profit_total']:.2f}"
        )
        return response
    except Exception as exc:
        if notify_frontend:
            notify_hermes_task(
                event="failed",
                flow_name="研发项目收益分析",
                callback_url=callback_url,
                payload={
                    "period_start": period_start.date().isoformat(),
                    "period_end": period_end.date().isoformat(),
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
            )
        raise
