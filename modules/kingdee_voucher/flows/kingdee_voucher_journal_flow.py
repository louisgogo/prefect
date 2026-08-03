"""Prefect flow for synchronizing the Kingdee GL voucher journal."""

from typing import Any, Dict, List, Optional

from prefect import flow, get_run_logger

from modules.common.tasks.notify_hermes_task import notify_hermes_task

from ..tasks.kingdee_voucher_tasks import resolve_voucher_months, sync_kingdee_voucher_period_task


@flow(name="kingdee_voucher_journal_flow", log_prints=True)
def kingdee_voucher_journal_flow(
    year: int,
    month: Optional[int] = None,
    months: Optional[List[int]] = None,
    page_size: int = 5000,
) -> Dict[str, Any]:
    """Synchronize one or more explicit Kingdee accounting months in order."""

    year, selected_months, page_size = resolve_voucher_months(
        year=year,
        month=month,
        months=months,
        page_size=page_size,
    )
    logger = get_run_logger()
    period_text = ", ".join(str(value) for value in selected_months)
    notify_hermes_task(
        event="started",
        flow_name="金蝶凭证序时簿同步",
        payload={"year": year, "months": selected_months, "page_size": page_size},
        include_logs=False,
    )

    try:
        results: List[Dict[str, Any]] = []
        for selected_month in selected_months:
            logger.info(f"开始同步金蝶凭证序时簿：{year} 年第 {selected_month} 期")
            result = sync_kingdee_voucher_period_task(
                year=year,
                month=selected_month,
                page_size=page_size,
            )
            results.append(result)

        summary = {
            "year": year,
            "months": selected_months,
            "period_count": len(results),
            "source_rows": sum(int(item["source_rows"]) for item in results),
            "inserted_rows": sum(int(item["inserted_rows"]) for item in results),
            "updated_rows": sum(int(item["updated_rows"]) for item in results),
            "period_results": results,
        }
        logger.info(
            f"金蝶凭证序时簿同步完成：{year} 年第 {period_text} 期，"
            f"源数据 {summary['source_rows']} 行，新增 {summary['inserted_rows']} 行，"
            f"更新 {summary['updated_rows']} 行"
        )
        notify_hermes_task(
            event="completed",
            flow_name="金蝶凭证序时簿同步",
            payload=summary,
        )
        return summary
    except Exception as exc:
        notify_hermes_task(
            event="failed",
            flow_name="金蝶凭证序时簿同步",
            payload={
                "year": year,
                "months": selected_months,
                "error": str(exc),
                "error_type": type(exc).__name__,
            },
        )
        raise
