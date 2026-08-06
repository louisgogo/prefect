"""Unified Prefect flow for business-report reference and acquiring data."""

import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from prefect import flow, get_run_logger
from prefect.context import get_run_context

from modules.common.tasks.notify_hermes_task import notify_hermes_task

from ..tasks.business_data_refresh_tasks import (
    DATASET_CODES,
    BusinessDataRefreshError,
    _connect_finance,
    refresh_acquiring_metrics_task,
    refresh_customer_task,
    refresh_material_task,
    refresh_rd_project_task,
    refresh_supplier_task,
)

DATASET_LABELS = {
    "customer": "客户主数据",
    "material": "物料主数据",
    "rd_project": "研发项目主数据",
    "supplier": "供应商主数据",
    "acquiring_metrics": "收单业务指标",
}


def resolve_datasets(datasets: Optional[Sequence[str]]) -> List[str]:
    if datasets is None:
        return list(DATASET_CODES)
    if isinstance(datasets, str):
        values = [item.strip() for item in datasets.split(",") if item.strip()]
    else:
        values = [str(item).strip() for item in datasets if str(item).strip()]
    if not values:
        raise ValueError("至少选择一个要更新的数据集")
    unknown = sorted(set(values) - set(DATASET_CODES))
    if unknown:
        raise ValueError(f"不支持的数据集：{', '.join(unknown)}")
    return [code for code in DATASET_CODES if code in set(values)]


def summarize_results(results: Mapping[str, Dict[str, Any]]) -> Dict[str, Any]:
    completed = [code for code, item in results.items() if item["status"] == "completed"]
    failed = [code for code, item in results.items() if item["status"] == "failed"]
    status = "completed" if not failed else ("failed" if not completed else "partial_failed")
    return {
        "status": status,
        "completed_datasets": completed,
        "failed_datasets": failed,
        "items": dict(results),
    }


def _begin_run(run_id: uuid.UUID, datasets: Sequence[str], requested_by: Optional[str]) -> None:
    connection = _connect_finance()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE sys_business_data_sync_run
                SET status = 'failed',
                    error_message = COALESCE(error_message, '运行超过六小时，已自动解除同步锁'),
                    updated_at = NOW(), completed_at = NOW()
                WHERE status IN ('queued', 'running')
                  AND updated_at < NOW() - INTERVAL '6 hours'
                """
            )
            cursor.execute(
                """
                INSERT INTO sys_business_data_sync_run (
                    id, prefect_flow_run_id, requested_by, requested_datasets,
                    trigger_source, status, started_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, 'running', NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET
                    requested_by = EXCLUDED.requested_by,
                    requested_datasets = EXCLUDED.requested_datasets,
                    trigger_source = EXCLUDED.trigger_source,
                    status = 'running', started_at = NOW(), updated_at = NOW(),
                    completed_at = NULL, error_message = NULL
                """,
                (
                    str(run_id),
                    str(run_id),
                    requested_by,
                    list(datasets),
                    "manual" if requested_by else "scheduled",
                ),
            )
            for code in datasets:
                cursor.execute(
                    """
                    INSERT INTO sys_business_data_sync_item (
                        id, run_id, dataset_code, status, created_at, updated_at
                    ) VALUES (%s, %s, %s, 'queued', NOW(), NOW())
                    ON CONFLICT (run_id, dataset_code) DO UPDATE SET
                        status = 'queued', source_rows = NULL, target_rows = NULL,
                        watermark = NULL, error_message = NULL,
                        started_at = NULL, completed_at = NULL, updated_at = NOW()
                    """,
                    (str(uuid.uuid4()), str(run_id), code),
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _mark_item_running(run_id: uuid.UUID, dataset: str) -> None:
    connection = _connect_finance()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE sys_business_data_sync_item
                SET status = 'running', started_at = NOW(), updated_at = NOW()
                WHERE run_id = %s AND dataset_code = %s
                """,
                (str(run_id), dataset),
            )
            cursor.execute(
                "UPDATE sys_business_data_sync_run SET updated_at = NOW() WHERE id = %s",
                (str(run_id),),
            )
        connection.commit()
    finally:
        connection.close()


def _mark_item_finished(
    run_id: uuid.UUID,
    dataset: str,
    *,
    status: str,
    result: Optional[Mapping[str, Any]] = None,
    error_message: Optional[str] = None,
) -> None:
    connection = _connect_finance()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE sys_business_data_sync_item
                SET status = %s, source_rows = %s, target_rows = %s,
                    watermark = %s, error_message = %s,
                    completed_at = NOW(), updated_at = NOW()
                WHERE run_id = %s AND dataset_code = %s
                """,
                (
                    status,
                    (result or {}).get("source_rows"),
                    (result or {}).get("target_rows"),
                    (result or {}).get("watermark"),
                    error_message,
                    str(run_id),
                    dataset,
                ),
            )
        connection.commit()
    finally:
        connection.close()


def _finish_run(run_id: uuid.UUID, summary: Mapping[str, Any]) -> None:
    errors = [
        f"{DATASET_LABELS[code]}：{summary['items'][code].get('error_message', '')}"
        for code in summary["failed_datasets"]
    ]
    connection = _connect_finance()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE sys_business_data_sync_run
                SET status = %s, error_message = %s,
                    completed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (summary["status"], "\n".join(errors) or None, str(run_id)),
            )
        connection.commit()
    finally:
        connection.close()


def execute_dataset_runners(
    datasets: Sequence[str],
    runners: Mapping[str, Callable[[], Dict[str, Any]]],
    *,
    on_start: Callable[[str], None],
    on_success: Callable[[str, Mapping[str, Any]], None],
    on_failure: Callable[[str, Exception], None],
) -> Dict[str, Any]:
    results: Dict[str, Dict[str, Any]] = {}
    for dataset in datasets:
        on_start(dataset)
        try:
            result = runners[dataset]()
            on_success(dataset, result)
            results[dataset] = {"status": "completed", **result}
        except Exception as exc:  # dataset failures must not discard prior successes
            on_failure(dataset, exc)
            results[dataset] = {"status": "failed", "error_message": str(exc)}
    return summarize_results(results)


@flow(name="业报基础数据更新", log_prints=False)
def business_data_refresh_flow(
    datasets: Optional[List[str]] = None,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Refresh all or selected business-report datasets from authoritative sources."""
    logger = get_run_logger()
    selected = resolve_datasets(datasets)
    run_id = uuid.UUID(str(get_run_context().flow_run.id))
    _begin_run(run_id, selected, requested_by)
    notify_hermes_task(
        event="started",
        flow_name="业报基础数据更新",
        payload={"datasets": selected, "requested_by": requested_by},
    )
    runners: Dict[str, Callable[[], Dict[str, Any]]] = {
        "customer": refresh_customer_task,
        "material": refresh_material_task,
        "rd_project": refresh_rd_project_task,
        "supplier": refresh_supplier_task,
        "acquiring_metrics": refresh_acquiring_metrics_task,
    }
    summary = execute_dataset_runners(
        selected,
        runners,
        on_start=lambda code: _mark_item_running(run_id, code),
        on_success=lambda code, result: _mark_item_finished(
            run_id, code, status="completed", result=result
        ),
        on_failure=lambda code, exc: _mark_item_finished(
            run_id, code, status="failed", error_message=str(exc)
        ),
    )
    summary.update(
        {
            "flow_run_id": str(run_id),
            "requested_by": requested_by,
            "completed_at": datetime.now().isoformat(),
        }
    )
    _finish_run(run_id, summary)
    event = "completed" if summary["status"] == "completed" else "failed"
    notify_hermes_task(event=event, flow_name="业报基础数据更新", payload=summary)
    logger.info(
        "业报基础数据更新结束：成功 %s 个，失败 %s 个",
        len(summary["completed_datasets"]),
        len(summary["failed_datasets"]),
    )
    if summary["failed_datasets"]:
        raise BusinessDataRefreshError(f"以下数据集更新失败：{', '.join(summary['failed_datasets'])}")
    return summary
