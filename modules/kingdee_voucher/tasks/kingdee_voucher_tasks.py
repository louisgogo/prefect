"""Prefect tasks for synchronizing the Kingdee GL voucher journal."""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import requests
from mypackage.utilities import connect_to_db
from prefect import get_run_logger, task
from psycopg2.errors import UniqueViolation
from psycopg2.extras import Json, execute_batch

KINGDEE_VOUCHER_FORM_ID = "GL_VOUCHER"
KINGDEE_VOUCHER_TOKEN_ENV = "XGD_TOKEN"
KINGDEE_VOUCHER_TOKEN_FALLBACK_ENV = "AIHUB_FONE_API_TOKEN"
KINGDEE_VOUCHER_DATABASE_URL_ENV = "KINGDEE_VOUCHER_DATABASE_URL"
KINGDEE_VOUCHER_BASE_URL_ENV = "KINGDEE_VOUCHER_BASE_URL"

KINGDEE_VOUCHER_FIELD_KEYS = (
    "FEntity_FEntryID",
    "FBillNo",
    "FAccountBookID",
    "FAccountBookID.FNumber",
    "FAccountBookID.FName",
    "FDate",
    "FVOUCHERGROUPID",
    "FVOUCHERGROUPID.FNumber",
    "FVOUCHERGROUPID.FName",
    "FATTACHMENTS",
    "FYEAR",
    "FPERIOD",
    "FDEBITTOTAL",
    "FCREDITTOTAL",
    "FInvalid",
    "FCHECKERID",
    "FCHECKERID.FName",
    "FPOSTERID",
    "FPOSTERID.FName",
    "FCASHIERID",
    "FCASHIERID.FName",
    "FAuditDate",
    "FDocumentStatus",
    "FCreatorId",
    "FCreatorId.FName",
    "FCreateDate",
    "FISADJUSTVOUCHER",
    "FSystemID",
    "FSourceBillKey",
    "FBASECURRENCYID",
    "FBASECURRENCYID.FNumber",
    "FBASECURRENCYID.FName",
    "FACCBOOKORGID",
    "FACCBOOKORGID.FNumber",
    "FACCBOOKORGID.FName",
    "FISCASHFLOW",
    "FIsSplit",
    "FVOUCHERGROUPNO",
    "FModifierId",
    "FModifierId.FName",
    "FModifyDate",
    "FPRINTTIMES",
    "FIMPORTVERSION",
    "FDepositIn",
    "FDepositOut",
    "FBUSDATE",
    "FPostDate",
    "FISREDWRITEOFF",
    "F_BHR_Offsettype",
    "FEXPLANATION",
    "FACCOUNTID",
    "FACCOUNTID.FNumber",
    "FACCOUNTID.FName",
    "FDetailID",
    "FCURRENCYID",
    "FCURRENCYID.FNumber",
    "FCURRENCYID.FName",
    "FEXCHANGERATETYPE",
    "FEXCHANGERATE",
    "FAMOUNTFOR",
    "FDEBIT",
    "FCREDIT",
    "FDC",
    "FSettleTypeID",
    "FAmount",
    "FSETTLENO",
    "FCASHFLOWITEM",
    "FISMULTICOLLECT",
    "FQty",
    "FPrice",
    "FUnitId",
    "FUnitId.FName",
    "FAcctUnitQty",
    "FBaseUnitQty",
    "FEXPORTENTRYID",
    "FBUSNO",
    "FBaseExplanation",
    "FCashFlowData",
)

DOCUMENT_STATUS_NAMES = {
    "Z": "暂存",
    "A": "创建",
    "B": "审核中",
    "C": "已审核",
    "D": "重新审核",
}

JOURNAL_COLUMNS = (
    "source_entry_id",
    "fiscal_year",
    "fiscal_period",
    "voucher_date",
    "account_book_id",
    "account_book_number",
    "account_book_name",
    "bill_no",
    "voucher_group_id",
    "voucher_group_number",
    "voucher_group_name",
    "voucher_group_no",
    "attachments",
    "debit_total",
    "credit_total",
    "invalid_status",
    "document_status",
    "document_status_name",
    "checker_id",
    "checker_name",
    "poster_id",
    "poster_name",
    "cashier_id",
    "cashier_name",
    "audit_date",
    "creator_id",
    "creator_name",
    "create_date",
    "modifier_id",
    "modifier_name",
    "modify_date",
    "is_adjust_voucher",
    "source_system",
    "source_bill_key",
    "base_currency_id",
    "base_currency_number",
    "base_currency_name",
    "account_book_org_id",
    "account_book_org_number",
    "account_book_org_name",
    "is_cashflow",
    "is_split",
    "print_times",
    "import_version",
    "deposit_in",
    "deposit_out",
    "business_date",
    "post_date",
    "is_red_write_off",
    "offset_type",
    "explanation",
    "account_id",
    "account_number",
    "account_name",
    "detail_id",
    "currency_id",
    "currency_number",
    "currency_name",
    "exchange_rate_type_id",
    "exchange_rate",
    "amount_for",
    "debit",
    "credit",
    "debit_credit_direction",
    "settle_type_id",
    "amount",
    "settle_no",
    "cashflow_item_status",
    "is_multi_collect",
    "quantity",
    "price",
    "unit_id",
    "unit_name",
    "account_unit_quantity",
    "base_unit_quantity",
    "export_entry_id",
    "business_no",
    "base_explanation_id",
    "cashflow_data",
    "source_payload",
)

INSERT_PLACEHOLDERS = ", ".join(f"%({column})s" for column in JOURNAL_COLUMNS)
UPDATE_ASSIGNMENTS = ",\n        ".join(
    f"{column} = EXCLUDED.{column}" for column in JOURNAL_COLUMNS if column != "source_entry_id"
)
UPSERT_JOURNAL_SQL = f"""
    INSERT INTO fact_gl_voucher_journal ({", ".join(JOURNAL_COLUMNS)})
    VALUES ({INSERT_PLACEHOLDERS})
    ON CONFLICT (source_entry_id) DO UPDATE SET
        {UPDATE_ASSIGNMENTS},
        last_synced_at = NOW()
"""


class KingdeeVoucherSyncError(RuntimeError):
    """Raised when a Kingdee voucher month cannot complete safely."""


def resolve_voucher_months(
    year: int,
    month: Optional[int] = None,
    months: Optional[Sequence[int]] = None,
    page_size: int = 5000,
) -> Tuple[int, List[int], int]:
    """Validate explicit accounting-period parameters for the Prefect flow."""

    if isinstance(year, bool) or not isinstance(year, int) or not 2000 <= year <= 2200:
        raise ValueError("year 必须是 2000-2200 之间的四位整数")
    if month is not None and months is not None:
        raise ValueError("month 和 months 只能填写一个")
    if month is None and months is None:
        raise ValueError("必须显式填写 month 或 months")

    selected = [month] if month is not None else list(months or [])
    if not selected:
        raise ValueError("months 不能为空")
    if any(isinstance(value, bool) or not isinstance(value, int) for value in selected):
        raise ValueError("月份必须是整数")
    if any(value < 1 or value > 13 for value in selected):
        raise ValueError("月份必须在 1-13 之间")
    if page_size < 1 or page_size > 10000:
        raise ValueError("page_size 必须在 1-10000 之间")
    return year, sorted(set(selected)), page_size


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _integer(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise KingdeeVoucherSyncError(f"无法转换金蝶整数值: {value!r}") from exc


def _boolean(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "t", "yes"}:
        return True
    if normalized in {"false", "0", "f", "no"}:
        return False
    raise KingdeeVoucherSyncError(f"无法转换金蝶布尔值: {value!r}")


def _decimal(value: Any, *, nullable: bool = False) -> Optional[Decimal]:
    if value is None or value == "":
        return None if nullable else Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise KingdeeVoucherSyncError(f"无法转换金蝶金额值: {value!r}") from exc


def _datetime(value: Any) -> Optional[datetime]:
    normalized = _text(value)
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KingdeeVoucherSyncError(f"无法转换金蝶日期时间: {normalized!r}") from exc
    return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed


def _date(value: Any) -> Optional[date]:
    parsed = _datetime(value)
    return parsed.date() if parsed else None


def normalize_voucher_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert one Kingdee BillQuery row to the fact-table record."""

    source_entry_id = _integer(row.get("FEntity_FEntryID"))
    voucher_date = _date(row.get("FDate"))
    fiscal_year = _integer(row.get("FYEAR"))
    fiscal_period = _integer(row.get("FPERIOD"))
    bill_no = _text(row.get("FBillNo"))
    if source_entry_id is None:
        raise KingdeeVoucherSyncError("金蝶凭证分录缺少 FEntity_FEntryID")
    if voucher_date is None or fiscal_year is None or fiscal_period is None or not bill_no:
        raise KingdeeVoucherSyncError(f"金蝶凭证分录 {source_entry_id} 缺少年度、期间、日期或凭证编号")

    document_status = _text(row.get("FDocumentStatus"))
    return {
        "source_entry_id": source_entry_id,
        "fiscal_year": fiscal_year,
        "fiscal_period": fiscal_period,
        "voucher_date": voucher_date,
        "account_book_id": _integer(row.get("FAccountBookID")),
        "account_book_number": _text(row.get("FAccountBookID.FNumber")),
        "account_book_name": _text(row.get("FAccountBookID.FName")),
        "bill_no": bill_no,
        "voucher_group_id": _integer(row.get("FVOUCHERGROUPID")),
        "voucher_group_number": _text(row.get("FVOUCHERGROUPID.FNumber")),
        "voucher_group_name": _text(row.get("FVOUCHERGROUPID.FName")),
        "voucher_group_no": _integer(row.get("FVOUCHERGROUPNO")),
        "attachments": _integer(row.get("FATTACHMENTS")),
        "debit_total": _decimal(row.get("FDEBITTOTAL")),
        "credit_total": _decimal(row.get("FCREDITTOTAL")),
        "invalid_status": _text(row.get("FInvalid")),
        "document_status": document_status,
        "document_status_name": DOCUMENT_STATUS_NAMES.get(document_status or ""),
        "checker_id": _integer(row.get("FCHECKERID")),
        "checker_name": _text(row.get("FCHECKERID.FName")),
        "poster_id": _integer(row.get("FPOSTERID")),
        "poster_name": _text(row.get("FPOSTERID.FName")),
        "cashier_id": _integer(row.get("FCASHIERID")),
        "cashier_name": _text(row.get("FCASHIERID.FName")),
        "audit_date": _datetime(row.get("FAuditDate")),
        "creator_id": _integer(row.get("FCreatorId")),
        "creator_name": _text(row.get("FCreatorId.FName")),
        "create_date": _datetime(row.get("FCreateDate")),
        "modifier_id": _integer(row.get("FModifierId")),
        "modifier_name": _text(row.get("FModifierId.FName")),
        "modify_date": _datetime(row.get("FModifyDate")),
        "is_adjust_voucher": _boolean(row.get("FISADJUSTVOUCHER")),
        "source_system": _text(row.get("FSystemID")),
        "source_bill_key": _text(row.get("FSourceBillKey")),
        "base_currency_id": _integer(row.get("FBASECURRENCYID")),
        "base_currency_number": _text(row.get("FBASECURRENCYID.FNumber")),
        "base_currency_name": _text(row.get("FBASECURRENCYID.FName")),
        "account_book_org_id": _integer(row.get("FACCBOOKORGID")),
        "account_book_org_number": _text(row.get("FACCBOOKORGID.FNumber")),
        "account_book_org_name": _text(row.get("FACCBOOKORGID.FName")),
        "is_cashflow": _boolean(row.get("FISCASHFLOW")),
        "is_split": _integer(row.get("FIsSplit")),
        "print_times": _integer(row.get("FPRINTTIMES")),
        "import_version": _text(row.get("FIMPORTVERSION")),
        "deposit_in": _text(row.get("FDepositIn")),
        "deposit_out": _text(row.get("FDepositOut")),
        "business_date": _datetime(row.get("FBUSDATE")),
        "post_date": _datetime(row.get("FPostDate")),
        "is_red_write_off": _boolean(row.get("FISREDWRITEOFF")),
        "offset_type": _text(row.get("F_BHR_Offsettype")),
        "explanation": _text(row.get("FEXPLANATION")),
        "account_id": _integer(row.get("FACCOUNTID")),
        "account_number": _text(row.get("FACCOUNTID.FNumber")),
        "account_name": _text(row.get("FACCOUNTID.FName")),
        "detail_id": _integer(row.get("FDetailID")),
        "currency_id": _integer(row.get("FCURRENCYID")),
        "currency_number": _text(row.get("FCURRENCYID.FNumber")),
        "currency_name": _text(row.get("FCURRENCYID.FName")),
        "exchange_rate_type_id": _integer(row.get("FEXCHANGERATETYPE")),
        "exchange_rate": _decimal(row.get("FEXCHANGERATE")),
        "amount_for": _decimal(row.get("FAMOUNTFOR")),
        "debit": _decimal(row.get("FDEBIT")),
        "credit": _decimal(row.get("FCREDIT")),
        "debit_credit_direction": _integer(row.get("FDC")),
        "settle_type_id": _integer(row.get("FSettleTypeID")),
        "amount": _decimal(row.get("FAmount")),
        "settle_no": _text(row.get("FSETTLENO")),
        "cashflow_item_status": _boolean(row.get("FCASHFLOWITEM")),
        "is_multi_collect": _boolean(row.get("FISMULTICOLLECT")),
        "quantity": _decimal(row.get("FQty")),
        "price": _decimal(row.get("FPrice")),
        "unit_id": _integer(row.get("FUnitId")),
        "unit_name": _text(row.get("FUnitId.FName")),
        "account_unit_quantity": _decimal(row.get("FAcctUnitQty"), nullable=True),
        "base_unit_quantity": _decimal(row.get("FBaseUnitQty"), nullable=True),
        "export_entry_id": _integer(row.get("FEXPORTENTRYID")),
        "business_no": _text(row.get("FBUSNO")),
        "base_explanation_id": _integer(row.get("FBaseExplanation")),
        "cashflow_data": _text(row.get("FCashFlowData")),
        "source_payload": Json(row, dumps=lambda value: json.dumps(value, ensure_ascii=False)),
    }


def _resolve_token() -> str:
    token = (
        os.environ.get(KINGDEE_VOUCHER_TOKEN_ENV)
        or os.environ.get(KINGDEE_VOUCHER_TOKEN_FALLBACK_ENV)
        or ""
    ).strip()
    if not token:
        raise KingdeeVoucherSyncError(
            f"必须配置 {KINGDEE_VOUCHER_TOKEN_ENV}；" f"兼容环境也可使用 {KINGDEE_VOUCHER_TOKEN_FALLBACK_ENV}"
        )
    return token


def _connect_database():
    database_url = os.environ.get(KINGDEE_VOUCHER_DATABASE_URL_ENV, "").strip()
    if database_url:
        normalized_url = database_url.replace("postgresql+asyncpg://", "postgresql://").replace(
            "postgresql+psycopg://", "postgresql://"
        )
        connection = psycopg2.connect(normalized_url)
        connection.autocommit = False
        return connection

    connection, cursor = connect_to_db()
    if cursor is not None:
        cursor.close()
    if connection is None:
        raise KingdeeVoucherSyncError("无法连接财务数据库")
    return connection


def _response_error_message(data: Any) -> str:
    if not isinstance(data, dict):
        return "未知错误"
    response_status = (data.get("Result") or {}).get("ResponseStatus") or {}
    errors = response_status.get("Errors") or []
    messages = [
        str(item.get("Message") or "").strip()
        for item in errors
        if isinstance(item, dict) and str(item.get("Message") or "").strip()
    ]
    return "; ".join(messages) or "未返回错误详情"


def _request_page(
    session: requests.Session,
    *,
    token: str,
    year: int,
    month: int,
    start_row: int,
    page_size: int,
    timeout_seconds: float,
    max_retries: int,
) -> List[Dict[str, Any]]:
    base_url = os.environ.get(KINGDEE_VOUCHER_BASE_URL_ENV, "https://aihub.xgd.com")
    endpoint = f"{base_url.rstrip('/')}/api/proxy/erp/sdk/BillQuery"
    payload = {
        "FormId": KINGDEE_VOUCHER_FORM_ID,
        "FieldKeys": ",".join(KINGDEE_VOUCHER_FIELD_KEYS),
        "FilterString": f"FYEAR = {year} AND FPERIOD = {month}",
        "OrderString": "FEntity_FEntryID ASC",
        "StartRow": start_row,
        "Limit": page_size,
    }
    for attempt in range(max_retries + 1):
        try:
            response = session.post(
                endpoint,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=timeout_seconds,
            )
            if (
                response.status_code == 429 or response.status_code >= 500
            ) and attempt < max_retries:
                time.sleep(min(2**attempt, 8))
                continue
            response.raise_for_status()
            data = response.json()
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt < max_retries:
                time.sleep(min(2**attempt, 8))
                continue
            raise KingdeeVoucherSyncError("金蝶凭证接口网络请求失败") from exc
        except (requests.HTTPError, ValueError) as exc:
            raise KingdeeVoucherSyncError("金蝶凭证接口返回无效响应") from exc

        if isinstance(data, list):
            if not all(isinstance(row, dict) for row in data):
                raise KingdeeVoucherSyncError("金蝶凭证接口返回了非对象数据行")
            return data
        raise KingdeeVoucherSyncError(f"金蝶凭证接口查询失败: {_response_error_message(data)}")

    raise KingdeeVoucherSyncError("金蝶凭证接口重试次数已用尽")


def _start_run(connection, year: int, month: int, page_size: int) -> uuid.UUID:
    run_id = uuid.uuid4()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE kingdee_gl_voucher_sync_runs
            SET status = 'failed',
                error_message = COALESCE(error_message, '运行超过六小时，已自动解除同步锁'),
                updated_at = NOW(), completed_at = NOW()
            WHERE fiscal_year = %s AND fiscal_period = %s
              AND status = 'running'
              AND updated_at < NOW() - INTERVAL '6 hours'
            """,
            (year, month),
        )
        try:
            cursor.execute(
                """
                INSERT INTO kingdee_gl_voucher_sync_runs (
                    id, fiscal_year, fiscal_period, page_size, status
                ) VALUES (%s, %s, %s, %s, 'running')
                """,
                (str(run_id), year, month, page_size),
            )
            connection.commit()
        except UniqueViolation as exc:
            connection.rollback()
            raise KingdeeVoucherSyncError(f"{year} 年第 {month} 期已有同步任务正在运行") from exc
    return run_id


def _upsert_page(connection, records: Iterable[Dict[str, Any]]) -> Tuple[int, int]:
    deduplicated = {record["source_entry_id"]: record for record in records}
    if not deduplicated:
        return 0, 0
    source_ids = list(deduplicated)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT source_entry_id
            FROM fact_gl_voucher_journal
            WHERE source_entry_id = ANY(%s)
            """,
            (source_ids,),
        )
        existing_ids = {int(row[0]) for row in cursor.fetchall()}
        execute_batch(cursor, UPSERT_JOURNAL_SQL, list(deduplicated.values()), page_size=500)
    updated = len(existing_ids)
    return len(deduplicated) - updated, updated


def _update_run_progress(
    connection,
    run_id: uuid.UUID,
    *,
    pages_completed: int,
    source_rows: int,
    inserted_rows: int,
    updated_rows: int,
    max_source_modified_at: Optional[datetime],
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE kingdee_gl_voucher_sync_runs
            SET pages_completed = %s,
                source_rows = %s,
                inserted_rows = %s,
                updated_rows = %s,
                max_source_modified_at = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                pages_completed,
                source_rows,
                inserted_rows,
                updated_rows,
                max_source_modified_at,
                str(run_id),
            ),
        )


def _complete_run(connection, run_id: uuid.UUID) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE kingdee_gl_voucher_sync_runs
            SET status = 'completed', error_message = NULL,
                updated_at = NOW(), completed_at = NOW()
            WHERE id = %s
            """,
            (str(run_id),),
        )
    connection.commit()


def _fail_run(connection, run_id: uuid.UUID, error: Exception) -> None:
    connection.rollback()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE kingdee_gl_voucher_sync_runs
            SET status = 'failed', error_message = %s,
                updated_at = NOW(), completed_at = NOW()
            WHERE id = %s
            """,
            (str(error)[:4000], str(run_id)),
        )
    connection.commit()


@task(name="同步金蝶凭证期间", retries=1, retry_delay_seconds=30)
def sync_kingdee_voucher_period_task(
    year: int,
    month: int,
    page_size: int = 5000,
    timeout_seconds: float = 120.0,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Synchronize one explicit Kingdee accounting month."""

    year, selected_months, page_size = resolve_voucher_months(
        year=year,
        month=month,
        page_size=page_size,
    )
    month = selected_months[0]
    logger = get_run_logger()
    token = _resolve_token()
    connection = _connect_database()
    session = requests.Session()
    run_id: Optional[uuid.UUID] = None
    start_row = 0
    pages_completed = 0
    source_rows = 0
    inserted_rows = 0
    updated_rows = 0
    max_source_modified_at: Optional[datetime] = None
    last_source_entry_id: Optional[int] = None
    try:
        run_id = _start_run(connection, year, month, page_size)
        while True:
            rows = _request_page(
                session,
                token=token,
                year=year,
                month=month,
                start_row=start_row,
                page_size=page_size,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
            )
            if not rows:
                break

            records = [normalize_voucher_row(row) for row in rows]
            if any(
                record["fiscal_year"] != year or record["fiscal_period"] != month
                for record in records
            ):
                raise KingdeeVoucherSyncError(f"金蝶返回了不属于 {year} 年第 {month} 期的分录")
            if (
                last_source_entry_id is not None
                and records[0]["source_entry_id"] <= last_source_entry_id
            ):
                raise KingdeeVoucherSyncError(
                    f"金蝶分页未继续前进：第 {month} 期分录标识 "
                    f"{records[0]['source_entry_id']} 不大于上一页末尾 {last_source_entry_id}"
                )
            last_source_entry_id = records[-1]["source_entry_id"]

            inserted, updated = _upsert_page(connection, records)
            pages_completed += 1
            source_rows += len(rows)
            inserted_rows += inserted
            updated_rows += updated
            page_max = max(
                (record["modify_date"] for record in records if record["modify_date"]),
                default=None,
            )
            if page_max and (max_source_modified_at is None or page_max > max_source_modified_at):
                max_source_modified_at = page_max
            _update_run_progress(
                connection,
                run_id,
                pages_completed=pages_completed,
                source_rows=source_rows,
                inserted_rows=inserted_rows,
                updated_rows=updated_rows,
                max_source_modified_at=max_source_modified_at,
            )
            connection.commit()
            logger.info(
                f"{year} 年第 {month} 期已完成 {source_rows} 行，" f"新增 {inserted_rows}，更新 {updated_rows}"
            )

            start_row += len(rows)

        _complete_run(connection, run_id)
        return {
            "run_id": str(run_id),
            "year": year,
            "month": month,
            "pages_completed": pages_completed,
            "source_rows": source_rows,
            "inserted_rows": inserted_rows,
            "updated_rows": updated_rows,
            "max_source_modified_at": (
                max_source_modified_at.isoformat() if max_source_modified_at else None
            ),
        }
    except Exception as exc:
        if run_id is not None:
            _fail_run(connection, run_id, exc)
        raise
    finally:
        session.close()
        connection.close()
