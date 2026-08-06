"""Safe snapshot-refresh tasks for business-report reference data."""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import psycopg2
import requests
from mypackage.utilities import connect_to_db
from prefect import get_run_logger, task
from psycopg2 import sql
from psycopg2.extras import execute_values

DATASET_CODES = (
    "customer",
    "material",
    "rd_project",
    "supplier",
    "acquiring_metrics",
)

SQLSERVER_CONNECTION_ENV = "BUSINESS_DATA_SQLSERVER_CONNECTION_STRING"
ORACLE_USER_ENV = "BUSINESS_DATA_ORACLE_USER"
ORACLE_PASSWORD_ENV = "BUSINESS_DATA_ORACLE_PASSWORD"
ORACLE_DSN_ENV = "BUSINESS_DATA_ORACLE_DSN"
FINANCE_DATABASE_URL_ENV = "BUSINESS_DATA_FINANCE_DATABASE_URL"
MIN_ROW_RATIO_ENV = "BUSINESS_DATA_MIN_ROW_RATIO"

KINGDEE_FORM_ID = "BD_Supplier"
KINGDEE_USE_ORG_CODE = "1000"
KINGDEE_BASE_URL_ENV = "KINGDEE_VOUCHER_BASE_URL"
KINGDEE_TOKEN_ENVS = ("XGD_TOKEN", "AIHUB_FONE_API_TOKEN")
KINGDEE_VALID_DOCUMENT_STATUSES = frozenset({"C", "D"})
KINGDEE_ENABLED_STATUS = "A"
KINGDEE_FIELD_KEYS = (
    "FSupplierId",
    "FNumber",
    "FName",
    "FCreateOrgId.FNumber",
    "FCreateOrgId.FName",
    "FUseOrgId.FNumber",
    "FUseOrgId.FName",
    "FDocumentStatus",
    "FForbidStatus",
)

ACQUIRING_TABLE_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "t_jl_area_merch_netin": (
        "stat_month",
        "belong_branch",
        "belong_branch_name",
        "product_type",
        "product_type_name",
        "prov_code",
        "city_code",
        "inc_mer_num",
        "stk_mer_num",
        "cancel_mer_num",
        "stop_mer_num",
    ),
    "t_jl_area_trade": (
        "stat_month",
        "belong_branch",
        "belong_branch_name",
        "product_type",
        "product_type_name",
        "prov_code",
        "city_code",
        "cnt",
        "amt",
        "dr_cnt",
        "dr_amt",
        "cr_cnt",
        "cr_amt",
        "wechat_cnt",
        "wechat_amt",
        "alipay_cnt",
        "alipay_amt",
        "unpay_cnt",
        "unpay_amt",
        "cap_cnt",
        "cap_amt",
    ),
    "t_jl_brch_merch_netin": (
        "stat_month",
        "belong_branch",
        "belong_branch_name",
        "product_type",
        "product_type_name",
        "stk_mer_num",
        "stk_license_mer_num",
        "stk_rent_mer_num",
        "stk_micro_mer_num",
        "inc_mer_num",
        "inc_license_mer_num",
        "inc_rent_mer_num",
        "inc_micro_mer_num",
        "cancel_mer_num",
        "app_real_usr_num",
        "stk_real_usr_num",
        "app_bind_usr_num",
        "stk_bind_usr_num",
        "give_rent_user_num",
        "standard_user_num",
        "stk_standard_user_num",
        "over_rent_user_num",
        "stk_over_rent_user_num",
        "apply_refund_rent_user_num",
        "stk_apply_refund_rent_user_num",
        "stk_cancel_user_num",
    ),
    "t_jl_brch_term": (
        "stat_month",
        "belong_branch",
        "belong_branch_name",
        "product_type",
        "product_type_name",
        "ext_term_num",
        "inc_term_num",
        "stk_term_num",
        "stk_using_term_num",
        "inc_rent_term_num",
        "stk_rent_term_num",
        "stop_term_num",
        "cancel_term_num",
        "stk_cancel_term_num",
        "inc_act_term_num",
        "stk_act_term_num",
        "inc_stock_num",
        "total_stock_num",
    ),
    "t_jl_brch_activite_merch": (
        "stat_month",
        "belong_branch",
        "belong_branch_name",
        "product_type",
        "product_type_name",
        "act_mer_cnt",
        "high_act_mer_cnt",
        "act_term_cnt",
        "high_act_term_cnt",
        "stk_act_mer_cnt",
        "inc_act_mer_cnt",
        "stk_high_act_mer_cnt",
        "inc_high_act_mer_cnt",
        "act_3m_mer_cnt",
        "act_3m_term_cnt",
        "no_trd_3m_mer_cnt",
        "no_trd_6m_mer_cnt",
        "good_mer_cnt",
        "sleep_mer_cnt",
    ),
}


class BusinessDataRefreshError(RuntimeError):
    """Raised when an external snapshot is incomplete or unsafe to replace."""


@dataclass(frozen=True)
class SupplierRecord:
    supplier_code: str
    supplier_name: str
    source_supplier_id: int
    create_org_code: str
    create_org_name: str
    use_org_code: str
    use_org_name: str
    document_status: str
    disabled_status: str
    source_present: bool
    is_active: bool


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def clean_optional(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _required_env(name: str) -> str:
    value = clean_text(os.environ.get(name))
    if not value:
        raise BusinessDataRefreshError(f"必须通过 Prefect Worker 环境变量配置 {name}")
    return value


def _minimum_row_ratio(dataset: str) -> float:
    raw = clean_text(os.environ.get(f"{MIN_ROW_RATIO_ENV}_{dataset.upper()}")) or clean_text(
        os.environ.get(MIN_ROW_RATIO_ENV)
    )
    if not raw:
        return 0.5
    try:
        ratio = float(raw)
    except ValueError as exc:
        raise BusinessDataRefreshError(f"{MIN_ROW_RATIO_ENV} 必须是0到1之间的小数") from exc
    if not 0 < ratio <= 1:
        raise BusinessDataRefreshError(f"{MIN_ROW_RATIO_ENV} 必须是0到1之间的小数")
    return ratio


def validate_snapshot_count(dataset: str, source_rows: int, previous_rows: int) -> None:
    if source_rows <= 0:
        raise BusinessDataRefreshError(f"{dataset} 外部源返回空快照，已拒绝替换")
    if previous_rows <= 0:
        return
    minimum = max(1, int(previous_rows * _minimum_row_ratio(dataset)))
    if source_rows < minimum:
        raise BusinessDataRefreshError(
            f"{dataset} 快照仅 {source_rows} 行，低于现有 {previous_rows} 行的安全阈值 {minimum}"
        )


def _deduplicate_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    key: str,
    dataset: str,
) -> List[Dict[str, Any]]:
    deduplicated: Dict[str, Dict[str, Any]] = {}
    for source in rows:
        item = {name: clean_optional(value) for name, value in source.items()}
        business_key = clean_text(item.get(key))
        if not business_key:
            raise BusinessDataRefreshError(f"{dataset} 源记录缺少业务键 {key}")
        item[key] = business_key
        existing = deduplicated.get(business_key)
        if existing is not None and existing != item:
            raise BusinessDataRefreshError(f"{dataset} 业务键重复且内容冲突：{business_key}")
        deduplicated[business_key] = item
    if not deduplicated:
        raise BusinessDataRefreshError(f"{dataset} 外部源返回空快照")
    return [deduplicated[value] for value in sorted(deduplicated)]


def normalize_customer_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized = _deduplicate_rows(
        (
            {
                "cust_code": row.get("fnumber"),
                "cust_name": row.get("fname"),
                "country": row.get("fcountry"),
                "province": row.get("fprovince"),
                "city": row.get("fcity"),
                "district": row.get("fprovincial"),
            }
            for row in rows
        ),
        key="cust_code",
        dataset="customer",
    )
    by_code = {row["cust_code"]: row for row in normalized}
    by_code["C99"] = {
        "cust_code": "C99",
        "cust_name": "不分客户",
        "country": None,
        "province": None,
        "city": None,
        "district": None,
    }
    return [by_code[code] for code in sorted(by_code)]


def normalize_material_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    precedence = {"1000": 0, "1700": 1, "1200": 2}
    selected: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    for row in rows:
        org_code = clean_text(row.get("forgid"))
        if org_code not in precedence:
            continue
        encoding = clean_text(row.get("fmnumber"))
        if not encoding:
            raise BusinessDataRefreshError("material 源记录缺少 fmnumber")
        item = {
            "encoding": encoding,
            "name": clean_optional(row.get("fmname")),
            "specification_model": clean_optional(row.get("fpmodel")),
            "material_group": clean_optional(row.get("fcategoryname")),
            "product_major_category_report": clean_optional(row.get("fpclass")),
        }
        candidate = (precedence[org_code], item)
        existing = selected.get(encoding)
        if existing is None or candidate[0] < existing[0]:
            selected[encoding] = candidate
        elif candidate[0] == existing[0] and candidate[1] != existing[1]:
            raise BusinessDataRefreshError(f"material 同组织业务键重复且内容冲突：{encoding}")
    if not selected:
        raise BusinessDataRefreshError("material 外部源返回空快照")
    result = {code: item for code, (_, item) in selected.items()}
    result["PD99"] = {
        "encoding": "PD99",
        "name": "不分产品",
        "specification_model": None,
        "material_group": None,
        "product_major_category_report": None,
    }
    return [result[code] for code in sorted(result)]


def normalize_rd_project_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized = _deduplicate_rows(
        ({"proj_code": row.get("fnumber"), "proj_name": row.get("fname")} for row in rows),
        key="proj_code",
        dataset="rd_project",
    )
    by_code = {row["proj_code"]: row for row in normalized}
    by_code["无"] = {"proj_code": "无", "proj_name": "公共部门"}
    return [by_code[code] for code in sorted(by_code)]


def supplier_is_active(document_status: str, disabled_status: str) -> bool:
    return (
        clean_text(document_status) in KINGDEE_VALID_DOCUMENT_STATUSES
        and clean_text(disabled_status) == KINGDEE_ENABLED_STATUS
    )


def normalize_supplier_row(row: Mapping[str, Any]) -> SupplierRecord:
    supplier_code = clean_text(row.get("FNumber"))
    supplier_name = clean_text(row.get("FName"))
    use_org_code = clean_text(row.get("FUseOrgId.FNumber"))
    if not supplier_code or not supplier_name:
        raise BusinessDataRefreshError("金蝶供应商记录缺少编码或名称")
    if use_org_code != KINGDEE_USE_ORG_CODE:
        raise BusinessDataRefreshError(f"金蝶供应商 {supplier_code} 使用组织为 {use_org_code or '空'}，预期1000")
    try:
        source_supplier_id = int(row.get("FSupplierId"))
    except (TypeError, ValueError) as exc:
        raise BusinessDataRefreshError(f"金蝶供应商 {supplier_code} 缺少有效内码") from exc
    document_status = clean_text(row.get("FDocumentStatus"))
    disabled_status = clean_text(row.get("FForbidStatus"))
    if document_status not in {"A", "B", "C", "D", "Z"}:
        raise BusinessDataRefreshError(f"金蝶供应商 {supplier_code} 返回未知单据状态")
    if disabled_status not in {"A", "B"}:
        raise BusinessDataRefreshError(f"金蝶供应商 {supplier_code} 返回未知禁用状态")
    return SupplierRecord(
        supplier_code=supplier_code,
        supplier_name=supplier_name,
        source_supplier_id=source_supplier_id,
        create_org_code=clean_text(row.get("FCreateOrgId.FNumber")),
        create_org_name=clean_text(row.get("FCreateOrgId.FName")),
        use_org_code=use_org_code,
        use_org_name=clean_text(row.get("FUseOrgId.FName")),
        document_status=document_status,
        disabled_status=disabled_status,
        source_present=True,
        is_active=supplier_is_active(document_status, disabled_status),
    )


def normalize_supplier_rows(rows: Iterable[Mapping[str, Any]]) -> List[SupplierRecord]:
    records: Dict[str, SupplierRecord] = {}
    source_ids: Dict[int, str] = {}
    for row in rows:
        record = normalize_supplier_row(row)
        existing = records.get(record.supplier_code)
        if existing is not None and existing != record:
            raise BusinessDataRefreshError(f"股份主体供应商编码重复且内容冲突：{record.supplier_code}")
        existing_code = source_ids.get(record.source_supplier_id)
        if existing_code is not None and existing_code != record.supplier_code:
            raise BusinessDataRefreshError(f"金蝶供应商内码 {record.source_supplier_id} 对应多个编码")
        records[record.supplier_code] = record
        source_ids[record.source_supplier_id] = record.supplier_code
    if not records:
        raise BusinessDataRefreshError("金蝶股份主体供应商接口返回空快照")
    return [records[code] for code in sorted(records)]


def _connect_finance():
    database_url = clean_text(os.environ.get(FINANCE_DATABASE_URL_ENV)) or clean_text(
        os.environ.get("KINGDEE_VOUCHER_DATABASE_URL")
    )
    if database_url:
        normalized = database_url.replace("postgresql+asyncpg://", "postgresql://").replace(
            "postgresql+psycopg://", "postgresql://"
        )
        connection = psycopg2.connect(normalized)
        connection.autocommit = False
        return connection
    connection, cursor = connect_to_db()
    if cursor is not None:
        cursor.close()
    if connection is None:
        raise BusinessDataRefreshError("无法连接财务数据库")
    connection.autocommit = False
    return connection


def _fetch_sqlserver(query: str) -> List[Dict[str, Any]]:
    try:
        import pyodbc
    except ImportError as exc:
        raise BusinessDataRefreshError("Prefect Worker 未安装 pyodbc") from exc
    connection = pyodbc.connect(_required_env(SQLSERVER_CONNECTION_ENV))
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        columns = [str(item[0]).lower() for item in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()


def _fetch_oracle_table(table: str, columns: Sequence[str]) -> List[Tuple[Any, ...]]:
    try:
        import oracledb
    except ImportError as exc:
        raise BusinessDataRefreshError("Prefect Worker 未安装 oracledb") from exc
    connection = oracledb.connect(
        user=_required_env(ORACLE_USER_ENV),
        password=_required_env(ORACLE_PASSWORD_ENV),
        dsn=_required_env(ORACLE_DSN_ENV),
    )
    try:
        cursor = connection.cursor()
        column_sql = ", ".join(column.upper() for column in columns)
        cursor.execute(f"SELECT {column_sql} FROM {table.upper()}")
        rows: List[Tuple[Any, ...]] = []
        while True:
            batch = cursor.fetchmany(10000)
            if not batch:
                return rows
            rows.extend(tuple(clean_optional(value) for value in row) for row in batch)
    finally:
        connection.close()


def _replace_snapshot(
    dataset: str,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    connection = _connect_finance()
    staging = f"stg_business_data_{uuid.uuid4().hex}"
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))", (f"business-data:{dataset}",)
            )
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
            previous_rows = int(cursor.fetchone()[0] or 0)
            validate_snapshot_count(dataset, len(rows), previous_rows)
            column_ids = [sql.Identifier(column) for column in columns]
            cursor.execute(
                sql.SQL("CREATE TEMP TABLE {} AS SELECT {} FROM {} WITH NO DATA").format(
                    sql.Identifier(staging),
                    sql.SQL(", ").join(column_ids),
                    sql.Identifier(table),
                )
            )
            values = [tuple(row.get(column) for column in columns) for row in rows]
            execute_values(
                cursor,
                sql.SQL("INSERT INTO {} ({}) VALUES %s")
                .format(sql.Identifier(staging), sql.SQL(", ").join(column_ids))
                .as_string(connection),
                values,
                page_size=5000,
            )
            cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
            cursor.execute(
                sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(column_ids),
                    sql.SQL(", ").join(column_ids),
                    sql.Identifier(staging),
                )
            )
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
            target_rows = int(cursor.fetchone()[0] or 0)
            if target_rows != len(rows):
                raise BusinessDataRefreshError(
                    f"{dataset} 写入后行数 {target_rows} 与源快照 {len(rows)} 不一致"
                )
        connection.commit()
        return {
            "dataset": dataset,
            "source_rows": len(rows),
            "target_rows": target_rows,
            "watermark": None,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _kingdee_token() -> str:
    for name in KINGDEE_TOKEN_ENVS:
        token = clean_text(os.environ.get(name))
        if token:
            return token
    raise BusinessDataRefreshError("必须配置 XGD_TOKEN 或 AIHUB_FONE_API_TOKEN")


def _kingdee_error(data: Any) -> str:
    if not isinstance(data, dict):
        return "未知错误"
    status = (data.get("Result") or {}).get("ResponseStatus") or {}
    messages = [
        clean_text(item.get("Message"))
        for item in status.get("Errors") or []
        if isinstance(item, dict) and clean_text(item.get("Message"))
    ]
    return "; ".join(messages) or "未返回错误详情"


def fetch_supplier_rows(
    session: requests.Session,
    *,
    page_size: int = 5000,
    timeout_seconds: float = 60,
    max_retries: int = 3,
) -> List[Dict[str, Any]]:
    endpoint = (
        f"{os.environ.get(KINGDEE_BASE_URL_ENV, 'https://aihub.xgd.com').rstrip('/')}"
        "/api/proxy/erp/sdk/BillQuery"
    )
    token = _kingdee_token()
    rows: List[Dict[str, Any]] = []
    start_row = 0
    while True:
        payload = {
            "FormId": KINGDEE_FORM_ID,
            "FieldKeys": ",".join(KINGDEE_FIELD_KEYS),
            "FilterString": f"FUseOrgId.FNumber = '{KINGDEE_USE_ORG_CODE}'",
            "OrderString": "FSupplierId ASC",
            "StartRow": start_row,
            "Limit": page_size,
        }
        page: Any = None
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
                page = response.json()
                break
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt < max_retries:
                    time.sleep(min(2**attempt, 8))
                    continue
                raise BusinessDataRefreshError("金蝶供应商接口网络请求失败") from exc
            except (requests.HTTPError, ValueError) as exc:
                raise BusinessDataRefreshError("金蝶供应商接口返回无效响应") from exc
        if not isinstance(page, list):
            raise BusinessDataRefreshError(f"金蝶供应商接口查询失败：{_kingdee_error(page)}")
        if not all(isinstance(row, dict) for row in page):
            raise BusinessDataRefreshError("金蝶供应商接口返回了非对象数据行")
        if not page:
            return rows
        rows.extend(page)
        start_row += len(page)


def _sync_supplier_records(records: Sequence[SupplierRecord]) -> Dict[str, Any]:
    connection = _connect_finance()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))", ("business-data:supplier",)
            )
            cursor.execute("SELECT COUNT(*) FROM dim_supplier_info WHERE source_present")
            previous_rows = int(cursor.fetchone()[0] or 0)
            validate_snapshot_count("supplier", len(records), previous_rows)
            cursor.execute(
                """
                UPDATE dim_supplier_info
                SET source_present = FALSE, is_active = FALSE, synced_at = NOW()
                WHERE source_present OR is_active
                """
            )
            values = [asdict(record) for record in records]
            execute_values(
                cursor,
                """
                INSERT INTO dim_supplier_info (
                    supplier_code, supplier_name, source_supplier_id,
                    create_org_code, create_org_name, use_org_code, use_org_name,
                    document_status, disabled_status, source_present, is_active, synced_at
                ) VALUES %s
                ON CONFLICT (supplier_code) DO UPDATE SET
                    supplier_name = EXCLUDED.supplier_name,
                    source_supplier_id = EXCLUDED.source_supplier_id,
                    create_org_code = EXCLUDED.create_org_code,
                    create_org_name = EXCLUDED.create_org_name,
                    use_org_code = EXCLUDED.use_org_code,
                    use_org_name = EXCLUDED.use_org_name,
                    document_status = EXCLUDED.document_status,
                    disabled_status = EXCLUDED.disabled_status,
                    source_present = EXCLUDED.source_present,
                    is_active = EXCLUDED.is_active,
                    synced_at = NOW()
                """,
                [
                    (
                        item["supplier_code"],
                        item["supplier_name"],
                        item["source_supplier_id"],
                        item["create_org_code"],
                        item["create_org_name"],
                        item["use_org_code"],
                        item["use_org_name"],
                        item["document_status"],
                        item["disabled_status"],
                        item["source_present"],
                        item["is_active"],
                        datetime.now(),
                    )
                    for item in values
                ],
                page_size=5000,
            )
            cursor.execute(
                """
                WITH fact_codes AS (
                    SELECT DISTINCT btrim(supplier_code) AS supplier_code
                    FROM fact_inventory_on_way
                    WHERE nullif(btrim(supplier_code), '') IS NOT NULL
                )
                SELECT fact.supplier_code
                FROM fact_codes AS fact
                LEFT JOIN dim_supplier_info AS supplier
                  ON supplier.supplier_code = fact.supplier_code
                 AND supplier.source_present AND supplier.is_active
                WHERE supplier.supplier_code IS NULL
                ORDER BY fact.supplier_code
                """
            )
            missing = [str(row[0]) for row in cursor.fetchall()]
            if missing:
                preview = ", ".join(missing[:20])
                raise BusinessDataRefreshError(f"供应商主数据不能覆盖 {len(missing)} 个在途供应商编码：{preview}")
            cursor.execute(
                """
                SELECT COUNT(*) FILTER (WHERE source_present),
                       COUNT(*) FILTER (WHERE source_present AND is_active)
                FROM dim_supplier_info
                """
            )
            source_present_rows, active_rows = cursor.fetchone()
        connection.commit()
        return {
            "dataset": "supplier",
            "source_rows": len(records),
            "target_rows": int(source_present_rows or 0),
            "active_rows": int(active_rows or 0),
            "watermark": datetime.now().isoformat(),
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _normalize_acquiring_value(value: Any) -> Any:
    if isinstance(value, (date, datetime, Decimal, int, float)) or value is None:
        return value
    return clean_optional(value)


def validate_acquiring_rows(table: str, rows: Sequence[Tuple[Any, ...]]) -> None:
    if not rows:
        raise BusinessDataRefreshError(f"{table} 外部源返回空快照")
    for row in rows:
        if len(row) != len(ACQUIRING_TABLE_COLUMNS[table]):
            raise BusinessDataRefreshError(f"{table} 返回列数与目标表不一致")


def _replace_acquiring_snapshots(
    snapshots: Mapping[str, Sequence[Tuple[Any, ...]]],
) -> Dict[str, Any]:
    connection = _connect_finance()
    staging_tables: Dict[str, str] = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("business-data:acquiring_metrics",),
            )
            previous_counts: Dict[str, int] = {}
            for table in ACQUIRING_TABLE_COLUMNS:
                cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
                previous_counts[table] = int(cursor.fetchone()[0] or 0)
                validate_snapshot_count(
                    "acquiring_metrics", len(snapshots[table]), previous_counts[table]
                )
                staging = f"stg_business_data_{uuid.uuid4().hex}"
                staging_tables[table] = staging
                columns = [sql.Identifier(column) for column in ACQUIRING_TABLE_COLUMNS[table]]
                cursor.execute(
                    sql.SQL("CREATE TEMP TABLE {} AS SELECT {} FROM {} WITH NO DATA").format(
                        sql.Identifier(staging),
                        sql.SQL(", ").join(columns),
                        sql.Identifier(table),
                    )
                )
                execute_values(
                    cursor,
                    sql.SQL("INSERT INTO {} ({}) VALUES %s")
                    .format(sql.Identifier(staging), sql.SQL(", ").join(columns))
                    .as_string(connection),
                    snapshots[table],
                    page_size=5000,
                )
            for table, staging in staging_tables.items():
                columns = [sql.Identifier(column) for column in ACQUIRING_TABLE_COLUMNS[table]]
                cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                cursor.execute(
                    sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(columns),
                        sql.SQL(", ").join(columns),
                        sql.Identifier(staging),
                    )
                )
            target_counts: Dict[str, int] = {}
            for table, rows in snapshots.items():
                cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
                target_counts[table] = int(cursor.fetchone()[0] or 0)
                if target_counts[table] != len(rows):
                    raise BusinessDataRefreshError(f"{table} 写入后行数不一致")
        connection.commit()
        watermarks = [
            clean_text(row[0])
            for rows in snapshots.values()
            for row in rows
            if row and clean_text(row[0])
        ]
        return {
            "dataset": "acquiring_metrics",
            "source_rows": sum(len(rows) for rows in snapshots.values()),
            "target_rows": sum(target_counts.values()),
            "watermark": max(watermarks) if watermarks else None,
            "tables": target_counts,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


@task(name="更新客户主数据", retries=1, retry_delay_seconds=30)
def refresh_customer_task() -> Dict[str, Any]:
    logger = get_run_logger()
    rows = _fetch_sqlserver(
        """
        SELECT fnumber, fname, FCOUNTRY, FPROVINCE, FCITY, FPROVINCIAL
        FROM V_XGD_BD_CUSTOMER
        WHERE forgid = '1000'
        """
    )
    normalized = normalize_customer_rows(rows)
    result = _replace_snapshot(
        "customer",
        "dim_customer_info",
        ("cust_code", "cust_name", "country", "province", "city", "district"),
        normalized,
    )
    logger.info("客户主数据更新完成：%s 行", result["target_rows"])
    return result


@task(name="更新物料主数据", retries=1, retry_delay_seconds=30)
def refresh_material_task() -> Dict[str, Any]:
    logger = get_run_logger()
    rows = _fetch_sqlserver(
        """
        SELECT forgid, fmnumber, fmname, fpmodel, FCATEGORYNAME, fpclass
        FROM V_XGD_BD_MATERIAL
        WHERE forgid IN ('1000', '1700', '1200')
        """
    )
    normalized = normalize_material_rows(rows)
    result = _replace_snapshot(
        "material",
        "dim_material_master",
        (
            "encoding",
            "name",
            "specification_model",
            "material_group",
            "product_major_category_report",
        ),
        normalized,
    )
    logger.info("物料主数据更新完成：%s 行", result["target_rows"])
    return result


@task(name="更新研发项目主数据", retries=1, retry_delay_seconds=30)
def refresh_rd_project_task() -> Dict[str, Any]:
    logger = get_run_logger()
    rows = _fetch_sqlserver("SELECT fnumber, fname FROM V_XGD_BD_YFPROJ")
    normalized = normalize_rd_project_rows(rows)
    result = _replace_snapshot("rd_project", "dim_rd_code", ("proj_code", "proj_name"), normalized)
    logger.info("研发项目主数据更新完成：%s 行", result["target_rows"])
    return result


@task(name="更新供应商主数据", retries=1, retry_delay_seconds=30)
def refresh_supplier_task() -> Dict[str, Any]:
    logger = get_run_logger()
    with requests.Session() as session:
        rows = fetch_supplier_rows(session)
    result = _sync_supplier_records(normalize_supplier_rows(rows))
    logger.info(
        "供应商主数据更新完成：%s 行，其中有效 %s 行",
        result["target_rows"],
        result["active_rows"],
    )
    return result


@task(name="更新收单业务指标", retries=1, retry_delay_seconds=30)
def refresh_acquiring_metrics_task() -> Dict[str, Any]:
    logger = get_run_logger()
    snapshots: Dict[str, List[Tuple[Any, ...]]] = {}
    for table, columns in ACQUIRING_TABLE_COLUMNS.items():
        rows = _fetch_oracle_table(table, columns)
        snapshots[table] = [
            tuple(_normalize_acquiring_value(value) for value in row) for row in rows
        ]
        validate_acquiring_rows(table, snapshots[table])
    result = _replace_acquiring_snapshots(snapshots)
    logger.info("收单业务指标更新完成：%s 行", result["target_rows"])
    return result
