"""Batch lifecycle helpers for monthly business-line staging data."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from datetime import date, datetime

from mypackage.utilities import connect_to_db

STAGING_TABLE_DATE_COLUMNS = {
    "staging_bus_expense": "会计期间",
    "staging_bus_revenue": "会计期间",
    "staging_bus_profit_bd": "日期",
    "staging_bus_inventory": "会计期间",
    "staging_bus_receivable": "会计期间",
    "staging_bus_in_transit_inventory": "会计期间",
}

STAGING_TABLE_CLASSES = {
    "staging_bus_expense": "费用",
    "staging_bus_revenue": "收入",
    "staging_bus_profit_bd": "其他",
    "staging_bus_inventory": "存货",
    "staging_bus_receivable": "应收",
    "staging_bus_in_transit_inventory": "在途存货",
}


def _month_start(value: date) -> date:
    return value.replace(day=1)


def accounting_periods(date_range: Iterable[date]) -> list[date]:
    """Return sorted, distinct accounting-month starts for a requested date range."""
    return sorted({_month_start(value) for value in date_range})


def _single_accounting_period(date_range: Iterable[date]) -> date:
    periods = accounting_periods(date_range)
    if not periods:
        raise ValueError("Cannot create a staging batch for an empty date range")
    if len(periods) != 1:
        raise ValueError("A business-line staging batch must contain exactly one accounting month")
    return periods[0]


def _validate_table_name(table_name: str) -> None:
    if table_name not in STAGING_TABLE_DATE_COLUMNS:
        raise ValueError(f"Unsupported staging table: {table_name}")


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _batch_no(acct_period: date, version_no: int) -> str:
    return f"BLS-{acct_period:%Y%m}-{version_no:03d}"


def ensure_batch_schema(cur) -> None:
    """Create the single monthly batch-management table when it does not exist."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bus_line_staging_batch (
            batch_id UUID PRIMARY KEY,
            batch_no VARCHAR(30) NOT NULL UNIQUE,
            acct_period DATE NOT NULL,
            version_no INTEGER NOT NULL,
            previous_batch_id UUID REFERENCES bus_line_staging_batch(batch_id),
            status VARCHAR(20) NOT NULL,
            flow_run_id VARCHAR(100),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ready_at TIMESTAMP,
            activated_at TIMESTAMP,
            published_at TIMESTAMP,
            failed_at TIMESTAMP,
            error_message TEXT,
            UNIQUE (acct_period, version_no)
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_bus_line_staging_batch_flow_run
        ON bus_line_staging_batch(flow_run_id)
        WHERE flow_run_id IS NOT NULL
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_bus_line_staging_batch_filling_period
        ON bus_line_staging_batch(acct_period)
        WHERE status = 'FILLING'
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_bus_line_staging_batch_published_period
        ON bus_line_staging_batch(acct_period)
        WHERE status = 'PUBLISHED'
        """
    )


def ensure_staging_table_batch_support(cur, table_name: str) -> None:
    """Add batch support and current-batch views to an existing staging table."""
    _validate_table_name(table_name)
    date_column = STAGING_TABLE_DATE_COLUMNS[table_name]
    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS batch_id UUID")
    cur.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_batch_source_lvl
        ON {table_name}(batch_id, "来源编号", "唯一层级")
        WHERE batch_id IS NOT NULL
        """
    )
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_batch_period
        ON {table_name}(batch_id, "{date_column}")
        """
    )
    cur.execute(
        f"""
        CREATE OR REPLACE VIEW vw_{table_name}_current_edit AS
        SELECT staging.*
        FROM {table_name} AS staging
        JOIN bus_line_staging_batch AS batch
          ON batch.batch_id = staging.batch_id
         AND batch.acct_period = staging."{date_column}"
         AND batch.status = 'FILLING'
        """
    )
    cur.execute(
        f"""
        CREATE OR REPLACE VIEW vw_{table_name}_current_published AS
        SELECT staging.*
        FROM {table_name} AS staging
        JOIN bus_line_staging_batch AS batch
          ON batch.batch_id = staging.batch_id
         AND batch.acct_period = staging."{date_column}"
         AND batch.status = 'PUBLISHED'
        """
    )


def _legacy_batch_status(cur, acct_period: date) -> str:
    """Infer whether existing staging rows were already finalized into fact_bus_line."""
    cur.execute("SELECT to_regclass('fact_bus_line')")
    if cur.fetchone()[0] is None:
        return "FILLING"
    cur.execute(
        "SELECT 1 FROM fact_bus_line WHERE acct_period = %s LIMIT 1",
        (acct_period,),
    )
    return "PUBLISHED" if cur.fetchone() else "FILLING"


def _bootstrap_legacy_period(cur, acct_period: date) -> str | None:
    """Attach unversioned rows to a human-readable version-zero baseline batch."""
    cur.execute(
        """
        SELECT batch_id
        FROM bus_line_staging_batch
        WHERE acct_period = %s
          AND status IN ('FILLING', 'PUBLISHED')
        ORDER BY CASE status WHEN 'FILLING' THEN 0 ELSE 1 END, version_no DESC
        LIMIT 1
        """,
        (acct_period,),
    )
    existing = cur.fetchone()
    if existing:
        return str(existing[0])

    legacy_tables = []
    for table_name, date_column in STAGING_TABLE_DATE_COLUMNS.items():
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        if cur.fetchone()[0] is None:
            continue
        ensure_staging_table_batch_support(cur, table_name)
        cur.execute(
            f'SELECT 1 FROM {table_name} WHERE "{date_column}" = %s AND batch_id IS NULL LIMIT 1',
            (acct_period,),
        )
        if cur.fetchone():
            legacy_tables.append((table_name, date_column))

    if not legacy_tables:
        return None

    batch_id = str(uuid.uuid4())
    status = _legacy_batch_status(cur, acct_period)
    cur.execute(
        """
        INSERT INTO bus_line_staging_batch(
            batch_id, batch_no, acct_period, version_no, status, flow_run_id,
            ready_at, activated_at, published_at
        )
        VALUES (
            %s, %s, %s, 0, %s, %s, CURRENT_TIMESTAMP,
            CASE WHEN %s = 'FILLING' THEN CURRENT_TIMESTAMP END,
            CASE WHEN %s = 'PUBLISHED' THEN CURRENT_TIMESTAMP END
        )
        """,
        (
            batch_id,
            _batch_no(acct_period, 0),
            acct_period,
            status,
            f"legacy:{acct_period.isoformat()}",
            status,
            status,
        ),
    )
    for table_name, date_column in legacy_tables:
        cur.execute(
            f'UPDATE {table_name} SET batch_id = %s WHERE "{date_column}" = %s AND batch_id IS NULL',
            (batch_id, acct_period),
        )
    return batch_id


def start_batch(date_range: Iterable[date], flow_run_id: str | None = None) -> str:
    """Create a generating batch for exactly one accounting month."""
    dates = list(date_range)
    acct_period = _single_accounting_period(dates)
    batch_id = str(uuid.uuid4())
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        if flow_run_id:
            cur.execute(
                "SELECT batch_id, status FROM bus_line_staging_batch WHERE flow_run_id = %s",
                (flow_run_id,),
            )
            existing = cur.fetchone()
            if existing:
                conn.commit()
                if existing[1] == "GENERATING":
                    return str(existing[0])
                raise ValueError(
                    f"Flow run {flow_run_id} already owns batch {existing[0]} "
                    f"with status {existing[1]}"
                )

        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"bus-line:{acct_period}",))
        _bootstrap_legacy_period(cur, acct_period)
        cur.execute(
            """
            SELECT batch_id
            FROM bus_line_staging_batch
            WHERE acct_period = %s
              AND status IN ('FILLING', 'PUBLISHED', 'SUPERSEDED', 'READY')
            ORDER BY
                CASE status
                    WHEN 'FILLING' THEN 0
                    WHEN 'PUBLISHED' THEN 1
                    WHEN 'SUPERSEDED' THEN 2
                    ELSE 3
                END,
                version_no DESC
            LIMIT 1
            """,
            (acct_period,),
        )
        previous = cur.fetchone()
        previous_batch_id = previous[0] if previous else None
        cur.execute(
            "SELECT COALESCE(MAX(version_no), -1) + 1 FROM bus_line_staging_batch WHERE acct_period = %s",
            (acct_period,),
        )
        version_no = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO bus_line_staging_batch(
                batch_id, batch_no, acct_period, version_no,
                previous_batch_id, status, flow_run_id
            )
            VALUES (%s, %s, %s, %s, %s, 'GENERATING', %s)
            """,
            (
                batch_id,
                _batch_no(acct_period, version_no),
                acct_period,
                version_no,
                previous_batch_id,
                flow_run_id,
            ),
        )
        conn.commit()
        return batch_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def inherit_previous_values(batch_id: str, bus_lines: Iterable[str]) -> dict[str, int]:
    """Copy ratios and audit status from this month's preceding batch."""
    conn, cur = connect_to_db()
    inherited: dict[str, int] = {}
    try:
        ensure_batch_schema(cur)
        cur.execute(
            """
            SELECT acct_period, previous_batch_id
            FROM bus_line_staging_batch
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        batch_row = cur.fetchone()
        if not batch_row:
            raise ValueError(f"Unknown batch: {batch_id}")
        acct_period, previous_batch_id = batch_row
        for table_name, date_column in STAGING_TABLE_DATE_COLUMNS.items():
            cur.execute("SELECT to_regclass(%s)", (table_name,))
            if cur.fetchone()[0] is None or previous_batch_id is None:
                inherited[table_name] = 0
                continue
            class_name = STAGING_TABLE_CLASSES[table_name]
            cur.execute(
                f"""
                UPDATE {table_name} AS new_row
                SET "审核状态" = previous."审核状态"
                FROM {table_name} AS previous
                WHERE new_row.batch_id = %s
                  AND previous.batch_id = %s
                  AND previous."{date_column}" = new_row."{date_column}"
                  AND previous."来源编号" = new_row."来源编号"
                  AND previous."唯一层级" = new_row."唯一层级"
                  AND NOT EXISTS (
                      SELECT 1
                      FROM staging_bus_line_ratio AS current_ratio
                      WHERE current_ratio.class = %s
                        AND current_ratio.record_id = new_row.record_id
                  )
                """,
                (batch_id, previous_batch_id, class_name),
            )
            inherited[table_name] = cur.rowcount
            cur.execute(
                f"""
                INSERT INTO staging_bus_line_ratio(
                    class, record_id, bus_line, rate, created_at, updated_at, updated_by
                )
                SELECT %s, new_row.record_id, ratio.bus_line, ratio.rate,
                       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ratio.updated_by
                FROM {table_name} AS new_row
                JOIN {table_name} AS previous
                  ON previous.batch_id = %s
                 AND previous."{date_column}" = new_row."{date_column}"
                 AND previous."来源编号" = new_row."来源编号"
                 AND previous."唯一层级" = new_row."唯一层级"
                JOIN staging_bus_line_ratio AS ratio
                  ON ratio.class = %s
                 AND ratio.record_id = previous.record_id
                WHERE new_row.batch_id = %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM staging_bus_line_ratio AS current_ratio
                      WHERE current_ratio.class = %s
                        AND current_ratio.record_id = new_row.record_id
                  )
                ON CONFLICT (class, record_id, bus_line) DO UPDATE
                SET rate = EXCLUDED.rate,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (class_name, previous_batch_id, class_name, batch_id, class_name),
            )
        conn.commit()
        return inherited
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def compare_batch_to_previous(
    batch_id: str,
    bus_lines: Iterable[str],
    sample_limit: int = 10,
) -> dict[str, object]:
    """Compare one batch with its predecessor without comparing filled ratios.

    Records are matched by ``来源编号 + 唯一层级``. Source-data fields are compared after
    excluding row identity, batch identity, fill ratios, audit state, and creation time.
    """
    if sample_limit < 0 or sample_limit > 100:
        raise ValueError("sample_limit must be between 0 and 100")

    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        cur.execute(
            """
            SELECT current_batch.batch_id, current_batch.batch_no,
                   current_batch.acct_period, current_batch.previous_batch_id,
                   previous_batch.batch_no
            FROM bus_line_staging_batch AS current_batch
            LEFT JOIN bus_line_staging_batch AS previous_batch
              ON previous_batch.batch_id = current_batch.previous_batch_id
            WHERE current_batch.batch_id = %s
            """,
            (batch_id,),
        )
        batch_row = cur.fetchone()
        if not batch_row:
            raise ValueError(f"Unknown batch: {batch_id}")

        (
            current_batch_id,
            batch_no,
            acct_period,
            previous_batch_id,
            previous_batch_no,
        ) = batch_row
        table_results: dict[str, dict[str, object]] = {}
        totals = {
            "old_records": 0,
            "new_records": 0,
            "added": 0,
            "removed": 0,
            "source_changed": 0,
            "unchanged": 0,
        }

        for table_name in STAGING_TABLE_DATE_COLUMNS:
            cur.execute("SELECT to_regclass(%s)", (table_name,))
            if cur.fetchone()[0] is None:
                continue

            excluded_columns = [
                "id",
                "batch_id",
                "record_id",
                "审核状态",
                "创建时间",
                *list(bus_lines),
            ]
            cur.execute(
                f"""
                WITH old_rows AS (
                    SELECT * FROM {table_name} WHERE batch_id = %s
                ),
                new_rows AS (
                    SELECT * FROM {table_name} WHERE batch_id = %s
                ),
                paired AS (
                    SELECT
                        COALESCE(new_row."来源编号", old_row."来源编号") AS source_no,
                        COALESCE(new_row."唯一层级", old_row."唯一层级") AS unique_lvl,
                        to_jsonb(old_row) - %s::text[] AS old_payload,
                        to_jsonb(new_row) - %s::text[] AS new_payload,
                        CASE
                            WHEN old_row.id IS NULL THEN 'ADDED'
                            WHEN new_row.id IS NULL THEN 'REMOVED'
                        END AS row_presence
                    FROM old_rows AS old_row
                    FULL OUTER JOIN new_rows AS new_row
                      ON new_row."来源编号" = old_row."来源编号"
                     AND new_row."唯一层级" = old_row."唯一层级"
                ),
                compared AS (
                    SELECT
                        source_no,
                        unique_lvl,
                        old_payload,
                        new_payload,
                        CASE
                            WHEN row_presence IS NOT NULL THEN row_presence
                            WHEN new_payload IS DISTINCT FROM old_payload THEN 'SOURCE_CHANGED'
                            ELSE 'UNCHANGED'
                        END AS change_type
                    FROM paired
                ),
                ranked AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY change_type ORDER BY source_no, unique_lvl
                    ) AS change_rank
                    FROM compared
                )
                SELECT
                    COUNT(*) FILTER (WHERE change_type = 'ADDED') AS added,
                    COUNT(*) FILTER (WHERE change_type = 'REMOVED') AS removed,
                    COUNT(*) FILTER (WHERE change_type = 'SOURCE_CHANGED') AS source_changed,
                    COUNT(*) FILTER (WHERE change_type = 'UNCHANGED') AS unchanged,
                    COALESCE(
                        JSONB_AGG(
                            JSONB_BUILD_OBJECT(
                                'change_type', change_type,
                                'source_no', source_no,
                                'unique_lvl', unique_lvl,
                                'changed_fields',
                                CASE
                                    WHEN change_type = 'SOURCE_CHANGED' THEN (
                                        SELECT COALESCE(
                                            JSONB_AGG(field_name ORDER BY field_name),
                                            '[]'::jsonb
                                        )
                                        FROM (
                                            SELECT JSONB_OBJECT_KEYS(
                                                COALESCE(old_payload, '{{}}'::jsonb)
                                            ) AS field_name
                                            UNION
                                            SELECT JSONB_OBJECT_KEYS(
                                                COALESCE(new_payload, '{{}}'::jsonb)
                                            ) AS field_name
                                        ) AS payload_fields
                                        WHERE old_payload -> field_name
                                              IS DISTINCT FROM new_payload -> field_name
                                    )
                                    ELSE '[]'::jsonb
                                END
                            ) ORDER BY change_type, source_no, unique_lvl
                        ) FILTER (
                            WHERE change_type <> 'UNCHANGED' AND change_rank <= %s
                        ),
                        '[]'::jsonb
                    ) AS samples
                FROM ranked
                """,
                (
                    previous_batch_id,
                    current_batch_id,
                    excluded_columns,
                    excluded_columns,
                    sample_limit,
                ),
            )
            added, removed, source_changed, unchanged, samples = cur.fetchone()
            added = int(added or 0)
            removed = int(removed or 0)
            source_changed = int(source_changed or 0)
            unchanged = int(unchanged or 0)
            table_result = {
                "old_records": removed + source_changed + unchanged,
                "new_records": added + source_changed + unchanged,
                "added": added,
                "removed": removed,
                "source_changed": source_changed,
                "unchanged": unchanged,
                "samples": samples or [],
            }
            table_results[table_name] = table_result
            for key in totals:
                totals[key] += int(table_result[key])

        conn.commit()
        return {
            "batch_id": str(current_batch_id),
            "batch_no": batch_no,
            "acct_period": acct_period.isoformat(),
            "previous_batch_id": (
                str(previous_batch_id) if previous_batch_id is not None else None
            ),
            "previous_batch_no": previous_batch_no,
            "totals": totals,
            "tables": table_results,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def complete_batch(batch_id: str) -> str:
    """Mark a generated batch ready or make it the default editable batch."""
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        cur.execute(
            "SELECT acct_period, status FROM bus_line_staging_batch WHERE batch_id = %s FOR UPDATE",
            (batch_id,),
        )
        row = cur.fetchone()
        if not row or row[1] != "GENERATING":
            raise ValueError(f"Batch {batch_id} is not in GENERATING status")
        acct_period = row[0]
        cur.execute(
            """
            SELECT 1 FROM bus_line_staging_batch
            WHERE acct_period = %s AND status = 'FILLING' AND batch_id <> %s
            LIMIT 1
            """,
            (acct_period, batch_id),
        )
        status = "READY" if cur.fetchone() else "FILLING"
        cur.execute(
            """
            UPDATE bus_line_staging_batch
            SET status = %s,
                ready_at = CURRENT_TIMESTAMP,
                activated_at = CASE WHEN %s = 'FILLING' THEN CURRENT_TIMESTAMP END
            WHERE batch_id = %s
            """,
            (status, status, batch_id),
        )
        conn.commit()
        return status
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def activate_batch(batch_id: str) -> None:
    """Make a ready batch the only editable batch for its accounting month."""
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        cur.execute(
            "SELECT acct_period, status FROM bus_line_staging_batch WHERE batch_id = %s FOR UPDATE",
            (batch_id,),
        )
        row = cur.fetchone()
        if not row or row[1] not in {"READY", "FILLING"}:
            raise ValueError(f"Batch {batch_id} must be READY or FILLING before activation")
        acct_period = row[0]
        cur.execute(
            """
            UPDATE bus_line_staging_batch
            SET status = 'SUPERSEDED'
            WHERE acct_period = %s AND status = 'FILLING' AND batch_id <> %s
            """,
            (acct_period, batch_id),
        )
        cur.execute(
            """
            UPDATE bus_line_staging_batch
            SET status = 'FILLING', activated_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def publish_batch(batch_id: str) -> None:
    """Mark a filling batch published after its ratios reach ``fact_bus_line``."""
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        cur.execute(
            "SELECT acct_period, status FROM bus_line_staging_batch WHERE batch_id = %s FOR UPDATE",
            (batch_id,),
        )
        row = cur.fetchone()
        if not row or row[1] != "FILLING":
            raise ValueError(f"Batch {batch_id} must be FILLING before publication")
        acct_period = row[0]
        cur.execute(
            """
            UPDATE bus_line_staging_batch
            SET status = 'SUPERSEDED'
            WHERE acct_period = %s AND status = 'PUBLISHED' AND batch_id <> %s
            """,
            (acct_period, batch_id),
        )
        cur.execute(
            """
            UPDATE bus_line_staging_batch
            SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def fail_batch(batch_id: str, error: Exception | str) -> None:
    """Remove rows from a failed new batch without touching preceding batches."""
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        cur.execute(
            "SELECT status FROM bus_line_staging_batch WHERE batch_id = %s FOR UPDATE",
            (batch_id,),
        )
        row = cur.fetchone()
        if not row or row[0] != "GENERATING":
            conn.commit()
            return
        for table_name in STAGING_TABLE_DATE_COLUMNS:
            cur.execute("SELECT to_regclass(%s)", (table_name,))
            if cur.fetchone()[0] is not None:
                cur.execute(f"DELETE FROM {table_name} WHERE batch_id = %s", (batch_id,))
        cur.execute(
            """
            UPDATE bus_line_staging_batch
            SET status = 'FAILED', failed_at = CURRENT_TIMESTAMP, error_message = %s
            WHERE batch_id = %s
            """,
            (str(error)[:4000], batch_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def get_current_batch(acct_period: date, purpose: str = "edit") -> str | None:
    """Resolve the default editable or published batch for one accounting month."""
    if purpose not in {"edit", "published"}:
        raise ValueError("purpose must be 'edit' or 'published'")
    status = "FILLING" if purpose == "edit" else "PUBLISHED"
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        cur.execute(
            """
            SELECT batch_id
            FROM bus_line_staging_batch
            WHERE acct_period = %s AND status = %s
            """,
            (_month_start(acct_period), status),
        )
        row = cur.fetchone()
        conn.commit()
        return str(row[0]) if row else None
    finally:
        cur.close()
        conn.close()


def batch_summary(batch_id: str) -> dict[str, object]:
    """Return non-sensitive batch metadata for flow results and administrators."""
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        cur.execute(
            """
            SELECT batch_id, batch_no, acct_period, version_no, previous_batch_id,
                   status, created_at, ready_at, activated_at, published_at, failed_at
            FROM bus_line_staging_batch
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            raise ValueError(f"Unknown batch: {batch_id}")
        keys = [
            "batch_id",
            "batch_no",
            "acct_period",
            "version_no",
            "previous_batch_id",
            "status",
            "created_at",
            "ready_at",
            "activated_at",
            "published_at",
            "failed_at",
        ]
        result = dict(zip(keys, row))
        for key in ("batch_id", "previous_batch_id"):
            if result[key] is not None:
                result[key] = str(result[key])
        for key, value in list(result.items()):
            if isinstance(value, (date, datetime)):
                result[key] = value.isoformat()
        return result
    finally:
        cur.close()
        conn.close()


def list_batches(acct_period: date | None = None, limit: int = 50) -> list[dict[str, object]]:
    """List recent monthly batch metadata for administrator tooling."""
    if limit < 1 or limit > 500:
        raise ValueError("limit must be between 1 and 500")
    conn, cur = connect_to_db()
    try:
        ensure_batch_schema(cur)
        params: list[object] = []
        period_filter = ""
        if acct_period is not None:
            period_filter = "WHERE acct_period = %s"
            params.append(_month_start(acct_period))
        params.append(limit)
        cur.execute(
            f"""
            SELECT batch_id, batch_no, acct_period, version_no, status,
                   created_at, ready_at, activated_at, published_at
            FROM bus_line_staging_batch
            {period_filter}
            ORDER BY acct_period DESC, version_no DESC
            LIMIT %s
            """,
            tuple(params),
        )
        keys = [
            "batch_id",
            "batch_no",
            "acct_period",
            "version_no",
            "status",
            "created_at",
            "ready_at",
            "activated_at",
            "published_at",
        ]
        results = []
        for row in cur.fetchall():
            item = dict(zip(keys, row))
            item["batch_id"] = str(item["batch_id"])
            for key, value in list(item.items()):
                if isinstance(value, (date, datetime)):
                    item[key] = value.isoformat()
            results.append(item)
        conn.commit()
        return results
    finally:
        cur.close()
        conn.close()
