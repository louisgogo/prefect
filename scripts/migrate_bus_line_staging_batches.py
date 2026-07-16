"""Migrate all historical business-line staging rows into monthly baseline batches."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from mypackage.utilities import connect_to_db

from modules.bus_line_staging.batch import (
    STAGING_TABLE_DATE_COLUMNS,
    _bootstrap_legacy_period,
    ensure_batch_schema,
    ensure_staging_table_batch_support,
)


@dataclass(frozen=True)
class TableSnapshot:
    row_count: int
    content_checksum: str
    blank_source_count: int
    blank_level_count: int
    duplicate_key_count: int


def _table_snapshot(cur, table_name: str, date_column: str) -> TableSnapshot:
    cur.execute(
        f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (
                   WHERE "来源编号" IS NULL OR BTRIM("来源编号"::text) = ''
               ),
               COUNT(*) FILTER (
                   WHERE "唯一层级" IS NULL OR BTRIM("唯一层级"::text) = ''
               )
        FROM {table_name}
        """
    )
    row_count, blank_source_count, blank_level_count = cur.fetchone()
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT "{date_column}", "来源编号", "唯一层级"
            FROM {table_name}
            GROUP BY "{date_column}", "来源编号", "唯一层级"
            HAVING COUNT(*) > 1
        ) duplicate_keys
        """
    )
    duplicate_key_count = cur.fetchone()[0]
    cur.execute(
        f"""
        SELECT md5(COALESCE(string_agg(row_hash, '' ORDER BY row_id), ''))
        FROM (
            SELECT id AS row_id,
                   md5((to_jsonb(staging_row) - 'batch_id')::text) AS row_hash
            FROM {table_name} AS staging_row
        ) hashed_rows
        """
    )
    content_checksum = cur.fetchone()[0]
    return TableSnapshot(
        row_count=row_count,
        content_checksum=content_checksum,
        blank_source_count=blank_source_count,
        blank_level_count=blank_level_count,
        duplicate_key_count=duplicate_key_count,
    )


def _fact_bus_line_guard(cur) -> tuple:
    cur.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT id), COALESCE(SUM(rate), 0),
               MIN(acct_period), MAX(acct_period)
        FROM fact_bus_line
        """
    )
    return cur.fetchone()


def _discover_periods(cur) -> list[date]:
    statements = [
        f'SELECT DISTINCT "{date_column}" AS acct_period FROM {table_name}'
        for table_name, date_column in STAGING_TABLE_DATE_COLUMNS.items()
    ]
    cur.execute(
        f"SELECT DISTINCT acct_period FROM ({' UNION ALL '.join(statements)}) periods "
        "WHERE acct_period IS NOT NULL ORDER BY acct_period"
    )
    return [row[0] for row in cur.fetchall()]


def _assert_preflight(snapshots: dict[str, TableSnapshot]) -> None:
    failures = []
    for table_name, snapshot in snapshots.items():
        if snapshot.blank_source_count:
            failures.append(f"{table_name}: {snapshot.blank_source_count} blank source numbers")
        if snapshot.blank_level_count:
            failures.append(f"{table_name}: {snapshot.blank_level_count} blank unique levels")
        if snapshot.duplicate_key_count:
            failures.append(f"{table_name}: {snapshot.duplicate_key_count} duplicate logical keys")
    if failures:
        raise ValueError("; ".join(failures))


def _assert_post_migration(
    cur,
    periods: list[date],
    before: dict[str, TableSnapshot],
    fact_guard_before: tuple,
) -> None:
    after = {
        table_name: _table_snapshot(cur, table_name, date_column)
        for table_name, date_column in STAGING_TABLE_DATE_COLUMNS.items()
    }
    for table_name, before_snapshot in before.items():
        if after[table_name] != before_snapshot:
            raise ValueError(
                f"{table_name} changed during migration: "
                f"before={before_snapshot}, after={after[table_name]}"
            )
        cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE batch_id IS NULL")
        null_batch_count = cur.fetchone()[0]
        if null_batch_count:
            raise ValueError(f"{table_name} still has {null_batch_count} rows without batch_id")

    cur.execute(
        """
        SELECT acct_period, COUNT(*), MIN(batch_no), MAX(batch_no),
               MIN(version_no), MAX(version_no)
        FROM bus_line_staging_batch
        WHERE version_no = 0
        GROUP BY acct_period
        ORDER BY acct_period
        """
    )
    baselines = {row[0]: row[1:] for row in cur.fetchall()}
    for acct_period in periods:
        expected_no = f"BLS-{acct_period:%Y%m}-000"
        baseline = baselines.get(acct_period)
        if baseline != (1, expected_no, expected_no, 0, 0):
            raise ValueError(f"Invalid baseline batch for {acct_period}: {baseline}")

    if _fact_bus_line_guard(cur) != fact_guard_before:
        raise ValueError("fact_bus_line changed during staging migration")


def migrate(apply: bool) -> None:
    conn, cur = connect_to_db()
    try:
        cur.execute("SET LOCAL lock_timeout = '10s'")
        before = {
            table_name: _table_snapshot(cur, table_name, date_column)
            for table_name, date_column in STAGING_TABLE_DATE_COLUMNS.items()
        }
        _assert_preflight(before)
        fact_guard_before = _fact_bus_line_guard(cur)
        periods = _discover_periods(cur)
        if not periods:
            raise ValueError("No historical staging periods were found")

        print(f"Historical periods: {len(periods)} ({periods[0]} to {periods[-1]})")
        print(f"Historical staging rows: {sum(item.row_count for item in before.values())}")
        ensure_batch_schema(cur)
        for table_name in STAGING_TABLE_DATE_COLUMNS:
            ensure_staging_table_batch_support(cur, table_name)
        for acct_period in periods:
            batch_id = _bootstrap_legacy_period(cur, acct_period)
            if not batch_id:
                raise ValueError(f"Failed to create or resolve baseline batch for {acct_period}")
        for table_name in STAGING_TABLE_DATE_COLUMNS:
            cur.execute(f"ALTER TABLE {table_name} ALTER COLUMN batch_id SET NOT NULL")

        _assert_post_migration(cur, periods, before, fact_guard_before)
        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM bus_line_staging_batch
            WHERE version_no = 0
            GROUP BY status
            ORDER BY status
            """
        )
        print("Baseline statuses:", dict(cur.fetchall()))
        if apply:
            conn.commit()
            print("Migration committed successfully.")
        else:
            conn.rollback()
            print("Dry-run validation passed; transaction rolled back.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the migration. Without this flag the complete migration is rolled back.",
    )
    args = parser.parse_args()
    migrate(apply=args.apply)


if __name__ == "__main__":
    main()
