"""Backfill expense source hierarchy without changing submitted allocation values."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime

from mypackage.utilities import connect_to_db


@dataclass(frozen=True)
class ValidationSummary:
    staging_rows: int
    missing_source_level: int
    missing_fact_source: int
    ambiguous_fact_source: int
    data_source_mismatch: int
    amount_total: float


def normalize_month(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m").strftime("%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("月份必须使用 YYYY-MM 格式") from exc


def load_validation_summary(cur, month: str) -> ValidationSummary:
    cur.execute(
        """
        WITH fact_source AS (
            SELECT
                source_no,
                COUNT(DISTINCT unique_lvl) AS level_count,
                MIN(unique_lvl) AS source_level
            FROM fact_expense
            WHERE acct_period::text LIKE %s
            GROUP BY source_no
        ), staging AS (
            SELECT *
            FROM staging_bus_expense
            WHERE "会计期间"::text LIKE %s
        )
        SELECT
            COUNT(*) AS staging_rows,
            COUNT(*) FILTER (
                WHERE NULLIF(BTRIM(staging."来源层级"), '') IS NULL
            ) AS missing_source_level,
            COUNT(*) FILTER (WHERE fact_source.source_no IS NULL) AS missing_fact_source,
            COUNT(*) FILTER (WHERE fact_source.level_count <> 1) AS ambiguous_fact_source,
            COUNT(*) FILTER (
                WHERE fact_source.source_level IS DISTINCT FROM staging."数据来源"
            ) AS data_source_mismatch,
            COALESCE(SUM(staging."费用金额"), 0) AS amount_total
        FROM staging
        LEFT JOIN fact_source ON fact_source.source_no = staging."来源编号"
        """,
        (f"{month}%", f"{month}%"),
    )
    row = cur.fetchone()
    return ValidationSummary(
        staging_rows=int(row[0]),
        missing_source_level=int(row[1]),
        missing_fact_source=int(row[2]),
        ambiguous_fact_source=int(row[3]),
        data_source_mismatch=int(row[4]),
        amount_total=float(row[5]),
    )


def validate_backfill(summary: ValidationSummary, month: str) -> None:
    if summary.staging_rows == 0:
        raise ValueError(f"{month} 没有费用 Staging 数据。")
    if summary.missing_fact_source:
        raise ValueError(f"{month} 有 {summary.missing_fact_source} 条 Staging 记录找不到原始费用来源。")
    if summary.ambiguous_fact_source:
        raise ValueError(f"{month} 有 {summary.ambiguous_fact_source} 条记录对应多个原始来源层级。")


def apply_backfill(cur, month: str) -> int:
    cur.execute(
        """
        WITH fact_source AS (
            SELECT source_no, MIN(unique_lvl) AS source_level
            FROM fact_expense
            WHERE acct_period::text LIKE %s
            GROUP BY source_no
            HAVING COUNT(DISTINCT unique_lvl) = 1
        )
        UPDATE staging_bus_expense AS staging
        SET
            "来源层级" = fact_source.source_level,
            "数据来源" = '费用'
        FROM fact_source
        WHERE staging."来源编号" = fact_source.source_no
          AND staging."会计期间"::text LIKE %s
          AND NULLIF(BTRIM(staging."来源层级"), '') IS NULL
        """,
        (f"{month}%", f"{month}%"),
    )
    return int(cur.rowcount)


def main() -> None:
    parser = argparse.ArgumentParser(description="按 fact_expense 原始唯一层级回填费用 Staging 来源层级。默认只预览。")
    parser.add_argument("month", type=normalize_month, help="会计月份，格式 YYYY-MM")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="实际提交回填；不传时只执行校验并回滚。",
    )
    args = parser.parse_args()

    conn, cur = connect_to_db()
    try:
        before = load_validation_summary(cur, args.month)
        validate_backfill(before, args.month)
        print(
            f"{args.month}: Staging {before.staging_rows} 行，待回填 {before.missing_source_level} 行，"
            f"数据来源与原始层级不同 {before.data_source_mismatch} 行，"
            f"费用金额合计 {before.amount_total:.2f}。"
        )

        if not args.apply:
            conn.rollback()
            print("预览完成，未修改数据库。使用 --apply 才会提交。")
            return

        updated = apply_backfill(cur, args.month)
        after = load_validation_summary(cur, args.month)
        if after.missing_source_level:
            raise ValueError(f"回填后仍有 {after.missing_source_level} 条来源层级为空。")
        if after.amount_total != before.amount_total:
            raise ValueError("回填前后费用金额合计发生变化，事务已回滚。")

        conn.commit()
        print(f"回填完成并已提交：更新 {updated} 行；金额和业务线比例字段未修改。")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
