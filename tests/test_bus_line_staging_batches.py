from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from modules.bus_line_staging import batch
from modules.bus_line_staging.utils import get_table_columns


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class InheritanceCursor:
    def __init__(self):
        self.last_sql = ""
        self.rowcount = 0
        self.executed = []
        self.closed = False

    def execute(self, sql, params=None):
        self.last_sql = " ".join(sql.split())
        self.executed.append((self.last_sql, params))
        self.rowcount = 3 if self.last_sql.startswith("UPDATE staging_bus_revenue") else 0

    def fetchone(self):
        if "SELECT acct_period, previous_batch_id" in self.last_sql:
            return (date(2026, 6, 1), "old-batch")
        if "SELECT to_regclass" in self.last_sql:
            return ("staging_bus_revenue",)
        return None

    def fetchall(self):
        if "FROM information_schema.columns" in self.last_sql:
            return [("国际业务",), ("国内硬件",), ("审核状态",)]
        return []

    def close(self):
        self.closed = True


class CurrentBatchCursor:
    def __init__(self):
        self.last_sql = ""
        self.params = None

    def execute(self, sql, params=None):
        self.last_sql = " ".join(sql.split())
        self.params = params

    def fetchone(self):
        if "FROM bus_line_staging_batch" in self.last_sql:
            return ("edit-batch",)
        return None

    def close(self):
        pass


class FailureCursor:
    def __init__(self, status="GENERATING"):
        self.status = status
        self.last_sql = ""
        self.executed = []

    def execute(self, sql, params=None):
        self.last_sql = " ".join(sql.split())
        self.executed.append((self.last_sql, params))

    def fetchone(self):
        if "SELECT status FROM bus_line_staging_batch" in self.last_sql:
            return (self.status,)
        if "SELECT to_regclass" in self.last_sql:
            return ("staging_bus_revenue",)
        return None

    def close(self):
        pass


class BusLineStagingBatchTests(unittest.TestCase):
    def test_accounting_periods_normalizes_and_deduplicates_months(self):
        periods = batch.accounting_periods([date(2026, 6, 30), date(2026, 6, 1), date(2026, 7, 15)])
        self.assertEqual(periods, [date(2026, 6, 1), date(2026, 7, 1)])

    def test_runtime_table_columns_include_batch_id(self):
        columns = get_table_columns("staging_bus_revenue", ["国际业务"])
        self.assertEqual(columns[0], "batch_id")
        self.assertIn("来源编号", columns)
        self.assertIn("国际业务", columns)

    def test_inheritance_uses_batch_source_and_target_level(self):
        cursor = InheritanceCursor()
        connection = FakeConnection(cursor)
        with patch.object(batch, "connect_to_db", return_value=(connection, cursor)), patch.dict(
            batch.STAGING_TABLE_DATE_COLUMNS,
            {"staging_bus_revenue": "会计期间"},
            clear=True,
        ):
            result = batch.inherit_previous_values("new-batch", ["国际业务", "国内硬件", "不存在的业务线"])

        self.assertEqual(result, {"staging_bus_revenue": 3})
        update_sql = next(sql for sql, _ in cursor.executed if sql.startswith("UPDATE"))
        self.assertIn('previous."来源编号" = new_row."来源编号"', update_sql)
        self.assertIn('previous."唯一层级" = new_row."唯一层级"', update_sql)
        self.assertIn('"审核状态" = previous."审核状态"', update_sql)
        self.assertNotIn("不存在的业务线", update_sql)
        self.assertTrue(connection.committed)

    def test_current_edit_batch_is_resolved_by_month_not_latest_id(self):
        cursor = CurrentBatchCursor()
        connection = FakeConnection(cursor)
        with patch.object(batch, "connect_to_db", return_value=(connection, cursor)):
            result = batch.get_current_batch(date(2026, 6, 18), purpose="edit")

        self.assertEqual(result, "edit-batch")
        self.assertEqual(cursor.params, (date(2026, 6, 1), "FILLING"))
        self.assertIn("status = %s", cursor.last_sql)

    def test_cross_month_batch_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "exactly one accounting month"):
            batch._single_accounting_period([date(2026, 6, 1), date(2026, 7, 1)])

    def test_batch_number_contains_period_and_version(self):
        self.assertEqual(batch._batch_no(date(2026, 6, 1), 0), "BLS-202606-000")
        self.assertEqual(batch._batch_no(date(2026, 6, 1), 2), "BLS-202606-002")

    def test_invalid_batch_purpose_is_rejected_before_database_access(self):
        with self.assertRaisesRegex(ValueError, "purpose"):
            batch.get_current_batch(date(2026, 6, 1), purpose="latest")

    def test_failed_generating_batch_only_deletes_its_own_rows(self):
        cursor = FailureCursor()
        connection = FakeConnection(cursor)
        with patch.object(batch, "connect_to_db", return_value=(connection, cursor)), patch.dict(
            batch.STAGING_TABLE_DATE_COLUMNS,
            {"staging_bus_revenue": "会计期间"},
            clear=True,
        ):
            batch.fail_batch("failed-batch", "source error")

        delete_sql, delete_params = next(
            (sql, params) for sql, params in cursor.executed if sql.startswith("DELETE")
        )
        self.assertEqual(delete_sql, "DELETE FROM staging_bus_revenue WHERE batch_id = %s")
        self.assertEqual(delete_params, ("failed-batch",))
        self.assertTrue(connection.committed)

    def test_completed_batch_is_not_deleted_by_late_notification_failure(self):
        cursor = FailureCursor(status="READY")
        connection = FakeConnection(cursor)
        with patch.object(batch, "connect_to_db", return_value=(connection, cursor)), patch.dict(
            batch.STAGING_TABLE_DATE_COLUMNS,
            {"staging_bus_revenue": "会计期间"},
            clear=True,
        ):
            batch.fail_batch("ready-batch", "notification error")

        self.assertFalse(any(sql.startswith("DELETE") for sql, _ in cursor.executed))
        self.assertTrue(connection.committed)


if __name__ == "__main__":
    unittest.main()
