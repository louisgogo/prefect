import unittest
from datetime import date
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pandas as pd

from modules.budget_update.flows import budget_update_flow as budget_flow_module
from modules.budget_update.flows.budget_update_flow import _get_official_budget_version
from modules.budget_update.tasks.budget_update_tasks import (
    BUDGET_VERSION_COLUMNS,
    _append_budget_data,
    _first_unused_archive_date,
    _prepare_budget_version_write,
)


class _FakeResult:
    def __init__(self, rows=(), rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def __iter__(self):
        return iter(self._rows)


class _FakeConnection:
    def __init__(self, existing_versions=()):
        self.existing_versions = list(existing_versions)
        self.calls = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.calls.append((sql, params))
        if sql.startswith("SELECT DISTINCT"):
            return _FakeResult((value,) for value in self.existing_versions)
        if sql.startswith("UPDATE") or sql.startswith("DELETE"):
            return _FakeResult(rowcount=1)
        return _FakeResult()


class BudgetVersionDateTests(unittest.TestCase):
    def test_annual_official_version_is_january_first(self):
        self.assertEqual(_get_official_budget_version("2026", "年初预算"), "2026-01-01")

    def test_midyear_official_version_is_july_first(self):
        self.assertEqual(_get_official_budget_version("2026", "年中预算"), "2026-07-01")

    def test_invalid_year_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "budget_year"):
            _get_official_budget_version("not-a-year", "年初预算")

    def test_first_archive_uses_day_two(self):
        result = _first_unused_archive_date(pd.Timestamp("2026-07-01"), {date(2026, 7, 1)})
        self.assertEqual(result, date(2026, 7, 2))

    def test_archive_uses_first_gap(self):
        result = _first_unused_archive_date(
            pd.Timestamp("2026-07-01"),
            {date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 4)},
        )
        self.assertEqual(result, date(2026, 7, 3))

    def test_archive_fails_when_month_is_full(self):
        occupied = {date(2026, 7, day) for day in range(1, 32)}
        with self.assertRaisesRegex(ValueError, "没有可用"):
            _first_unused_archive_date(pd.Timestamp("2026-07-01"), occupied)

    def test_non_first_day_cannot_be_official(self):
        with self.assertRaisesRegex(ValueError, "必须为每月 1 日"):
            _first_unused_archive_date(pd.Timestamp("2026-07-02"), set())


class BudgetVersionWritePreparationTests(unittest.TestCase):
    def test_first_official_version_does_not_archive_or_delete(self):
        connection = _FakeConnection(existing_versions=[])

        archive_date = _prepare_budget_version_write(
            connection,
            official_version=pd.Timestamp("2026-07-01"),
            save_previous_version=True,
        )

        self.assertIsNone(archive_date)
        write_calls = [
            call
            for call in connection.calls
            if call[0].startswith("UPDATE") or call[0].startswith("DELETE")
        ]
        self.assertEqual(write_calls, [])

    def test_save_previous_archives_all_tables_to_same_date(self):
        connection = _FakeConnection(existing_versions=[date(2026, 7, 1), date(2026, 7, 2)])

        archive_date = _prepare_budget_version_write(
            connection,
            official_version=pd.Timestamp("2026-07-01"),
            save_previous_version=True,
        )

        self.assertEqual(archive_date, date(2026, 7, 3))
        update_calls = [call for call in connection.calls if call[0].startswith("UPDATE")]
        self.assertEqual(len(update_calls), len(BUDGET_VERSION_COLUMNS))
        self.assertEqual({params["archive_date"] for _, params in update_calls}, {date(2026, 7, 3)})

    def test_direct_overwrite_deletes_official_from_all_tables(self):
        connection = _FakeConnection(existing_versions=[date(2026, 7, 1)])

        archive_date = _prepare_budget_version_write(
            connection,
            official_version=pd.Timestamp("2026-07-01"),
            save_previous_version=False,
        )

        self.assertIsNone(archive_date)
        delete_calls = [call for call in connection.calls if call[0].startswith("DELETE")]
        self.assertEqual(len(delete_calls), len(BUDGET_VERSION_COLUMNS))
        self.assertEqual(
            {params["official_date"] for _, params in delete_calls}, {date(2026, 7, 1)}
        )

    def test_insert_errors_are_not_swallowed(self):
        df = pd.DataFrame({"report_date": [date(2026, 7, 1)]})
        with patch.object(pd.DataFrame, "to_sql", side_effect=RuntimeError("insert failed")):
            with self.assertRaisesRegex(RuntimeError, "insert failed"):
                _append_budget_data(object(), "bud_expense", df)


class BudgetUpdateFlowParameterTests(unittest.TestCase):
    def test_fast_run_defaults_to_direct_overwrite(self):
        properties = budget_flow_module.budget_update_flow.parameters.model_dump()["properties"]
        self.assertFalse(properties["save_previous_version"]["default"])

    def test_flow_derives_official_version_and_passes_archive_switch(self):
        source_data = {
            "fone_exp": pd.DataFrame(),
            "fone_biz": pd.DataFrame(),
            "fone_emp": pd.DataFrame(),
            "fone_amo": pd.DataFrame(),
            "fone_pro": pd.DataFrame(),
            "fone_shared_rate": pd.DataFrame(),
        }
        empty_result = pd.DataFrame()
        write_mock = MagicMock(return_value="2026-07-02")
        notify_mock = MagicMock()

        with TemporaryDirectory() as output_dir, patch.multiple(
            budget_flow_module,
            fetch_fone_budget_data_task=MagicMock(return_value=source_data),
            process_expense_budget_task=MagicMock(return_value=empty_result),
            process_income_budget_task=MagicMock(return_value=empty_result),
            process_personnel_budget_task=MagicMock(return_value=empty_result),
            process_cash_budget_task=MagicMock(return_value=empty_result),
            process_profit_budget_task=MagicMock(return_value=empty_result),
            process_shared_rate_budget_task=MagicMock(return_value=empty_result),
            write_budget_to_db_task=write_mock,
            notify_hermes_task=notify_mock,
        ):
            budget_flow_module.budget_update_flow.fn(
                budget_year="2026",
                fone_version="AdjustVersion1",
                budget_type="年中预算",
                save_previous_version=True,
                actual_through_month=6,
                refresh_ai_data_etl=False,
                output_dir=output_dir,
            )

        write_kwargs = write_mock.call_args.kwargs
        self.assertEqual(write_kwargs["budget_version"], pd.Timestamp("2026-07-01"))
        self.assertTrue(write_kwargs["save_previous_version"])

        completed_payload = [
            call.kwargs["payload"]
            for call in notify_mock.call_args_list
            if call.kwargs["event"] == "completed"
        ][0]
        self.assertEqual(completed_payload["budget_version"], "2026-07-01")
        self.assertEqual(completed_payload["archived_version"], "2026-07-02")


if __name__ == "__main__":
    unittest.main()
