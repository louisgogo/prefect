"""现金流量表独立刷新子流程的单元测试。"""

import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from modules.data_import.flows.cashflow_refresh_flow import (
    _get_cashflow_refresh_defaults_by_date,
    cashflow_refresh_flow,
)
from modules.data_import.tasks import data_import_tasks


class CashflowRefreshTests(unittest.TestCase):
    def test_refresh_task_only_updates_cashflow_tables(self):
        dfs = {
            "fact_cashflow": pd.DataFrame({"date": ["2026-08-01"], "project": ["经营活动"]}),
            "excel_cashflow_intl": pd.DataFrame({"date": ["2026-08-01"], "project": ["国际经营活动"]}),
            "fact_profit_stmt": pd.DataFrame({"date": ["2026-08-01"]}),
        }

        with patch.object(data_import_tasks, "update_data_by_date_range_task") as update:
            result = data_import_tasks.update_cashflow_data_task.fn(
                dfs,
                "2026-08-01",
                "2026-08-31",
            )

        self.assertEqual(result, {"updated_count": 2, "skipped_count": 0})
        self.assertEqual(
            [call.args[0] for call in update.call_args_list],
            [
                "fact_cashflow",
                "excel_cashflow_intl",
            ],
        )

    def test_refresh_task_can_skip_existing_tables_without_replacing(self):
        dfs = {
            "fact_cashflow": pd.DataFrame({"date": ["2026-08-01"]}),
            "excel_cashflow_intl": pd.DataFrame({"date": ["2026-08-01"]}),
        }

        with (
            patch.object(data_import_tasks, "_check_data_exists", return_value=True),
            patch.object(data_import_tasks, "update_data_by_date_range_task") as update,
        ):
            result = data_import_tasks.update_cashflow_data_task.fn(
                dfs,
                "2026-08-01",
                "2026-08-31",
                replace_existing=False,
            )

        self.assertEqual(result, {"updated_count": 0, "skipped_count": 2})
        update.assert_not_called()

    def test_flow_requires_valid_month(self):
        with self.assertRaisesRegex(ValueError, "1-12"):
            cashflow_refresh_flow.fn(year=2026, month=13)

    def test_defaults_use_reference_year_and_month(self):
        defaults = _get_cashflow_refresh_defaults_by_date(date(2026, 9, 22))

        self.assertEqual(defaults, {"year": 2026, "month": 9})


if __name__ == "__main__":
    unittest.main()
