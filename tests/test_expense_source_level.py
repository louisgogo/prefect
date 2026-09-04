from __future__ import annotations

import unittest

import pandas as pd

from modules.bus_line_staging.tasks.expense_tasks import (
    _deduplicate_wage_rates,
    _set_expense_source_metadata,
)
from modules.bus_line_staging.utils import get_table_columns


class ExpenseSourceLevelTests(unittest.TestCase):
    def test_expense_staging_columns_include_source_level(self):
        columns = get_table_columns("staging_bus_expense", ["小POS"])

        self.assertIn("来源层级", columns)

    def test_source_metadata_separates_original_org_from_source_type(self):
        source = pd.DataFrame({"唯一层级": ["服务事业群-行政中心-公共部门"]})

        result = _set_expense_source_metadata(source, "唯一层级")

        self.assertEqual(result.loc[0, "来源层级"], "服务事业群-行政中心-公共部门")
        self.assertEqual(result.loc[0, "数据来源"], "费用")

    def test_source_metadata_rejects_blank_source_level(self):
        source = pd.DataFrame({"唯一层级": [None]})

        with self.assertRaisesRegex(ValueError, "缺少来源层级"):
            _set_expense_source_metadata(source, "唯一层级")

    def test_identical_wage_rates_are_deduplicated_after_org_rename(self):
        source = pd.DataFrame(
            {
                "unique_lvl": ["合并后组织", "合并后组织", "合并后组织"],
                "bus_line": ["小POS", "小POS", "大POS"],
                "rate": [0.7, 0.7, 0.3],
                "date": ["2026-06-01", "2026-06-01", "2026-06-01"],
            }
        )

        result, removed = _deduplicate_wage_rates(source)

        self.assertEqual(removed, 1)
        self.assertEqual(len(result), 2)

    def test_conflicting_wage_rates_after_org_rename_are_rejected(self):
        source = pd.DataFrame(
            {
                "unique_lvl": ["合并后组织", "合并后组织"],
                "bus_line": ["小POS", "小POS"],
                "rate": [0.7, 0.6],
                "date": ["2026-06-01", "2026-06-01"],
            }
        )

        with self.assertRaisesRegex(ValueError, "比例不一致"):
            _deduplicate_wage_rates(source)


if __name__ == "__main__":
    unittest.main()
