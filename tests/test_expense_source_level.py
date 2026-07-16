from __future__ import annotations

import unittest

import pandas as pd

from modules.bus_line_staging.tasks.expense_tasks import _set_expense_source_metadata
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


if __name__ == "__main__":
    unittest.main()
