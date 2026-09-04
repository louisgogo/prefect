import unittest
from unittest.mock import patch

import pandas as pd

from modules.bus_line_cal.tasks import profit_tasks


class ProfitSourceLevelTests(unittest.TestCase):
    def test_manual_other_profit_keeps_source_and_distribution_levels_separate(self):
        source_profit = pd.DataFrame(
            [
                {
                    "id": 1,
                    "source_no": "OTHER-001",
                    "unique_lvl": "源事实唯一层级",
                    "mo_amt": 100,
                }
            ]
        )
        business_line_ratios = pd.DataFrame(
            [
                {
                    "source_no": "OTHER-001",
                    "source_lvl": "源事实唯一层级",
                    "unique_lvl": "Staging分摊层级",
                    "bus_line": "业务线A",
                    "category": "二次分配",
                    "rate": 0.6,
                    "class": "其他",
                }
            ]
        )

        processed = profit_tasks.process_manual_profit_task.fn(source_profit, business_line_ratios)

        captured = {}

        def capture_write(table_name, date_column, frame, frame_date_column, date_range):
            captured["table_name"] = table_name
            captured["frame"] = frame.copy()

        with patch.object(profit_tasks, "replace_date_range_data", capture_write):
            profit_tasks.save_profit_detail_task.fn(
                processed, pd.date_range("2026-07-01", "2026-07-31")
            )

        saved = captured["frame"].iloc[0]
        self.assertEqual(captured["table_name"], "fact_bus_profit_bd")
        self.assertEqual(saved["unique_lvl"], "源事实唯一层级")
        self.assertEqual(saved["sec_dist_lvl"], "Staging分摊层级")
        self.assertEqual(saved["mo_amt"], 60)


if __name__ == "__main__":
    unittest.main()
