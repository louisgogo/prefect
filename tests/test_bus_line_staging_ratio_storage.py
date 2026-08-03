from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from modules.bus_line_staging import utils


class FakeCursor:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class BusLineRatioStorageTests(unittest.TestCase):
    def _sample_frame(self):
        return pd.DataFrame(
            [
                {
                    "来源编号": "SRC-1",
                    "唯一层级": "ORG-1",
                    "一级组织": "一级",
                    "二级组织": "二级",
                    "三级组织": "三级",
                    "会计期间": date(2026, 6, 1),
                    "收入大类": "服务",
                    "产品大类": "AI",
                    "物料名称": "算力服务",
                    "不含税金额本位币": 100.0,
                    "AI算力": 0.75,
                    "集团": 0.25,
                }
            ]
        )

    def test_dimension_only_line_is_written_to_ratio_table_not_base_table(self):
        cursor = FakeCursor()
        connection = FakeConnection()
        copied = []

        def capture_copy(df, table_name, columns, conn, cur, commit=True):
            copied.append((table_name, list(columns), df.copy(), commit))

        with patch.object(utils, "connect_to_db", return_value=(connection, cursor)), patch.object(
            utils, "create_staging_table"
        ), patch.object(utils, "copy_data_to_postgres", side_effect=capture_copy):
            utils.insert_to_staging_table(
                self._sample_frame(),
                pd.DataFrame(),
                [],
                [date(2026, 6, 1)],
                "会计期间",
                "staging_bus_revenue",
                ["AI算力", "集团"],
                "batch-1",
            )

        base_copy, ratio_copy = copied
        self.assertEqual(base_copy[0], "staging_bus_revenue")
        self.assertIn("record_id", base_copy[1])
        self.assertNotIn("AI算力", base_copy[1])
        self.assertFalse(base_copy[3])
        self.assertEqual(ratio_copy[0], "staging_bus_line_ratio")
        self.assertEqual(ratio_copy[2]["bus_line"].tolist(), ["AI算力", "集团"])
        self.assertEqual(ratio_copy[2]["rate"].tolist(), [0.75, 0.25])
        self.assertFalse(ratio_copy[3])
        self.assertEqual(connection.commits, 2)

    def test_ratio_copy_failure_rolls_back_data_transaction(self):
        cursor = FakeCursor()
        connection = FakeConnection()
        calls = 0

        def fail_ratio_copy(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("ratio copy failed")

        with patch.object(utils, "connect_to_db", return_value=(connection, cursor)), patch.object(
            utils, "create_staging_table"
        ), patch.object(utils, "copy_data_to_postgres", side_effect=fail_ratio_copy):
            with self.assertRaisesRegex(RuntimeError, "ratio copy failed"):
                utils.insert_to_staging_table(
                    self._sample_frame(),
                    pd.DataFrame(),
                    [],
                    [date(2026, 6, 1)],
                    "会计期间",
                    "staging_bus_revenue",
                    ["AI算力", "集团"],
                    "batch-1",
                )

        self.assertEqual(connection.rollbacks, 1)


if __name__ == "__main__":
    unittest.main()
