from __future__ import annotations

import unittest
from unittest.mock import patch

from modules.bus_line_staging.config import get_bus_lines


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = None
        self.closed = False

    def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = params

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class BusLineStagingConfigTests(unittest.TestCase):
    def test_business_lines_come_from_dimension_without_filtering_terminated_lines(self):
        cursor = FakeCursor([("集团",), ("web3",), ("审核业务",)])
        connection = FakeConnection()

        with patch(
            "modules.bus_line_staging.config.connect_to_db",
            return_value=(connection, cursor),
        ):
            result = get_bus_lines()

        self.assertEqual(result, ["集团", "web3", "审核业务"])
        self.assertIn("FROM dim_bus_line", cursor.sql)
        self.assertNotIn("status", cursor.sql.lower())
        self.assertEqual(cursor.params, ("无", "抵销数"))
        self.assertTrue(cursor.closed)
        self.assertTrue(connection.closed)

    def test_fallback_keeps_group_and_web3(self):
        with patch(
            "modules.bus_line_staging.config.connect_to_db",
            side_effect=RuntimeError("database unavailable"),
        ):
            result = get_bus_lines()

        self.assertIn("集团", result)
        self.assertIn("web3", result)
        self.assertNotIn("无", result)
        self.assertNotIn("抵销数", result)


if __name__ == "__main__":
    unittest.main()
