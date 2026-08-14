from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from modules.bus_line_staging import batch
from modules.bus_line_staging.flows import bus_line_staging_flow as flow_module
from modules.bus_line_staging.module_selection import (
    ALL_MODULE_CODES,
    ALL_MODULE_OPTIONS,
    normalize_modules,
)


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


class CloneCursor:
    def __init__(self, previous_batch_id="previous-batch"):
        self.previous_batch_id = previous_batch_id
        self.last_sql = ""
        self.executed = []
        self.rowcount = 0
        self.closed = False

    def execute(self, sql, params=None):
        self.last_sql = " ".join(sql.split())
        self.executed.append((self.last_sql, params))
        if self.last_sql.startswith("INSERT INTO staging_bus_") and "SELECT %s" in self.last_sql:
            self.rowcount = 2
        else:
            self.rowcount = 0

    def fetchone(self):
        if "SELECT previous_batch_id, status" in self.last_sql:
            return (self.previous_batch_id, "GENERATING")
        if "SELECT to_regclass" in self.last_sql:
            return ("present",)
        return None

    def fetchall(self):
        if "FROM information_schema.columns" in self.last_sql:
            return [
                ("id",),
                ("batch_id",),
                ("record_id",),
                ("来源编号",),
                ("唯一层级",),
                ("审核状态",),
                ("创建时间",),
            ]
        return []

    def close(self):
        self.closed = True


class ModuleSelectionTests(unittest.TestCase):
    def test_empty_selection_defaults_to_all_modules(self):
        self.assertEqual(normalize_modules(None), ALL_MODULE_CODES)
        self.assertEqual(normalize_modules([]), ALL_MODULE_CODES)

    def test_chinese_multiselect_maps_to_canonical_codes(self):
        self.assertEqual(
            normalize_modules(["存货", "收入", "收入"]),
            ("revenue", "inventory"),
        )

    def test_unknown_module_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "不支持的Staging模块"):
            normalize_modules(["不存在"])

    def test_prefect_schema_exposes_enum_array_for_multiselect(self):
        schema = flow_module.bus_line_staging_flow.parameters.model_dump()
        module_schema = schema["properties"]["modules"]["anyOf"][0]
        self.assertEqual(module_schema["type"], "array")
        self.assertEqual(module_schema["items"]["enum"], list(ALL_MODULE_OPTIONS))

    def test_all_deployment_entries_default_to_all_multiselect_options(self):
        root = Path(__file__).resolve().parents[1]
        for script_name in ("deploy_local.py", "deploy_to_server.py", "deploy_production.py"):
            source = (root / script_name).read_text(encoding="utf-8")
            self.assertIn(
                "from modules.bus_line_staging.module_selection import ALL_MODULE_OPTIONS", source
            )
            self.assertIn('parameters={"modules": list(ALL_MODULE_OPTIONS)}', source)


class BatchCloneTests(unittest.TestCase):
    def test_clone_previous_batch_copies_every_table_and_ratios(self):
        cursor = CloneCursor()
        connection = FakeConnection(cursor)
        with patch.object(batch, "connect_to_db", return_value=(connection, cursor)):
            result = batch.clone_previous_batch("new-batch")

        self.assertEqual(set(result), set(batch.STAGING_TABLE_DATE_COLUMNS))
        self.assertTrue(all(count == 2 for count in result.values()))
        base_insert = next(
            sql
            for sql, _ in cursor.executed
            if sql.startswith("INSERT INTO staging_bus_expense(batch_id, record_id")
        )
        self.assertIn("clone.new_record_id", base_insert)
        self.assertNotIn('"创建时间"', base_insert)
        ratio_inserts = [
            sql
            for sql, _ in cursor.executed
            if sql.startswith("INSERT INTO staging_bus_line_ratio")
        ]
        self.assertEqual(len(ratio_inserts), len(batch.STAGING_TABLE_DATE_COLUMNS))
        self.assertTrue(connection.committed)

    def test_partial_refresh_without_previous_batch_rolls_back(self):
        cursor = CloneCursor(previous_batch_id=None)
        connection = FakeConnection(cursor)
        with patch.object(batch, "connect_to_db", return_value=(connection, cursor)):
            with self.assertRaisesRegex(ValueError, "请先运行一次全部模块"):
                batch.clone_previous_batch("new-batch")

        self.assertTrue(connection.rolled_back)


class FlowRoutingTests(unittest.TestCase):
    def _patch_flow(self):
        logger = MagicMock()
        patches = {
            "get_run_logger": patch.object(flow_module, "get_run_logger", return_value=logger),
            "flow_run": patch.object(flow_module, "flow_run", SimpleNamespace(id="flow-run")),
            "get_date_range": patch.object(
                flow_module, "get_date_range", return_value=[date(2026, 6, 1)]
            ),
            "notify": patch.object(flow_module, "notify_hermes_task"),
            "validate": patch.object(
                flow_module,
                "validate_fact_assignments_task",
                return_value={"fact_revenue": 0, "fact_inventory": 0},
            ),
            "start": patch.object(flow_module, "start_batch", return_value="new-batch"),
            "clone": patch.object(
                flow_module, "clone_previous_batch", return_value={"staging_bus_revenue": 5}
            ),
            "reset": patch.object(
                flow_module, "reset_batch_tables", return_value={"staging_bus_revenue": 5}
            ),
            "restore": patch.object(
                flow_module,
                "restore_fact_assignments_task",
                return_value={"fact_revenue": 0, "fact_inventory": 0},
            ),
            "expense": patch.object(flow_module, "run_expense_split_to_staging_task"),
            "revenue": patch.object(flow_module, "run_revenue_other_split_task"),
            "unassigned": patch.object(flow_module, "run_unassigned_split_task"),
            "ratio": patch.object(flow_module, "run_revenue_ratio_fill_task"),
            "asset": patch.object(flow_module, "run_inv_ar_split_task"),
            "lines": patch.object(flow_module, "get_bus_lines", return_value=["国际业务"]),
            "inherit": patch.object(
                flow_module,
                "inherit_previous_values",
                return_value={"staging_bus_revenue": 2},
            ),
            "compare": patch.object(
                flow_module,
                "compare_batch_to_previous",
                return_value={
                    "previous_batch_no": "BLS-202606-001",
                    "totals": {
                        "old_records": 5,
                        "new_records": 5,
                        "added": 0,
                        "removed": 0,
                        "source_changed": 0,
                        "unchanged": 5,
                    },
                    "tables": {},
                },
            ),
            "complete": patch.object(flow_module, "complete_batch", return_value="READY"),
            "summary": patch.object(
                flow_module,
                "batch_summary",
                return_value={"batch_no": "BLS-202606-002"},
            ),
            "fail": patch.object(flow_module, "fail_batch"),
        }
        return logger, patches

    def test_selected_modules_route_only_matching_writes(self):
        _, patches = self._patch_flow()
        started = [patcher.start() for patcher in patches.values()]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(list(patches.values()))])

        result = flow_module.bus_line_staging_flow.fn(
            start_date="2026-06-01",
            end_date="2026-06-01",
            modules=["收入", "存货"],
        )

        mocks = dict(zip(patches, started))
        mocks["expense"].assert_not_called()
        mocks["revenue"].assert_called_once_with([date(2026, 6, 1)], "new-batch", ["revenue"])
        mocks["unassigned"].assert_called_once_with([date(2026, 6, 1)], "new-batch", ["revenue"])
        mocks["ratio"].assert_called_once_with([date(2026, 6, 1)], "new-batch")
        mocks["asset"].assert_called_once_with([date(2026, 6, 1)], "new-batch", ["inventory"])
        mocks["clone"].assert_called_once_with("new-batch")
        mocks["reset"].assert_called_once_with(
            "new-batch", ("staging_bus_revenue", "staging_bus_inventory")
        )
        self.assertEqual(result["modules"], ["revenue", "inventory"])
        self.assertEqual(result["module_labels"], ["收入", "存货"])
        self.assertEqual(result["refresh_mode"], "selected")

    def test_default_selection_keeps_existing_full_flow_path(self):
        _, patches = self._patch_flow()
        patches["validate"] = patch.object(
            flow_module,
            "validate_fact_assignments_task",
            return_value={
                table: 0
                for table in (
                    "fact_revenue",
                    "fact_expense",
                    "fact_profit_bd",
                    "fact_receivable",
                    "fact_inventory",
                    "fact_inventory_on_way",
                )
            },
        )
        patches["restore"] = patch.object(
            flow_module,
            "restore_fact_assignments_task",
            return_value={
                table: 0
                for table in (
                    "fact_revenue",
                    "fact_expense",
                    "fact_profit_bd",
                    "fact_receivable",
                    "fact_inventory",
                    "fact_inventory_on_way",
                )
            },
        )
        started = [patcher.start() for patcher in patches.values()]
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(list(patches.values()))])

        result = flow_module.bus_line_staging_flow.fn(
            start_date="2026-06-01", end_date="2026-06-01", modules=[]
        )

        mocks = dict(zip(patches, started))
        mocks["clone"].assert_not_called()
        mocks["reset"].assert_not_called()
        mocks["expense"].assert_called_once()
        mocks["revenue"].assert_called_once()
        mocks["unassigned"].assert_called_once()
        mocks["ratio"].assert_called_once()
        mocks["asset"].assert_called_once()
        self.assertEqual(result["modules"], list(ALL_MODULE_CODES))
        self.assertEqual(result["refresh_mode"], "all")


if __name__ == "__main__":
    unittest.main()
