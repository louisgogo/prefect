"""Focused tests for the unified business-data refresh workflow."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from modules.business_data_refresh.flows.business_data_refresh_flow import (
    execute_dataset_runners,
    resolve_datasets,
)
from modules.business_data_refresh.tasks.business_data_refresh_tasks import (
    BusinessDataRefreshError,
    fetch_supplier_rows,
    normalize_material_rows,
    normalize_supplier_rows,
    supplier_is_active,
    validate_acquiring_rows,
    validate_snapshot_count,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, pages):
        self.pages = list(pages)
        self.payloads = []

    def post(self, _url, **kwargs):
        self.payloads.append(kwargs["json"])
        return FakeResponse(self.pages.pop(0))


class BusinessDataRefreshTests(unittest.TestCase):
    def test_server_deployments_default_to_supplier_only(self):
        repository = Path(__file__).resolve().parents[1]
        for script_name in ("deploy_to_server.py", "deploy_production.py"):
            source = (repository / script_name).read_text(encoding="utf-8")
            self.assertIn('parameters={"datasets": ["supplier"], "requested_by": None}', source)

    def test_dataset_selection_uses_canonical_order(self):
        self.assertEqual(
            resolve_datasets(["supplier", "customer"]),
            ["customer", "supplier"],
        )
        with self.assertRaisesRegex(ValueError, "不支持的数据集"):
            resolve_datasets(["unknown"])

    def test_status_d_supplier_is_active_when_enabled(self):
        self.assertTrue(supplier_is_active("D", "A"))
        self.assertTrue(supplier_is_active("C", "A"))
        self.assertFalse(supplier_is_active("D", "B"))

    def test_supplier_duplicate_conflict_is_rejected(self):
        base = {
            "FSupplierId": 1,
            "FNumber": "S001",
            "FName": "供应商甲",
            "FCreateOrgId.FNumber": "1000",
            "FCreateOrgId.FName": "股份",
            "FUseOrgId.FNumber": "1000",
            "FUseOrgId.FName": "股份",
            "FDocumentStatus": "C",
            "FForbidStatus": "A",
        }
        with self.assertRaisesRegex(BusinessDataRefreshError, "重复且内容冲突"):
            normalize_supplier_rows([base, {**base, "FName": "供应商乙"}])

    def test_material_prefers_holdings_org_then_other_orgs(self):
        rows = [
            {
                "forgid": "1200",
                "fmnumber": "M001",
                "fmname": "子公司名称",
                "fpmodel": None,
                "fcategoryname": "A",
                "fpclass": "P",
            },
            {
                "forgid": "1000",
                "fmnumber": "M001",
                "fmname": "股份名称",
                "fpmodel": None,
                "fcategoryname": "A",
                "fpclass": "P",
            },
        ]
        result = normalize_material_rows(rows)
        selected = next(item for item in result if item["encoding"] == "M001")
        self.assertEqual(selected["name"], "股份名称")
        self.assertIn("PD99", {item["encoding"] for item in result})

    def test_row_drop_guard_rejects_abnormal_snapshot(self):
        with patch.dict(os.environ, {"BUSINESS_DATA_MIN_ROW_RATIO": "0.8"}):
            with self.assertRaisesRegex(BusinessDataRefreshError, "安全阈值"):
                validate_snapshot_count("customer", 79, 100)
            validate_snapshot_count("customer", 80, 100)
        with self.assertRaisesRegex(BusinessDataRefreshError, "空快照"):
            validate_snapshot_count("customer", 0, 100)

    def test_acquiring_duplicate_dimension_key_is_rejected(self):
        row = ("202607", "01", "分公司", "P", "产品", "44", "4401", 1, 2, 3, 4)
        with self.assertRaisesRegex(BusinessDataRefreshError, "重复业务维度键"):
            validate_acquiring_rows("t_jl_area_merch_netin", [row, row])

    def test_supplier_api_paginates_until_empty_page(self):
        session = FakeSession([[{"FNumber": "S1"}], [{"FNumber": "S2"}], []])
        with patch.dict(os.environ, {"XGD_TOKEN": "test-token"}, clear=False):
            rows = fetch_supplier_rows(session, page_size=1, max_retries=0)
        self.assertEqual([row["FNumber"] for row in rows], ["S1", "S2"])
        self.assertEqual(
            [payload["StartRow"] for payload in session.payloads],
            [0, 1, 2],
        )

    def test_partial_failure_preserves_success_summary(self):
        events = []

        def fail():
            raise RuntimeError("source unavailable")

        summary = execute_dataset_runners(
            ["customer", "supplier"],
            {"customer": lambda: {"source_rows": 2, "target_rows": 2}, "supplier": fail},
            on_start=lambda code: events.append((code, "running")),
            on_success=lambda code, _result: events.append((code, "completed")),
            on_failure=lambda code, _exc: events.append((code, "failed")),
        )

        self.assertEqual(summary["status"], "partial_failed")
        self.assertEqual(summary["completed_datasets"], ["customer"])
        self.assertEqual(summary["failed_datasets"], ["supplier"])
        self.assertIn(("customer", "completed"), events)
        self.assertIn(("supplier", "failed"), events)


if __name__ == "__main__":
    unittest.main()
