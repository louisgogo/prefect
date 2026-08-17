"""Focused tests for the unified business-data refresh workflow."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from modules.business_data_refresh.flows.business_data_refresh_flow import (
    business_data_refresh_flow,
    execute_dataset_runners,
    resolve_datasets,
)
from modules.business_data_refresh.tasks.business_data_refresh_tasks import (
    BusinessDataRefreshError,
    fetch_customer_rows,
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
    def test_prefect_flow_parameter_model_resolves_typing_annotations(self):
        validated = business_data_refresh_flow.validate_parameters(
            {"datasets": ["supplier"], "requested_by": "test-user"}
        )
        self.assertEqual(validated["datasets"], ["supplier"])
        self.assertEqual(validated["requested_by"], "test-user")

    def test_server_deployments_default_to_all_datasets(self):
        repository = Path(__file__).resolve().parents[1]
        for script_name in ("deploy_to_server.py", "deploy_production.py"):
            source = (repository / script_name).read_text(encoding="utf-8")
            for dataset in (
                "customer",
                "material",
                "rd_project",
                "supplier",
                "acquiring_metrics",
            ):
                self.assertIn(f'"{dataset}"', source)
            self.assertNotIn('parameters={"datasets": ["supplier"]', source)

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

    def test_acquiring_preserves_multiple_rows_for_the_same_dimension(self):
        row = ("202607", "01", "分公司", "P", "产品", "44", "4401", 1, 2, 3, 4)
        validate_acquiring_rows("t_jl_area_merch_netin", [row, row])

    def test_acquiring_rejects_source_column_mismatch(self):
        with self.assertRaisesRegex(BusinessDataRefreshError, "列数与目标表不一致"):
            validate_acquiring_rows("t_jl_area_merch_netin", [("202607",)])

    def test_supplier_api_paginates_until_empty_page(self):
        session = FakeSession([[{"FNumber": "S1"}], [{"FNumber": "S2"}], []])
        with patch.dict(os.environ, {"XGD_TOKEN": "test-token"}, clear=False):
            rows = fetch_supplier_rows(session, page_size=1, max_retries=0)
        self.assertEqual([row["FNumber"] for row in rows], ["S1", "S2"])
        self.assertEqual(
            [payload["StartRow"] for payload in session.payloads],
            [0, 1, 2],
        )

    def test_customer_api_paginates_and_maps_location_names(self):
        session = FakeSession(
            [
                [
                    {
                        "FCUSTID": 1,
                        "FNumber": "C001",
                        "FName": "客户甲",
                        "FCOUNTRY.FDataValue": "中国",
                        "FPROVINCE.FDataValue": "广东省",
                        "FCITY.FDataValue": "深圳市",
                        "FPROVINCIAL.FDataValue": "华南地区",
                    }
                ],
                [],
            ]
        )
        with patch.dict(os.environ, {"XGD_TOKEN": "test-token"}, clear=False):
            rows = fetch_customer_rows(session, page_size=1, max_retries=0)

        self.assertEqual(
            rows,
            [
                {
                    "fnumber": "C001",
                    "fname": "客户甲",
                    "fcountry": "中国",
                    "fprovince": "广东省",
                    "fcity": "深圳市",
                    "fprovincial": "华南地区",
                }
            ],
        )
        self.assertEqual(
            [payload["StartRow"] for payload in session.payloads],
            [0, 1],
        )
        self.assertEqual(session.payloads[0]["FormId"], "BD_Customer")
        self.assertEqual(
            session.payloads[0]["FilterString"],
            "FUseOrgId.FNumber = '1000'",
        )
        self.assertEqual(session.payloads[0]["OrderString"], "FCUSTID ASC")

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
