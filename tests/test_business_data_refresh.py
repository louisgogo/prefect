"""Focused tests for the unified business-data refresh workflow."""

from __future__ import annotations

import os
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from modules.business_data_refresh.flows.business_data_refresh_flow import (
    business_data_refresh_flow,
    execute_dataset_runners,
    resolve_datasets,
)
from modules.business_data_refresh.tasks.business_data_refresh_tasks import (
    BusinessDataRefreshError,
    _replace_exchange_rate_month,
    fetch_customer_rows,
    fetch_exchange_rate_rows,
    fetch_supplier_rows,
    normalize_exchange_rate_rows,
    normalize_material_rows,
    normalize_supplier_rows,
    resolve_exchange_rate_period,
    supplier_is_active,
    validate_acquiring_rows,
    validate_snapshot_count,
)
from modules.data_import.flows.data_import_flow import data_import_flow


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


class FakeCursor:
    def __init__(self, fetch_rows):
        self.fetch_rows = list(fetch_rows)
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params))

    def fetchone(self):
        return self.fetch_rows.pop(0)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class BusinessDataRefreshTests(unittest.TestCase):
    def test_prefect_flow_parameter_model_resolves_typing_annotations(self):
        validated = business_data_refresh_flow.validate_parameters(
            {
                "datasets": ["supplier"],
                "requested_by": "test-user",
                "exchange_rate_year": 2026,
                "exchange_rate_month": 8,
            }
        )
        self.assertEqual(validated["datasets"], ["supplier"])
        self.assertEqual(validated["requested_by"], "test-user")
        self.assertEqual(validated["exchange_rate_year"], 2026)
        self.assertEqual(validated["exchange_rate_month"], 8)

    def test_excel_exchange_rate_import_is_disabled_by_default(self):
        validated = data_import_flow.validate_parameters({})
        self.assertFalse(validated["import_exchange_rates_from_excel"])

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
                "exchange_rate",
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

    def test_exchange_rate_period_defaults_and_validates_explicit_month(self):
        start, end = resolve_exchange_rate_period(reference_time=datetime(2026, 8, 18, 6, 0))
        self.assertEqual((start, end), (date(2026, 8, 1), date(2026, 8, 31)))
        self.assertEqual(
            resolve_exchange_rate_period(2024, 2),
            (date(2024, 2, 1), date(2024, 2, 29)),
        )
        with self.assertRaisesRegex(BusinessDataRefreshError, "同时指定"):
            resolve_exchange_rate_period(2026, None)

    def test_exchange_rate_api_filters_selected_month_and_currencies(self):
        session = FakeSession([[{"FRateID": 1}], []])
        with patch.dict(os.environ, {"XGD_TOKEN": "test-token"}, clear=False):
            rows = fetch_exchange_rate_rows(
                session,
                date(2026, 8, 1),
                date(2026, 8, 31),
                page_size=1,
                max_retries=0,
            )
        self.assertEqual(rows, [{"FRateID": 1}])
        payload = session.payloads[0]
        self.assertEqual(payload["FormId"], "BD_Rate")
        self.assertIn("FCyForID.FNumber = 'PRE003'", payload["FilterString"])
        self.assertIn("FCyForID.FNumber = 'PRE007'", payload["FilterString"])
        self.assertIn("FCyToID.FNumber = 'PRE001'", payload["FilterString"])
        self.assertIn("FBegDate >= '2026-08-01'", payload["FilterString"])
        self.assertIn("FBegDate <= '2026-08-31'", payload["FilterString"])

    def test_exchange_rate_normalization_matches_target_schema(self):
        base = {
            "FRateTypeID.FName": "固定汇率",
            "FCyToID.FNumber": "PRE001",
            "FCyToID.FName": "人民币",
            "FBegDate": "2026-08-01T00:00:00",
            "FEndDate": "2026-08-31T00:00:00",
            "FDocumentStatus": "C",
            "FForbidStatus": "A",
            "FIsSysPreset": False,
        }
        rows = normalize_exchange_rate_rows(
            [
                {
                    **base,
                    "FRateID": 1,
                    "FCyForID.FNumber": "PRE007",
                    "FCyForID.FName": "美元",
                    "FExchangeRate": 6.7894,
                    "FReverseExRate": 0.147288,
                },
                {
                    **base,
                    "FRateID": 2,
                    "FCyForID.FNumber": "PRE003",
                    "FCyForID.FName": "欧元",
                    "FExchangeRate": 7.7886,
                    "FReverseExRate": 0.128393,
                },
            ],
            date(2026, 8, 1),
            date(2026, 8, 31),
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["original_currency"] for row in rows}, {"美元", "欧元"})
        self.assertTrue(all(row["data_status"] == "已审核" for row in rows))
        self.assertTrue(all(row["disabled_status"] == "否" for row in rows))

    def test_exchange_rate_duplicate_business_key_is_rejected(self):
        row = {
            "FRateID": 1,
            "FRateTypeID.FName": "固定汇率",
            "FCyForID.FNumber": "PRE007",
            "FCyForID.FName": "美元",
            "FCyToID.FNumber": "PRE001",
            "FCyToID.FName": "人民币",
            "FBegDate": "2026-08-01",
            "FEndDate": "2026-08-31",
            "FExchangeRate": 6.7894,
            "FReverseExRate": 0.147288,
            "FDocumentStatus": "C",
            "FForbidStatus": "A",
            "FIsSysPreset": False,
        }
        with self.assertRaisesRegex(BusinessDataRefreshError, "业务键重复"):
            normalize_exchange_rate_rows(
                [row, {**row, "FRateID": 2}],
                date(2026, 8, 1),
                date(2026, 8, 31),
            )

    def test_exchange_rate_write_replaces_only_selected_month(self):
        rows = [
            {
                "direct_exchange_rate": 6.7894,
                "indirect_exchange_rate": 0.147288,
                "exchange_rate_type": "固定汇率",
                "original_currency": "美元",
                "target_currency": "人民币",
                "effective_date": date(2026, 8, 1),
                "expiration_date": date(2026, 8, 31),
                "data_status": "已审核",
                "disabled_status": "否",
                "system_preset": "否",
            }
        ]
        cursor = FakeCursor([(0, 323), (1, 323)])
        connection = FakeConnection(cursor)
        with patch(
            "modules.business_data_refresh.tasks.business_data_refresh_tasks._connect_finance",
            return_value=connection,
        ), patch(
            "modules.business_data_refresh.tasks.business_data_refresh_tasks.execute_values"
        ) as mocked_values:
            result = _replace_exchange_rate_month(
                rows,
                date(2026, 8, 1),
                date(2026, 8, 31),
            )
        delete_calls = [item for item in cursor.statements if item[0].startswith("DELETE")]
        self.assertEqual(
            delete_calls,
            [
                (
                    "DELETE FROM excel_exchange_rates WHERE effective_date BETWEEN %s AND %s",
                    (date(2026, 8, 1), date(2026, 8, 31)),
                )
            ],
        )
        mocked_values.assert_called_once()
        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)
        self.assertEqual(result["previous_month_rows"], 0)
        self.assertEqual(result["target_rows"], 1)

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
