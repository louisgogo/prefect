import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from modules.inventory_impairment.flows.inventory_impairment_flow import inventory_impairment_flow
from modules.inventory_impairment.tasks import inventory_impairment_tasks
from modules.inventory_impairment.tasks.inventory_impairment_tasks import (
    AGING_AMOUNT_COLUMNS,
    AGING_QUANTITY_COLUMNS,
    FACT_PROFIT_BD_COLUMNS,
    IN_TRANSIT_COLUMNS,
    calculate_impairment_detail,
    calculate_monthly_and_quarterly_impairment,
    get_default_inventory_impairment_period,
    get_quarter_periods,
    prepare_fact_profit_bd_rows,
    prepare_in_transit_detail,
    reconcile_quarterly_impairment,
    resolve_inventory_impairment_period,
    sync_inventory_impairment_via_platform,
)


def _inventory_row(**overrides):
    row = {
        "fin_con": "新国都技术",
        "fin_ind": "新国都技术",
        "inv_cat": "原材料",
        "cust_cat": "第三方",
        "warehouse": "原材料仓",
        "ref_amt": 600.0,
        "acct_period": pd.Timestamp("2026-06-01"),
    }
    row.update({column: 100.0 for column in AGING_AMOUNT_COLUMNS})
    row.update({column: 1.0 for column in AGING_QUANTITY_COLUMNS})
    row.update(overrides)
    return row


class InventoryImpairmentTests(unittest.TestCase):
    def test_default_period_is_most_recent_completed_quarter(self):
        self.assertEqual(get_default_inventory_impairment_period(date(2026, 7, 21)), (2026, 2))
        self.assertEqual(get_default_inventory_impairment_period(date(2027, 1, 2)), (2026, 4))

    def test_partial_explicit_period_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "同时提供"):
            resolve_inventory_impairment_period(2026, None)

    def test_quarter_periods_include_prior_quarter_end(self):
        periods, quarter_period = get_quarter_periods(2026, 1)

        self.assertEqual(
            periods,
            [
                pd.Timestamp("2025-12-01"),
                pd.Timestamp("2026-01-01"),
                pd.Timestamp("2026-02-01"),
                pd.Timestamp("2026-03-01"),
            ],
        )
        self.assertEqual(quarter_period, pd.Timestamp("2026-03-01"))

    def test_quarter_periods_reject_invalid_quarter(self):
        with self.assertRaisesRegex(ValueError, "quarter"):
            get_quarter_periods(2026, 5)

    def test_impairment_rule_matrix_and_precedence(self):
        df = pd.DataFrame(
            [
                _inventory_row(fin_con="中正"),
                _inventory_row(inv_cat="发出商品", cust_cat="第三方"),
                _inventory_row(inv_cat="发出商品", cust_cat="银行"),
                _inventory_row(warehouse="不良品仓"),
                _inventory_row(),
                _inventory_row(inv_cat="发出商品", cust_cat="第三方", warehouse="不良品仓"),
            ]
        )

        result = calculate_impairment_detail(df)

        self.assertEqual(
            result["impairment_balance"].tolist(), [0.0, 400.0, 250.0, 600.0, 380.0, 400.0]
        )

    def test_null_jialian_warehouse_maps_to_group_warehouse(self):
        result = calculate_impairment_detail(
            pd.DataFrame([_inventory_row(fin_con="嘉联", fin_ind="嘉联总部", warehouse=None)])
        )

        self.assertEqual(result.loc[0, "warehouse_class"], "集团仓库")

    def test_in_transit_amount_falls_back_to_order_date_and_uses_exchange_rate(self):
        row = {column: None for column in IN_TRANSIT_COLUMNS}
        row.update(
            {
                "fin_con": "嘉嘉电",
                "fin_ind": "嘉嘉电",
                "inv_cat": "原材料",
                "order_date": pd.Timestamp("2025-11-01"),
                "unit_price": 10.0,
                "exchange_rate": 2.0,
                "unreceived_inventory": 5.0,
                "acct_period": pd.Timestamp("2026-06-01"),
            }
        )

        result = prepare_in_transit_detail(pd.DataFrame([row]))

        self.assertEqual(result.loc[0, "transit_amount"], 100.0)
        self.assertEqual(result.loc[0, "amt_6_9m"], 100.0)
        self.assertEqual(result.loc[0, "warehouse_class"], "在途仓库")
        self.assertEqual(result.loc[0, "impairment_balance"], 30.0)
        self.assertEqual(result.loc[0, "aging_date_source"], "order_date")

    def test_in_transit_aging_prefers_delivery_date_over_order_date(self):
        row = {column: None for column in IN_TRANSIT_COLUMNS}
        row.update(
            {
                "fin_con": "新国都技术",
                "fin_ind": "新国都技术",
                "inv_cat": "原材料",
                "order_date": pd.Timestamp("2026-05-01"),
                "delivery_date": pd.Timestamp("2026-06-01") - pd.Timedelta(days=360),
                "unit_price": 100.0,
                "exchange_rate": 1.0,
                "unreceived_inventory": 1.0,
                "acct_period": pd.Timestamp("2026-06-01"),
            }
        )

        result = prepare_in_transit_detail(pd.DataFrame([row]))

        self.assertEqual(result.loc[0, "amt_1_2y"], 100.0)
        self.assertEqual(result.loc[0, "impairment_balance"], 100.0)
        self.assertEqual(result.loc[0, "aging_date_source"], "delivery_date")

    def test_in_transit_aging_uses_180_270_360_day_boundaries(self):
        rows = []
        for age_days in [179, 180, 269, 270, 359, 360]:
            row = {column: None for column in IN_TRANSIT_COLUMNS}
            row.update(
                {
                    "fin_con": "新国都技术",
                    "fin_ind": "新国都技术",
                    "inv_cat": "原材料",
                    "order_date": pd.Timestamp("2026-06-01"),
                    "delivery_date": pd.Timestamp("2026-06-01") - pd.Timedelta(days=age_days),
                    "unit_price": 100.0,
                    "exchange_rate": 1.0,
                    "unreceived_inventory": 1.0,
                    "acct_period": pd.Timestamp("2026-06-01"),
                }
            )
            rows.append(row)

        result = prepare_in_transit_detail(pd.DataFrame(rows))

        self.assertEqual(
            result["impairment_balance"].tolist(),
            [0.0, 30.0, 30.0, 50.0, 50.0, 100.0],
        )

    def test_quarter_amount_is_sum_of_monthly_balance_movements(self):
        periods, quarter_period = get_quarter_periods(2026, 2)
        balances = [100.0, 80.0, 70.0, 60.0]
        inventory_rows = []
        for period, balance in zip(periods, balances):
            inventory_rows.append(
                _inventory_row(
                    acct_period=period,
                    amt_6m_less=0.0,
                    amt_6_9m=0.0,
                    amt_9m_1y=0.0,
                    amt_1_2y=balance,
                    amt_2_3y=0.0,
                    amt_3y_plus=0.0,
                )
            )
        empty_in_transit = pd.DataFrame(columns=IN_TRANSIT_COLUMNS)

        result = calculate_monthly_and_quarterly_impairment(
            pd.DataFrame(inventory_rows), empty_in_transit, periods, quarter_period
        )

        self.assertEqual(result["monthly"]["mo_amt"].tolist(), [20.0, 10.0, 10.0])
        self.assertEqual(result["quarterly"].loc[0, "quarter_impairment_amount"], 40.0)
        self.assertEqual(result["quarterly"].loc[0, "prior_quarter_balance"], 100.0)
        self.assertEqual(result["quarterly"].loc[0, "quarter_end_balance"], 60.0)

    def test_reconciliation_reports_matches_and_missing_entities(self):
        calculated = pd.DataFrame(
            [
                {
                    "fin_ind": "嘉联",
                    "unique_lvl": "智造事业群-收单供应中心-公共部门",
                    "quarter_impairment_amount": 100.0,
                },
                {
                    "fin_ind": "新国都技术",
                    "unique_lvl": "智造事业群-智造管理中心-公共部门",
                    "quarter_impairment_amount": 200.0,
                },
            ]
        )
        recorded = pd.DataFrame(
            [
                {
                    "fin_ind": "嘉联",
                    "unique_lvl": "智造事业群-收单供应中心-公共部门",
                    "mo_amt": 100.0,
                },
                {
                    "fin_ind": "广州科技",
                    "unique_lvl": "智造事业群-智造管理中心-公共部门",
                    "mo_amt": 50.0,
                },
            ]
        )

        result = reconcile_quarterly_impairment(calculated, recorded)
        statuses = dict(zip(result["fin_ind"], result["status"]))

        self.assertEqual(statuses["嘉联"], "matched")
        self.assertEqual(statuses["新国都技术"], "missing_recorded")
        self.assertEqual(statuses["广州科技"], "missing_calculated")

    def test_zero_activity_entity_is_not_reported_as_quarter_adjustment(self):
        periods, quarter_period = get_quarter_periods(2026, 2)
        inventory_rows = [
            _inventory_row(
                fin_con="运服",
                fin_ind="运服",
                acct_period=period,
                amt_6m_less=0.0,
                amt_6_9m=0.0,
                amt_9m_1y=0.0,
                amt_1_2y=0.0,
                amt_2_3y=0.0,
                amt_3y_plus=0.0,
            )
            for period in periods
        ]

        result = calculate_monthly_and_quarterly_impairment(
            pd.DataFrame(inventory_rows),
            pd.DataFrame(columns=IN_TRANSIT_COLUMNS),
            periods,
            quarter_period,
        )

        self.assertTrue(result["quarterly"].empty)

    def test_fact_profit_rows_are_deterministic_quarter_summaries(self):
        quarterly = pd.DataFrame(
            [
                {
                    "fin_ind": "嘉联",
                    "unique_lvl": "智造事业群-收单供应中心-公共部门",
                    "prim_org": "智造事业群",
                    "sec_org": "收单供应中心",
                    "third_org": "公共部门",
                    "quarter_impairment_amount": 100.125,
                },
                {
                    "fin_ind": "新国都技术",
                    "unique_lvl": "智造事业群-智造管理中心-公共部门",
                    "prim_org": "智造事业群",
                    "sec_org": "智造管理中心",
                    "third_org": "公共部门",
                    "quarter_impairment_amount": 200.0,
                },
            ]
        )

        first = prepare_fact_profit_bd_rows(quarterly, pd.Timestamp("2026-06-01"))
        second = prepare_fact_profit_bd_rows(quarterly, pd.Timestamp("2026-06-01"))

        self.assertEqual(first.columns.tolist(), FACT_PROFIT_BD_COLUMNS)
        self.assertEqual(first["mo_amt"].tolist(), [100.13, 200.0])
        self.assertEqual(first["source_no"].tolist(), second["source_no"].tolist())
        self.assertTrue(first["source_no"].str.startswith("INVIMP-202606-").all())
        self.assertEqual(first["remarks"].unique().tolist(), ["存货跌价自动计算-2026Q2"])

    def test_platform_sync_posts_calculated_rows_with_internal_token(self):
        rows = self._prepared_write_rows()
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "code": 200,
            "data": {
                "date": "2026-06-01",
                "inserted": 1,
                "updated": 1,
                "repaired_links": 0,
                "deleted": 0,
            },
        }

        with (
            patch.dict(
                "os.environ",
                {
                    "AIHUB_PLATFORM_BASE_URL": "https://platform.example/api/v1",
                    "AIHUB_PLATFORM_API_TOKEN": "platform-token",
                },
                clear=False,
            ),
            patch.object(
                inventory_impairment_tasks.requests, "post", return_value=response
            ) as post,
        ):
            metrics = sync_inventory_impairment_via_platform(
                rows,
                pd.Timestamp("2026-06-01"),
            )

        self.assertEqual(metrics["updated"], 1)
        request = post.call_args
        self.assertEqual(
            request.args[0],
            "https://platform.example/api/v1/data-collect/business-report/"
            "inventory-impairment-sync",
        )
        self.assertEqual(request.kwargs["headers"], {"X-Internal-Token": "platform-token"})
        self.assertEqual(request.kwargs["json"]["quarter_period"], "2026-06-01")
        self.assertEqual(len(request.kwargs["json"]["rows"]), 2)
        self.assertTrue(
            all(
                row["source_no"].startswith("INVIMP-202606-")
                for row in request.kwargs["json"]["rows"]
            )
        )

    def test_platform_sync_fails_closed_without_service_token(self):
        with (
            patch.dict(
                "os.environ",
                {"AIHUB_PLATFORM_API_TOKEN": "", "XGD_TOKEN": ""},
                clear=False,
            ),
            self.assertRaisesRegex(RuntimeError, "AIHUB_PLATFORM_API_TOKEN 或 XGD_TOKEN"),
        ):
            sync_inventory_impairment_via_platform(
                self._prepared_write_rows(),
                pd.Timestamp("2026-06-01"),
            )

    @staticmethod
    def _prepared_write_rows():
        quarterly = pd.DataFrame(
            [
                {
                    "fin_ind": "嘉联",
                    "unique_lvl": "智造事业群-收单供应中心-公共部门",
                    "prim_org": "智造事业群",
                    "sec_org": "收单供应中心",
                    "third_org": "公共部门",
                    "quarter_impairment_amount": 100.13,
                },
                {
                    "fin_ind": "新国都技术",
                    "unique_lvl": "智造事业群-智造管理中心-公共部门",
                    "prim_org": "智造事业群",
                    "sec_org": "智造管理中心",
                    "third_org": "公共部门",
                    "quarter_impairment_amount": 200.0,
                },
            ]
        )
        return prepare_fact_profit_bd_rows(quarterly, pd.Timestamp("2026-06-01"))

    def test_flow_schema_uses_runtime_period_defaults_and_writes_by_default(self):
        self.assertEqual(inventory_impairment_flow.parameters.required, [])
        self.assertTrue(
            inventory_impairment_flow.parameters.properties["write_to_fact_profit_bd"]["default"]
        )


if __name__ == "__main__":
    unittest.main()
