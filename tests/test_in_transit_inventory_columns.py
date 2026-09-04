import unittest

import pandas as pd

from modules.bus_line_staging.tasks.asset_tasks import (
    calculate_in_transit_order_amount,
    select_in_transit_inventory_rows,
)
from modules.data_import.tasks.data_import_tasks import drop_unstored_columns


class InTransitInventoryColumnTests(unittest.TestCase):
    def test_staging_selection_uses_unreceived_quantity(self):
        source = pd.DataFrame(
            [
                {"业务线": "无", "未入库数量": 2, "订单金额": None},
                {"业务线": "小POS", "未入库数量": 0, "订单金额": 100},
                {"业务线": "其他", "未入库数量": 3, "订单金额": 100},
            ]
        )

        result = select_in_transit_inventory_rows(source)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["未入库数量"], 2)

    def test_in_transit_amount_uses_unreceived_quantity_price_and_exchange_rate(self):
        source = pd.DataFrame(
            [
                {"未入库数量": 5, "单价": 10, "汇率": 2},
                {"未入库数量": 3, "单价": 4, "汇率": None},
            ]
        )

        result = calculate_in_transit_order_amount(source)

        self.assertEqual(result.tolist(), [100.0, 12.0])

    def test_data_import_drops_unstored_legacy_columns(self):
        source = pd.DataFrame(
            [
                {
                    "order_amount": 100,
                    "total_payment_amount": 20,
                    "total_inventory_received": 4,
                    "order_count": 10,
                    "unit_price": 10,
                    "unreceived_inventory": 6,
                }
            ]
        )

        result = drop_unstored_columns("fact_inventory_on_way", source)

        self.assertNotIn("order_amount", result.columns)
        self.assertNotIn("total_payment_amount", result.columns)
        self.assertNotIn("total_inventory_received", result.columns)
        self.assertEqual(result.iloc[0]["order_count"], 10)


if __name__ == "__main__":
    unittest.main()
