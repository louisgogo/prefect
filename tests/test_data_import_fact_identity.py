import unittest
import uuid
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pandas as pd

from modules.data_import.tasks import data_import_tasks
from modules.data_import.tasks.data_import_tasks import (
    merge_fact_identity_columns,
    replace_linked_fact_rows,
    verify_fact_identity_rows,
)


class DataImportFactIdentityTests(unittest.TestCase):
    def test_manual_refresh_skips_platform_managed_business_report_facts(self):
        platform_managed_facts = {
            "fact_revenue": pd.DataFrame(
                [{"source_no": "R1", "acct_period": "2026-07-01", "amt": 10}]
            ),
            "fact_expense": pd.DataFrame(
                [{"source_no": "E1", "acct_period": "2026-07-01", "amt": 20}]
            ),
            "fact_profit_bd": pd.DataFrame([{"source_no": "P1", "date": "2026-07-01", "amt": 30}]),
            "fact_receivable": pd.DataFrame(
                [{"source_no": "A1", "acct_period": "2026-07-01", "amt": 40}]
            ),
            "fact_inventory": pd.DataFrame(
                [{"source_no": "I1", "acct_period": "2026-07-01", "amt": 50}]
            ),
            "fact_inventory_on_way": pd.DataFrame(
                [{"source_no": "W1", "acct_period": "2026-07-01", "amt": 60}]
            ),
        }

        with patch.object(data_import_tasks, "update_data_by_date_range_task") as update:
            data_import_tasks.update_manual_refresh_data_task.fn(
                platform_managed_facts,
                "2026-07-01",
                "2026-07-31",
                True,
            )

        update.assert_not_called()

    def test_merge_preserves_existing_id_and_staging_link(self):
        linked_id = uuid.uuid4()
        incoming = pd.DataFrame(
            [
                {"source_no": "R1", "acct_period": "2026-07-01", "amt": 10},
                {"source_no": "R2", "acct_period": "2026-07-01", "amt": 20},
            ]
        )
        existing = pd.DataFrame(
            [
                {
                    "id": 101,
                    "source_no": "R1",
                    "business_report_staging_id": linked_id,
                }
            ]
        )

        result = merge_fact_identity_columns(incoming, existing, [202])

        self.assertEqual(result["id"].tolist(), [101, 202])
        self.assertEqual(result.iloc[0]["business_report_staging_id"], linked_id)
        self.assertIsNone(result.iloc[1]["business_report_staging_id"])

    def test_merge_rejects_duplicate_source_numbers(self):
        incoming = pd.DataFrame([{"source_no": "R1"}, {"source_no": "R1"}])
        existing = pd.DataFrame(columns=["id", "source_no", "business_report_staging_id"])

        with self.assertRaisesRegex(ValueError, "重复 source_no"):
            merge_fact_identity_columns(incoming, existing, [1, 2])

    def test_verification_detects_lost_staging_link(self):
        linked_id = uuid.uuid4()
        expected = pd.DataFrame(
            [{"id": 101, "source_no": "R1", "business_report_staging_id": linked_id}]
        )
        actual = pd.DataFrame([{"id": 101, "source_no": "R1", "business_report_staging_id": None}])

        with self.assertRaisesRegex(RuntimeError, "身份关联校验失败"):
            verify_fact_identity_rows(expected, actual)

    def test_replace_task_uses_atomic_link_preserving_replacement(self):
        source = pd.DataFrame([{"source_no": "R1", "acct_period": "2026-07-01", "amt": 10}])

        with (
            patch.object(data_import_tasks, "replace_linked_fact_rows") as replace_linked,
            patch.object(data_import_tasks, "update_between_dates") as replace,
        ):
            data_import_tasks.update_data_by_date_range_task.fn(
                "fact_revenue",
                "acct_period",
                source,
                "acct_period",
                "2026-07-01",
                "2026-07-31",
                True,
            )

        replace_linked.assert_called_once()
        replace.assert_not_called()

    def test_atomic_replacement_deletes_and_inserts_on_same_connection(self):
        linked_id = uuid.uuid4()
        incoming = pd.DataFrame([{"source_no": "R1", "acct_period": "2026-07-01", "amt": 10}])
        identity = pd.DataFrame(
            [{"id": 101, "source_no": "R1", "business_report_staging_id": linked_id}]
        )
        connection = MagicMock()
        engine = MagicMock()
        engine.begin.return_value = nullcontext(connection)

        with (
            patch.object(data_import_tasks, "create_engine", return_value=engine),
            patch.object(data_import_tasks, "url_to_db", return_value="postgresql://test"),
            patch.object(
                data_import_tasks,
                "_read_fact_identity_rows",
                side_effect=[identity, identity],
            ),
            patch.object(pd.DataFrame, "to_sql") as to_sql,
        ):
            replace_linked_fact_rows(
                "fact_revenue",
                "acct_period",
                incoming,
                "acct_period",
                "2026-07-01",
                "2026-07-31",
            )

        self.assertIn("DELETE FROM fact_revenue", str(connection.execute.call_args.args[0]))
        self.assertIs(to_sql.call_args.kwargs["con"], connection)


if __name__ == "__main__":
    unittest.main()
