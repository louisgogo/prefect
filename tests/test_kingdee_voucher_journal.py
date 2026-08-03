import importlib
import unittest
import uuid
from datetime import date
from unittest.mock import MagicMock, call, patch

import requests

from modules.kingdee_voucher.tasks import kingdee_voucher_tasks as task_module
from modules.kingdee_voucher.tasks.kingdee_voucher_tasks import (
    KingdeeVoucherSyncError,
    _boolean,
    _request_page,
    _resolve_token,
    _start_run,
    _upsert_page,
    resolve_voucher_months,
    sync_kingdee_voucher_period_task,
)

flow_module = importlib.import_module("modules.kingdee_voucher.flows.kingdee_voucher_journal_flow")


def _source_row(entry_id, month=8):
    return {
        "FEntity_FEntryID": entry_id,
        "FBillNo": f"记-{entry_id}",
        "FDate": "2026-08-31T00:00:00",
        "FYEAR": 2026,
        "FPERIOD": month,
        "FModifyDate": "2026-08-31T12:00:00",
        "FISADJUSTVOUCHER": False,
        "FISCASHFLOW": True,
        "FISREDWRITEOFF": "false",
        "FCASHFLOWITEM": "1",
        "FISMULTICOLLECT": "0",
    }


class VoucherParameterTests(unittest.TestCase):
    def test_quick_run_defaults_to_previous_calendar_month(self):
        self.assertEqual(
            flow_module._get_kingdee_voucher_defaults_by_date(date(2026, 8, 3)),
            {"year": 2026, "month": 7, "page_size": 5000},
        )

    def test_quick_run_defaults_cross_year_boundary(self):
        self.assertEqual(
            flow_module._get_kingdee_voucher_defaults_by_date(date(2026, 1, 15)),
            {"year": 2025, "month": 12, "page_size": 5000},
        )

    def test_requires_exactly_one_month_input(self):
        with self.assertRaisesRegex(ValueError, "必须显式填写"):
            resolve_voucher_months(year=2026)
        with self.assertRaisesRegex(ValueError, "只能填写一个"):
            resolve_voucher_months(year=2026, month=8, months=[8])

    def test_month_list_is_sorted_and_deduplicated(self):
        self.assertEqual(
            resolve_voucher_months(year=2026, months=[8, 1, 8, 3]),
            (2026, [1, 3, 8], 5000),
        )

    def test_month_13_is_supported_but_later_months_are_rejected(self):
        self.assertEqual(resolve_voucher_months(2026, month=13)[1], [13])
        with self.assertRaisesRegex(ValueError, "1-13"):
            resolve_voucher_months(2026, month=14)


class VoucherNormalizationTests(unittest.TestCase):
    def test_boolean_flags_accept_kingdee_json_and_string_values(self):
        self.assertIs(_boolean(True), True)
        self.assertIs(_boolean(False), False)
        self.assertIs(_boolean("1"), True)
        self.assertIs(_boolean("false"), False)
        with self.assertRaises(KingdeeVoucherSyncError):
            _boolean("unexpected")

    def test_deduplicated_upsert_counts_existing_rows_as_updates(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [(1,)]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        records = [
            {"source_entry_id": 1},
            {"source_entry_id": 2},
            {"source_entry_id": 2},
        ]

        with patch.object(task_module, "execute_batch") as execute_batch:
            inserted, updated = _upsert_page(connection, records)

        self.assertEqual((inserted, updated), (1, 1))
        written_records = execute_batch.call_args.args[2]
        self.assertEqual({item["source_entry_id"] for item in written_records}, {1, 2})
        self.assertIn("ON CONFLICT (source_entry_id) DO UPDATE", execute_batch.call_args.args[1])


class VoucherRequestTests(unittest.TestCase):
    def test_request_uses_stable_pagination_and_does_not_leak_token(self):
        token = "sensitive-token-value"
        response = MagicMock()
        response.status_code = 401
        response.raise_for_status.side_effect = requests.HTTPError(
            f"401 unauthorized for bearer {token}"
        )
        session = MagicMock()
        session.post.return_value = response

        with self.assertRaises(KingdeeVoucherSyncError) as raised:
            _request_page(
                session,
                token=token,
                year=2026,
                month=8,
                start_row=5000,
                page_size=5000,
                timeout_seconds=1,
                max_retries=0,
            )

        self.assertNotIn(token, str(raised.exception))
        request_kwargs = session.post.call_args.kwargs
        self.assertEqual(request_kwargs["json"]["StartRow"], 5000)
        self.assertEqual(request_kwargs["json"]["Limit"], 5000)
        self.assertEqual(request_kwargs["json"]["OrderString"], "FEntity_FEntryID ASC")

    def test_token_prefers_dedicated_environment_variable(self):
        with patch.dict(
            "os.environ",
            {"XGD_TOKEN": "dedicated", "AIHUB_FONE_API_TOKEN": "fallback"},
            clear=True,
        ):
            self.assertEqual(_resolve_token(), "dedicated")


class VoucherPeriodTaskTests(unittest.TestCase):
    def test_start_run_passes_uuid_as_psycopg2_compatible_text(self):
        run_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        with patch.object(task_module.uuid, "uuid4", return_value=run_id):
            self.assertEqual(_start_run(connection, 2026, 8, 5000), run_id)

        insert_params = cursor.execute.call_args_list[1].args[1]
        self.assertEqual(insert_params[0], str(run_id))

    def test_period_task_advances_pages_and_commits_progress(self):
        connection = MagicMock()
        session = MagicMock()
        logger = MagicMock()
        first_page = [_source_row(1), _source_row(2)]
        second_page = [_source_row(3)]

        with (
            patch.object(task_module, "_resolve_token", return_value="secret"),
            patch.object(task_module, "_connect_database", return_value=connection),
            patch.object(task_module.requests, "Session", return_value=session),
            patch.object(task_module, "get_run_logger", return_value=logger),
            patch.object(task_module, "_start_run", return_value="run-id"),
            patch.object(
                task_module,
                "_request_page",
                side_effect=[first_page, second_page, []],
            ) as request_page,
            patch.object(task_module, "_upsert_page", side_effect=[(2, 0), (1, 0)]),
            patch.object(task_module, "_update_run_progress") as update_progress,
            patch.object(task_module, "_complete_run") as complete_run,
        ):
            result = sync_kingdee_voucher_period_task.fn(
                year=2026,
                month=8,
                page_size=2,
            )

        self.assertEqual(
            [item.kwargs["start_row"] for item in request_page.call_args_list],
            [0, 2, 3],
        )
        self.assertEqual(result["source_rows"], 3)
        self.assertEqual(result["inserted_rows"], 3)
        self.assertEqual(update_progress.call_count, 2)
        complete_run.assert_called_once_with(connection, "run-id")
        self.assertEqual(connection.commit.call_count, 2)
        session.close.assert_called_once_with()
        connection.close.assert_called_once_with()


class VoucherFlowTests(unittest.TestCase):
    def test_flow_runs_month_tasks_in_sorted_order(self):
        logger = MagicMock()

        def result_for_month(year, month, page_size):
            return {
                "year": year,
                "month": month,
                "source_rows": month,
                "inserted_rows": 0,
                "updated_rows": month,
            }

        period_task = MagicMock(side_effect=result_for_month)
        with (
            patch.object(flow_module, "sync_kingdee_voucher_period_task", period_task),
            patch.object(flow_module, "notify_hermes_task") as notify,
            patch.object(flow_module, "get_run_logger", return_value=logger),
        ):
            result = flow_module.kingdee_voucher_journal_flow.fn(
                year=2026,
                months=[8, 1, 3],
                page_size=1000,
            )

        self.assertEqual(
            period_task.call_args_list,
            [
                call(year=2026, month=1, page_size=1000),
                call(year=2026, month=3, page_size=1000),
                call(year=2026, month=8, page_size=1000),
            ],
        )
        self.assertEqual(result["months"], [1, 3, 8])
        self.assertEqual(result["source_rows"], 12)
        self.assertEqual(
            [item.kwargs["event"] for item in notify.call_args_list], ["started", "completed"]
        )


if __name__ == "__main__":
    unittest.main()
