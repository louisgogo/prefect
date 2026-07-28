import importlib
import json
import unittest
from unittest.mock import MagicMock, patch

from modules.recon.tasks import fone_recon_tasks
from modules.recon.tasks.fone_income_expense_tasks import (
    FONE_DETAIL_SCRIPTS,
    execute_fone_income_expense_script_task,
)


class _Response:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def raise_for_status(self):
        return None

    def json(self):
        return self._data


class FoneProxyAuthTests(unittest.TestCase):
    def test_proxy_token_is_required(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "AIHUB_FONE_API_TOKEN"):
                fone_recon_tasks._get_fone_proxy_token()

    def test_proxy_headers_use_bearer_token(self):
        with patch.dict("os.environ", {"AIHUB_FONE_API_TOKEN": "proxy-token"}, clear=True):
            headers = fone_recon_tasks._fone_proxy_headers()

        self.assertEqual(headers["Authorization"], "Bearer proxy-token")
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_recon_task_calls_aihub_proxy_without_ticket(self):
        response_data = {
            "status": 0,
            "isSuccess": True,
            "data": json.dumps(
                {
                    "status": 0,
                    "consoleLogs": ["one"],
                    "warningMessage": [],
                    "errorMessage": [],
                }
            ),
        }
        with patch.dict(
            "os.environ", {"AIHUB_FONE_API_TOKEN": "proxy-token"}, clear=True
        ), patch.object(
            fone_recon_tasks.requests,
            "post",
            return_value=_Response(response_data),
        ) as post_mock, patch.object(
            fone_recon_tasks,
            "_build_script_text",
            return_value="compiled-script",
        ):
            result = fone_recon_tasks.execute_fone_recon_script_task.fn(
                start_date="2026-05-01",
                end_date="2026-05-31",
            )

        call = post_mock.call_args
        self.assertEqual(
            call.args[0],
            "https://aihub.xgd.com/api/proxy/fone/api/Script/ExcuteScriptText",
        )
        self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer proxy-token")
        self.assertNotIn("ticket", call.kwargs)
        self.assertEqual(result["script_status"], 0)
        self.assertEqual(result["console_log_count"], 1)
        self.assertNotIn("console_logs", result)

    def test_detail_task_reads_and_executes_through_aihub_proxy(self):
        definition = {
            "variables": [
                {"name": "实际数月"},
                {"name": "实际数年"},
                {"name": "数据流", "id": "stream-id"},
            ],
            "scriptText": ("run(@实际数月@, @实际数年@, @数据流@);" "var userID = user.account;"),
        }
        content_response = _Response(
            {
                "status": 0,
                "isSuccess": True,
                "data": {"data": json.dumps(definition)},
            }
        )
        execute_response = _Response(
            {
                "status": 0,
                "isSuccess": True,
                "data": json.dumps(
                    {
                        "status": 0,
                        "consoleLogs": [],
                        "warningMessage": [],
                        "errorMessage": [],
                    }
                ),
            }
        )
        task_module = importlib.import_module("modules.recon.tasks.fone_income_expense_tasks")
        with patch.dict(
            "os.environ", {"AIHUB_FONE_API_TOKEN": "proxy-token"}, clear=True
        ), patch.object(
            task_module.requests,
            "post",
            side_effect=[content_response, execute_response],
        ) as post_mock:
            result = execute_fone_income_expense_script_task.fn(
                detail_type="income",
                year=2026,
                month=5,
                permission_user="finance-user",
            )

        urls = [call.args[0] for call in post_mock.call_args_list]
        self.assertEqual(
            urls,
            [
                "https://aihub.xgd.com/api/proxy/fone/api/FContent/GetFContent",
                "https://aihub.xgd.com/api/proxy/fone/api/Script/ExcuteScriptText",
            ],
        )
        for call in post_mock.call_args_list:
            self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer proxy-token")
        self.assertEqual(result["detail_type"], "income")
        self.assertEqual(result["script_status"], 0)
        self.assertEqual(FONE_DETAIL_SCRIPTS["income"]["content_id"], "661b866863556863c96d4bbf")


if __name__ == "__main__":
    unittest.main()
