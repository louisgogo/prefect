import importlib
import json
import unittest
from unittest.mock import patch

from modules.recon.tasks import fone_recon_tasks
from modules.recon.tasks.fone_income_expense_tasks import (
    FONE_DETAIL_SCRIPTS,
    execute_fone_income_expense_script_task,
)


class _Response:
    def __init__(self, data, status_code=200, text=""):
        self._data = data
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise fone_recon_tasks.requests.HTTPError(f"HTTP {self.status_code}", response=self)

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
        definition = {
            "variables": [
                {"name": "开始日期", "value": "2026-01-01"},
                {"name": "结束日期", "value": "2026-01-31"},
                {"name": "数据流", "id": "stream-id"},
            ],
            "scriptText": "run(@开始日期@, @结束日期@, @数据流@);",
        }
        content_response = _Response(
            {
                "status": 0,
                "isSuccess": True,
                "data": {
                    "appId": fone_recon_tasks.APP_ID,
                    "data": json.dumps(definition),
                },
            }
        )
        execute_response = _Response(
            {
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
        )
        with patch.dict(
            "os.environ", {"AIHUB_FONE_API_TOKEN": "proxy-token"}, clear=True
        ), patch.object(
            fone_recon_tasks.requests,
            "post",
            side_effect=[content_response, execute_response],
        ) as post_mock:
            result = fone_recon_tasks.execute_fone_recon_script_task.fn(
                start_date="2026-05-01",
                end_date="2026-05-31",
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
            self.assertNotIn("ticket", call.kwargs)
        payload = post_mock.call_args_list[1].kwargs["json"]
        self.assertIn('var 开始日期="2026-05-01";', payload["scriptText"])
        self.assertIn('var 结束日期="2026-05-31";', payload["scriptText"])
        self.assertIn('var 数据流="stream-id";', payload["scriptText"])
        self.assertNotIn("@开始日期@", payload["scriptText"])
        self.assertNotIn("appUserId", payload)
        self.assertEqual(payload["operateSourceName"], "Prefect-往来数据")
        self.assertEqual(payload["from"], "report")
        self.assertEqual(result["script_status"], 0)
        self.assertEqual(result["console_log_count"], 1)
        self.assertTrue(result["task_id"].startswith("script_prefect_recon_"))
        self.assertNotIn("console_logs", result)

    def test_compile_recon_script_rejects_missing_date_marker(self):
        definition = {
            "variables": [{"name": "开始日期", "value": "2026-01-01"}],
            "scriptText": "run(@开始日期@);",
        }

        with self.assertRaisesRegex(RuntimeError, "结束日期"):
            fone_recon_tasks._compile_fone_recon_script(
                definition,
                start_date="2026-05-01",
                end_date="2026-05-31",
            )

    def test_recon_http_error_keeps_safe_service_message(self):
        definition = {
            "variables": [
                {"name": "开始日期", "value": "2026-01-01"},
                {"name": "结束日期", "value": "2026-01-31"},
            ],
            "scriptText": "run(@开始日期@, @结束日期@);",
        }
        content_response = _Response(
            {
                "status": 0,
                "isSuccess": True,
                "data": {
                    "appId": fone_recon_tasks.APP_ID,
                    "data": json.dumps(definition),
                },
            }
        )
        execute_response = _Response(
            {"status": 400, "message": "script validation failed"}, status_code=400
        )

        with patch.dict(
            "os.environ", {"AIHUB_FONE_API_TOKEN": "proxy-token"}, clear=True
        ), patch.object(
            fone_recon_tasks.requests,
            "post",
            side_effect=[content_response, execute_response],
        ):
            with self.assertRaisesRegex(
                RuntimeError, "HTTP 400.*script validation failed"
            ) as raised:
                fone_recon_tasks.execute_fone_recon_script_task.fn(
                    start_date="2026-05-01",
                    end_date="2026-05-31",
                )

        self.assertNotIn("run(开始日期", str(raised.exception))
        self.assertNotIn("proxy-token", str(raised.exception))

    def test_detail_task_reads_and_executes_through_aihub_proxy(self):
        definition = {
            "variables": [
                {"name": "实际数月"},
                {"name": "实际数年"},
                {"name": "数据流", "id": "stream-id"},
            ],
            "scriptText": """var userID = user.account;
try{
run(@实际数月@, @实际数年@, @数据流@);
//08-通过数据中心生成Excel-Beg
generateExcel();
//10-程序锁-解锁，清空表数据
releaseLock();
//11-生成操作日志
writeDeliveryLog();
}catch(e){//异常捕获End
releaseLock();
throw e;
}
""",
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
        execution_payload = post_mock.call_args_list[1].kwargs["json"]
        self.assertNotIn("appUserId", execution_payload)
        executed_script = execution_payload["scriptText"]
        self.assertIn("run(实际数月, 实际数年, 数据流);", executed_script)
        self.assertIn("releaseLock();", executed_script)
        self.assertNotIn("generateExcel();", executed_script)
        self.assertNotIn("writeDeliveryLog();", executed_script)
        self.assertEqual(result["detail_type"], "income")
        self.assertEqual(result["script_status"], 0)
        self.assertEqual(FONE_DETAIL_SCRIPTS["income"]["content_id"], "661b866863556863c96d4bbf")


if __name__ == "__main__":
    unittest.main()
