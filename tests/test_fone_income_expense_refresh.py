import importlib
import json
import unittest
from unittest.mock import MagicMock, patch

from modules.recon.tasks.fone_income_expense_tasks import (
    _compile_fone_detail_script,
    _parse_fone_content_response,
    _validate_fone_detail_table_state,
    resolve_fone_detail_refresh_parameters,
)


def _script_definition(script_text=None):
    return {
        "variables": [
            {"name": "实际数月"},
            {"name": "实际数年"},
            {"name": "数据流", "id": "stream-id"},
            {"name": "报表范围", "type": "enum", "value": "费用明细"},
        ],
        "scriptText": script_text or ("run(@实际数月@, @实际数年@, @数据流@);" "var userID = user.account;"),
    }


def _income_state(row_count=10, id_min=101, id_max=110, period="2026-05-01"):
    return {
        "detail_type": "income",
        "tables": {
            "FONE_MRPT_AC_OffLineFormat": {
                "row_count": row_count,
                "id_min": id_min,
                "id_max": id_max,
                "distinct_periods": 1,
                "period_min": period,
                "period_max": period,
            }
        },
    }


def _expense_state():
    return {
        "detail_type": "expense",
        "tables": {
            "FONE_MRPT_FY_OffLineFormat": {
                "row_count": 20,
                "id_min": 201,
                "id_max": 220,
                "distinct_periods": 1,
                "period_min": "2026-05-01",
                "period_max": "2026-05-01",
            },
            "FONE_MRPT_FY_OffLineDetail": {
                "row_count": 15,
                "id_min": 301,
                "id_max": 315,
                "distinct_periods": 1,
                "period_min": "2026-M5",
                "period_max": "2026-M5",
            },
        },
    }


class FoneContentCompilationTests(unittest.TestCase):
    def test_parse_nested_fone_content_definition(self):
        definition = _script_definition()
        response = {"isSuccess": True, "data": {"data": json.dumps(definition)}}

        self.assertEqual(_parse_fone_content_response(response), definition)

    def test_parse_rejects_invalid_nested_json(self):
        response = {"isSuccess": True, "data": {"data": "not-json"}}

        with self.assertRaisesRegex(RuntimeError, "有效 JSON"):
            _parse_fone_content_response(response)

    def test_compile_sets_period_ids_and_permission_user(self):
        script = _compile_fone_detail_script(_script_definition(), 2026, 5, "finance-user")

        self.assertIn('var 实际数月="M5";', script)
        self.assertIn('var 实际数年="2026";', script)
        self.assertIn('var 数据流="stream-id";', script)
        self.assertIn("run(实际数月, 实际数年, 数据流);", script)
        self.assertIn('var userID = "finance-user";', script)
        self.assertNotIn("var userID = user.account;", script)
        self.assertNotIn("@实际数月@", script)
        self.assertNotIn("var 报表范围=", script)

    def test_compile_rejects_missing_permission_assignment(self):
        definition = _script_definition("run(@实际数月@, @实际数年@, @数据流@);")

        with self.assertRaisesRegex(RuntimeError, "实际出现 0 次"):
            _compile_fone_detail_script(definition, 2026, 5, "finance-user")

    def test_compile_rejects_ambiguous_permission_assignment(self):
        assignment = "var userID = user.account;"
        definition = _script_definition(f"run(@实际数月@, @实际数年@, @数据流@);{assignment}{assignment}")

        with self.assertRaisesRegex(RuntimeError, "实际出现 2 次"):
            _compile_fone_detail_script(definition, 2026, 5, "finance-user")

    def test_compile_rejects_unknown_marker(self):
        definition = _script_definition(
            "run(@实际数月@, @实际数年@, @数据流@, @未知变量@);" "var userID = user.account;"
        )

        with self.assertRaisesRegex(RuntimeError, "未知变量"):
            _compile_fone_detail_script(definition, 2026, 5, "finance-user")


class FoneRefreshValidationTests(unittest.TestCase):
    def test_period_requires_valid_month(self):
        with self.assertRaisesRegex(ValueError, "1-12"):
            resolve_fone_detail_refresh_parameters(2026, 13, "finance-user")

    def test_permission_user_is_required(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ValueError, "permission_user"):
                resolve_fone_detail_refresh_parameters(2026, 5)

    def test_income_state_accepts_requested_period_and_changed_ids(self):
        previous = _income_state(id_min=1, id_max=10)
        current = _income_state(id_min=11, id_max=20)

        result = _validate_fone_detail_table_state(
            "income", current, 2026, 5, previous_state=previous
        )

        self.assertIs(result, current)

    def test_income_state_rejects_empty_table(self):
        current = _income_state(row_count=0, id_min=None, id_max=None)

        with self.assertRaisesRegex(RuntimeError, "表为空"):
            _validate_fone_detail_table_state("income", current, 2026, 5)

    def test_income_state_rejects_wrong_period(self):
        current = _income_state(period="2026-04-01")

        with self.assertRaisesRegex(RuntimeError, "期间异常"):
            _validate_fone_detail_table_state("income", current, 2026, 5)

    def test_income_state_rejects_unchanged_signature(self):
        current = _income_state()

        with self.assertRaisesRegex(RuntimeError, "刷新前后状态未变化"):
            _validate_fone_detail_table_state(
                "income", current, 2026, 5, previous_state=_income_state()
            )

    def test_expense_state_checks_both_period_formats(self):
        current = _expense_state()

        result = _validate_fone_detail_table_state("expense", current, 2026, 5)

        self.assertIs(result, current)


class FoneRefreshFlowTests(unittest.TestCase):
    def test_flow_runs_income_before_expense(self):
        flow_module = importlib.import_module(
            "modules.recon.flows.fone_income_expense_refresh_flow"
        )
        state_task = MagicMock(side_effect=[_income_state(), _expense_state()])
        execute_task = MagicMock(
            side_effect=[
                {"detail_type": "income", "script_status": 0},
                {"detail_type": "expense", "script_status": 0},
            ]
        )
        validate_task = MagicMock(side_effect=[_income_state(), _expense_state()])

        with patch.multiple(
            flow_module,
            notify_hermes_task=MagicMock(),
            get_fone_detail_table_state_task=state_task,
            execute_fone_income_expense_script_task=execute_task,
            validate_fone_detail_table_state_task=validate_task,
        ):
            result = flow_module.fone_income_expense_refresh_flow.fn(
                year=2026,
                month=5,
                permission_user="finance-user",
            )

        detail_types = [call.kwargs["detail_type"] for call in execute_task.call_args_list]
        self.assertEqual(detail_types, ["income", "expense"])
        self.assertEqual(result["income_tables"]["FONE_MRPT_AC_OffLineFormat"]["row_count"], 10)
        self.assertEqual(result["expense_tables"]["FONE_MRPT_FY_OffLineDetail"]["row_count"], 15)


if __name__ == "__main__":
    unittest.main()
