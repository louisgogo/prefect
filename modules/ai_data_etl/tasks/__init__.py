"""AI数据ETL任务"""

from .ai_data_etl_tasks import create_ai_view_task, create_revenue_calc_view_task
from .budget_profit_calc_tasks import (
    calculate_budget_profit_indicators_task,
    create_budget_profit_view_task,
    load_budget_data_task,
    merge_budget_data_task,
    save_budget_profit_task,
)

__all__ = [
    "create_revenue_calc_view_task",
    "create_ai_view_task",
    "load_budget_data_task",
    "merge_budget_data_task",
    "calculate_budget_profit_indicators_task",
    "save_budget_profit_task",
    "create_budget_profit_view_task",
]
