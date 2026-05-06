"""AI数据ETL任务"""

from .ai_data_etl_tasks import create_ai_view_task, create_revenue_calc_view_task

__all__ = [
    "create_revenue_calc_view_task",
    "create_ai_view_task",
]
