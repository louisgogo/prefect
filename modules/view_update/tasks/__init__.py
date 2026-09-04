"""视图更新任务模块"""
from .view_update_tasks import (
    grant_fone_permissions_task,
    refresh_views_task,
    update_map_translate_task,
)

__all__ = [
    "update_map_translate_task",
    "refresh_views_task",
    "grant_fone_permissions_task",
]
