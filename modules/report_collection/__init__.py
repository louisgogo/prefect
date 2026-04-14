"""报表数据收集模块

提供报表数据的收集、校对和上传功能。
"""

from .flows.report_collection_flow import report_collection_flow

__all__ = ["report_collection_flow"]
