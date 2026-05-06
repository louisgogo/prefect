"""Prefect 模块包 - 统一导出所有 flows"""
# AI数据ETL流程
from .ai_data_etl.flows.ai_data_etl_flow import ai_data_etl_flow

# 业务线损益计算流程（业务线数据计算+利润表刷新）
# 预算更新流程
from .budget_update.flows.budget_update_flow import budget_update_flow
from .bus_line_cal.flows.business_line_profit_flow import business_line_profit_flow

# 数据导入流程
from .data_import.flows.data_import_flow import data_import_flow

# 利润表刷新流程
from .profit_refresh.flows.profit_refresh_flow import profit_refresh_flow

# 往来对账流程
from .recon.flows.recon_flow import recon_flow

# 报表数据收集流程
from .report_collection.flows.profit_report_flow import profit_report_flow
from .report_collection.flows.report_collection_flow import report_collection_flow
from .shared_rate.flows.fetch_budget_shared_rate_flow import fetch_budget_shared_rate_flow

# 综合比例计算流程（独立流程）
from .shared_rate.flows.shared_rate_flow import calculate_shared_rate_flow

__all__ = [
    "ai_data_etl_flow",
    "business_line_profit_flow",
    "calculate_shared_rate_flow",
    "fetch_budget_shared_rate_flow",
    "data_import_flow",
    "budget_update_flow",
    "profit_refresh_flow",
    "recon_flow",
    "report_collection_flow",
    "profit_report_flow",
]
