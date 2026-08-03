"""Prefect 模块包 - 统一导出所有 flows"""

# AI数据ETL流程
from .ai_data_etl.flows.ai_data_etl_flow import ai_data_etl_flow

# 业务线损益计算流程（业务线数据计算+利润表刷新）
# 预算更新流程
from .budget_update.flows.budget_update_flow import budget_update_flow
from .bus_line_cal.flows.business_line_profit_flow import business_line_profit_flow

# 数据导入流程
from .data_import.flows.data_import_flow import data_import_flow

# 存货跌价季度计算子流程
from .inventory_impairment.flows.inventory_impairment_flow import inventory_impairment_flow

# 金蝶凭证序时簿同步流程
from .kingdee_voucher.flows.kingdee_voucher_journal_flow import kingdee_voucher_journal_flow

# 组织架构同步对比流程
from .org_sync.flows.org_sync_flow import org_sync_flow

# 利润表刷新流程
from .profit_refresh.flows.profit_refresh_flow import profit_refresh_flow

# 研发项目收益分析流程
from .rd_project_profitability.flows.rd_project_profitability_flow import (
    rd_project_profitability_flow,
)

# 往来对账流程
from .recon.flows.fone_income_expense_refresh_flow import fone_income_expense_refresh_flow
from .recon.flows.fone_recon_flow import fone_recon_flow
from .recon.flows.recon_flow import recon_flow
from .recon.flows.staging_recon_flow import staging_recon_flow

# 报表数据收集流程
from .report_collection.flows.profit_report_flow import profit_report_flow
from .report_collection.flows.report_collection_flow import report_collection_flow
from .shared_rate.flows.fetch_budget_shared_rate_flow import fetch_budget_shared_rate_flow

# 综合比例计算流程（独立流程）
from .shared_rate.flows.shared_rate_flow import calculate_shared_rate_flow

# 视图更新流程
from .view_update.flows.view_update_flow import view_update_flow

__all__ = [
    "ai_data_etl_flow",
    "business_line_profit_flow",
    "calculate_shared_rate_flow",
    "fetch_budget_shared_rate_flow",
    "data_import_flow",
    "inventory_impairment_flow",
    "kingdee_voucher_journal_flow",
    "budget_update_flow",
    "org_sync_flow",
    "profit_refresh_flow",
    "rd_project_profitability_flow",
    "fone_income_expense_refresh_flow",
    "fone_recon_flow",
    "recon_flow",
    "staging_recon_flow",
    "report_collection_flow",
    "profit_report_flow",
    "view_update_flow",
]
