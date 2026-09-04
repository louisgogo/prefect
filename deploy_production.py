"""生产环境部署脚本（带计划执行）"""

import os
import sys
from datetime import datetime

from prefect.client.schemas.schedules import CronSchedule

from modules import (
    budget_update_flow,
    business_data_refresh_flow,
    business_line_profit_flow,
    calculate_shared_rate_flow,
    data_import_flow,
    fetch_budget_shared_rate_flow,
    fone_income_expense_refresh_flow,
    inventory_impairment_flow,
    kingdee_voucher_journal_flow,
    profit_refresh_flow,
    rd_project_profitability_flow,
)
from modules.bus_line_staging import bus_line_staging_flow
from modules.bus_line_staging.module_selection import ALL_MODULE_OPTIONS

# 添加当前目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


if __name__ == "__main__":
    print("=" * 60)
    print("业务线数据计算流程 - 生产环境部署")
    print("=" * 60)

    # 计算自动运行日期范围：1月到上个月；1月份则为上年全部
    now = datetime.now()
    if now.month == 1:
        process_year = now.year - 1
        months = list(range(1, 13))
    else:
        process_year = now.year
        months = list(range(1, now.month))

    print(f"默认参数：year={process_year}, months={months}")

    # 后续流程（综合比例、数据导入）默认使用上个月
    last_month_year = process_year
    last_month = months[-1]

    # 部署 flow 到 Prefect server（带计划执行）
    business_line_profit_flow.serve(
        name="主流程-业务线损益计算",
        parameters={
            "year": process_year,
            "months": months,
        },
        tags=["业务线核算", "月度任务", "自动执行"],
        description="业务线损益计算流程：生成收入、费用、利润、应收、存货、在途存货明细表，并刷新利润表",
    )

    print("\n" + "=" * 60)
    print("综合比例计算流程 - 生产环境部署")
    print("=" * 60)

    print(f"默认参数：year={last_month_year}, month={last_month}")
    print("计划执行：每月1号凌晨3点自动执行（处理上个月数据，在业务线数据计算后）")

    # 部署综合比例计算流程到 Prefect server（带计划执行）
    calculate_shared_rate_flow.serve(
        name="主流程-综合比例年底重算",
        parameters={
            "year": last_month_year,
            "month": last_month,
        },
        tags=["业务线核算", "月度任务", "自动执行", "综合比例"],
        description="综合比例计算流程：计算业务线综合比例（收入、毛利润、净利润、人数的加权平均）",
    )

    print("\n" + "=" * 60)
    print("拉取预算综合比例流程 - 生产环境部署")
    print("=" * 60)
    print("计划执行：随时手工或定时执行，处理年初至上月范围的预算比例")
    fetch_budget_shared_rate_flow.serve(
        name="子流程-拉取预算综合比例",
        tags=["预算更新", "手动触发", "自动执行", "综合比例"],
        description="获取预算表中最新1号的综合比例，并写入业务线实际比例表中覆盖年初至上月底。",
    )

    print("\n" + "=" * 60)
    print("数据导入流程 - 生产环境部署")
    print("=" * 60)

    print(f"默认参数：使用上个月数据（{last_month_year}年{last_month}月）")
    print("说明：默认不替换已存在的数据；业务数据板块默认替换，Excel 汇率导入关闭")

    # 部署数据导入流程到 Prefect server（带计划执行）
    data_import_flow.serve(
        name="主流程-数据导入",
        parameters={
            "year": last_month_year,
            "month": last_month,
            "replace_existing": False,
            "import_exchange_rates_from_excel": False,
        },
        tags=["数据导入", "月度任务", "自动执行"],
        description="数据导入流程：从 Excel 文件导入业务数据；汇率默认不再从 Excel 写入，由金蝶基础数据流程更新。",
    )

    print("\n" + "=" * 60)
    print("预算更新流程 - 生产环境部署")
    print("=" * 60)
    from modules.budget_update.flows.budget_update_flow import _get_budget_defaults_by_date

    budget_defaults = _get_budget_defaults_by_date()
    print("说明：预算更新为手动触发；参数已按当前月份设默认值（11月～2月→年初预算，4月～7月→年中预算）")
    print("预算版本保存规则：")
    print("  - 正式版日期自动生成：年初预算为 YYYY-01-01，年中预算为 YYYY-07-01")
    print("  - save_previous_version=false（默认）：快速执行直接覆盖1日正式版")
    print("  - save_previous_version=true：当前正式版自动归档到同月2日、3日等空闲日期")
    budget_update_flow.serve(
        name="主流程-预算更新",
        tags=["预算更新", "手动触发"],
        description=(
            "从 FONE 拉取预算、严格映射检查并写库。正式版日期自动生成：年初为 YYYY-01-01，"
            "年中为 YYYY-07-01。save_previous_version 默认关闭并直接覆盖；手动开启时归档当前正式版。"
        ),
        parameters=budget_defaults,
    )

    print("\n" + "=" * 60)
    print("FONE 收入费用明细刷新子流程 - 生产环境注册")
    print("=" * 60)
    print("说明：仅手工触发；year、month 必须显式填写，权限用户必须通过参数或环境变量提供。")
    fone_income_expense_refresh_flow.serve(
        name="子流程-FONE收入费用明细刷新",
        tags=["FONE", "收入明细", "费用明细", "财务刷新", "手动触发"],
        description="按显式年月顺序刷新 FONE 收入、费用明细，并回读数据库验证非空、期间和刷新状态。",
    )

    print("\n" + "=" * 60)
    print("金蝶凭证序时簿同步子流程 - 生产环境注册")
    print("=" * 60)
    from modules.kingdee_voucher.flows.kingdee_voucher_journal_flow import (
        _get_kingdee_voucher_defaults_by_date,
    )

    voucher_defaults = _get_kingdee_voucher_defaults_by_date()
    print(
        f"说明：快速执行默认同步 {voucher_defaults['year']} 年第 "
        f"{voucher_defaults['month']} 期；自定义时 month 和 months 必须且只能填写一个。"
    )
    kingdee_voucher_journal_flow.serve(
        name="子流程-金蝶凭证序时簿同步",
        tags=["金蝶", "凭证序时簿", "月度任务", "手动触发", "财务写入"],
        description="快速执行默认同步上个自然月；也可自定义单月或月份列表，按分录稳定标识幂等写库。",
        parameters=voucher_defaults,
    )

    print("\n" + "=" * 60)
    print("业报基础数据更新子流程 - 生产环境注册")
    print("=" * 60)
    print("计划执行：每天06:00（Asia/Shanghai）更新全部六类基础数据；也支持业报收集界面按数据集手工触发。")
    business_data_refresh_flow.serve(
        name="子流程-业报基础数据更新",
        schedule=CronSchedule(cron="0 6 * * *", timezone="Asia/Shanghai"),
        parameters={
            "datasets": [
                "customer",
                "material",
                "rd_project",
                "supplier",
                "acquiring_metrics",
                "exchange_rate",
            ],
            "requested_by": None,
            "exchange_rate_year": None,
            "exchange_rate_month": None,
        },
        tags=["业报收集", "基础数据", "每日任务", "手动触发", "财务写入"],
        description="每日06:00更新客户、物料、研发项目、供应商、收单指标和当月汇率；也支持业报编辑人员按数据集手工更新。",
    )

    print("\n" + "=" * 60)
    print("利润表刷新流程 - 生产环境部署")
    print("=" * 60)
    print(f"说明：利润表刷新通常在业务线损益计算流程最后一步自动调用。部署此任务是为了方便单独手动触发。")
    print("易混点：date_range参数目前只为了占位，通常使用手动触发时需要设置参数")
    profit_refresh_flow.serve(
        name="子流程-利润表刷新",
        tags=["业务线核算", "手动触发", "自动执行"],
        description="利润表刷新流程：处理所有已计算的月份数据，生成 fact_profit 和 fact_bus_profit 表",
    )

    print("\n" + "=" * 60)
    print("季度存货跌价计算子流程 - 生产环境注册")
    print("=" * 60)
    from modules.inventory_impairment.flows.inventory_impairment_flow import (
        _get_inventory_impairment_defaults_by_date,
    )

    impairment_defaults = _get_inventory_impairment_defaults_by_date()
    print(
        f"默认期间：{impairment_defaults['year']}年第{impairment_defaults['quarter']}季度；"
        "默认通过平台同步 fact_profit_bd 与业报填报记录后回读核对。"
    )
    inventory_impairment_flow.serve(
        name="子流程-季度存货跌价计算",
        tags=["存货跌价", "季度任务", "手动触发", "财务写入"],
        description="默认计算最近已结束季度，通过平台原子同步业报资产减值损失及填报记录并回读核对。",
        parameters=impairment_defaults,
    )

    print("\n" + "=" * 60)
    print("研发项目收益分析流程 - 生产环境注册")
    print("=" * 60)
    from modules.rd_project_profitability.flows.rd_project_profitability_flow import (
        _get_rd_project_profitability_defaults_by_date,
    )

    rd_defaults = _get_rd_project_profitability_defaults_by_date()
    print(
        f"默认期间：{rd_defaults['start_date']} 至 {rd_defaults['end_date']}；"
        "手动触发后生成 Excel 并回调文件信息，默认不写数据库。"
    )
    rd_project_profitability_flow.serve(
        name="主流程-研发项目收益分析",
        tags=["研发项目", "收益分析", "手动触发", "Excel输出", "前端回调"],
        description="按显式期间计算研发项目收益，生成 Excel，并将文件路径或下载链接回调给前端。",
        parameters=rd_defaults,
    )

    print("\n" + "=" * 60)
    print("业务线Staging抽取流程 - 生产环境部署")
    print("=" * 60)
    print("说明：用于从各类数据源中提取业务线拆分的基础数据（含费用、收入、存货等），打竖后存入PostgreSQL以便前端填报")
    bus_line_staging_flow.serve(
        name="主流程-业务线Staging抽取",
        parameters={"modules": list(ALL_MODULE_OPTIONS)},
        tags=["Staging", "业务线核算", "自动执行", "月度任务"],
        description="按modules参数全量或选择性刷新业务线Staging；部分刷新会保留未选模块并生成完整新批次",
    )

    print("\n部署完成！")
    print("Flow 将按计划自动执行")
    print("可在 Prefect UI 中查看：http://127.0.0.1:4200")
