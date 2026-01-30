"""生产环境部署脚本（带计划执行）"""
from modules import business_line_profit_flow, calculate_shared_rate_flow, data_import_flow
import sys
import os
from datetime import datetime

# 添加当前目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


if __name__ == "__main__":
    print("=" * 60)
    print("业务线数据计算流程 - 生产环境部署")
    print("=" * 60)

    # 计算上个月的年份和月份
    now = datetime.now()
    if now.month == 1:
        last_month_year = now.year - 1
        last_month = 12
    else:
        last_month_year = now.year
        last_month = now.month - 1

    print(f"默认参数：year={last_month_year}, month={last_month}")

    # 部署 flow 到 Prefect server（带计划执行）
    business_line_profit_flow.serve(
        name="业务线损益计算流程",
        parameters={
            "year": last_month_year,
            "month": last_month,
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
        name="综合比例计算流程",
        parameters={
            "year": last_month_year,
            "month": last_month,
        },
        tags=["业务线核算", "月度任务", "自动执行", "综合比例"],
        description="综合比例计算流程：计算业务线综合比例（收入、毛利润、净利润、人数的加权平均）",
    )

    print("\n" + "=" * 60)
    print("数据导入流程 - 生产环境部署")
    print("=" * 60)

    print(f"默认参数：使用上个月数据（{last_month_year}年{last_month}月）")
    print("说明：默认不替换已存在的数据（replace_existing=False）")

    # 部署数据导入流程到 Prefect server（带计划执行）
    data_import_flow.serve(
        name="数据导入流程",
        parameters={
            "year": last_month_year,
            "month": last_month,
            "replace_existing": False,
        },
        tags=["数据导入", "月度任务", "自动执行"],
        description="数据导入流程：从 Excel 文件导入数据到数据库（默认不替换已存在数据）",
    )

    print("\n部署完成！")
    print("Flow 将按计划自动执行")
    print("可在 Prefect UI 中查看：http://127.0.0.1:4200")
