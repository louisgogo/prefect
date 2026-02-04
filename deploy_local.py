"""本地测试部署脚本"""
from modules import (
    business_line_profit_flow,
    calculate_shared_rate_flow,
    data_import_flow,
    budget_update_flow,
)
import sys
import os
from multiprocessing import Process
from datetime import datetime

# 添加当前目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


def get_last_month():
    """获取上个月的年份和月份"""
    now = datetime.now()
    if now.month == 1:
        last_month_year = now.year - 1
        last_month = 12
    else:
        last_month_year = now.year
        last_month = now.month - 1
    return last_month_year, last_month


def deploy_business_line_profit_flow():
    """部署业务线损益计算流程"""
    print("=" * 60)
    print("业务线损益计算流程 - 本地测试部署")
    print("=" * 60)

    print("说明：请在 UI 中手动输入参数（year, month, months）")
    print("提示：year 是必需参数，month 和 months 二选一")

    # 部署到本地 Prefect Server（无计划执行，仅用于手动触发）
    # 不指定 parameters，让用户在 UI 中手动输入，避免误操作
    business_line_profit_flow.serve(
        name="业务线损益计算流程-本地测试",
        tags=["本地测试", "业务线核算"],
        description="本地测试用：业务线损益计算流程（业务线数据计算+利润表刷新）",
    )


def deploy_shared_rate_flow():
    """部署综合比例计算流程"""
    print("=" * 60)
    print("综合比例计算流程 - 本地测试部署")
    print("=" * 60)

    print("说明：请在 UI 中手动输入参数（year, month, months）")
    print("提示：year 是必需参数，month 和 months 二选一")

    # 部署综合比例计算流程到本地 Prefect Server
    # 不指定 parameters，让用户在 UI 中手动输入，避免误操作
    calculate_shared_rate_flow.serve(
        name="综合比例计算流程-本地测试",
        tags=["本地测试", "业务线核算", "综合比例"],
        description="本地测试用：计算业务线综合比例",
    )


def deploy_data_import_flow():
    """部署数据导入流程"""
    print("=" * 60)
    print("数据导入流程 - 本地测试部署")
    print("=" * 60)

    print("说明：请在 UI 中手动输入参数（或使用默认值）")
    print("提示：")
    print("  - year: 可选，如果不提供则使用上个月的年份")
    print("  - month: 可选，单个月份（1-12），与 months 二选一")
    print("  - months: 可选，月份列表，例如 [10, 11, 12]，与 month 二选一")
    print("  - replace_existing: 默认 False（不替换已存在数据），设为 True 则替换")
    print("  - root_directory: 默认使用手工刷新目录")
    print("\n注意：如果不提供 year/month/months，将自动使用上个月的数据")

    # 部署数据导入流程到本地 Prefect Server
    # 不指定 parameters，让用户在 UI 中手动输入，避免误操作
    # 由于 flow 函数中所有参数都有默认值，Prefect 会自动识别参数
    data_import_flow.serve(
        name="数据导入流程-本地测试",
        tags=["本地测试", "数据导入"],
        description="本地测试用：从 Excel 文件导入数据到数据库（默认不替换已存在数据，如存在则跳过）",
    )


def deploy_budget_update_flow():
    """部署预算更新流程（带按当前日期计算的默认参数）"""
    from modules.budget_update.flows.budget_update_flow import _get_budget_defaults_by_date

    print("=" * 60)
    print("预算更新流程 - 本地测试部署")
    print("=" * 60)

    defaults = _get_budget_defaults_by_date()
    print("说明：以下参数已按当前月份设默认值，可在 UI 中修改")
    print("默认值规则：上年11月～2月→年初预算（11/12月用下年度-01-01，1/2月用当年度-01-01）；4月～7月→年中预算（当年度-07-01）")
    print("当前默认参数：")
    for k, v in defaults.items():
        print(f"  - {k}: {v}")
    print("  - output_dir: 不填则使用当前工作目录")
    print("\n易混点：report_date=要替换的那批日期；version=本批新数据的填报日期标签。")
    print("\n注意：任一映射检查点存在未映射将中断执行并导出 CSV")

    budget_update_flow.serve(
        name="预算更新流程-本地测试",
        tags=["本地测试", "预算更新"],
        description="从 FONE 拉取预算、严格映射检查、写库；未映射则中断并导出 CSV。参数可留空，按运行时的当前月份自动填默认值。",
        parameters=defaults,
    )


if __name__ == "__main__":
    print("开始部署流程...")
    print("确保已启动 Prefect Server：prefect server start")
    print("请在 Prefect UI 中查看：http://127.0.0.1:4200")
    print("\n" + "=" * 60)

    # 使用多进程同时部署四个流程
    process1 = Process(target=deploy_business_line_profit_flow)
    process2 = Process(target=deploy_shared_rate_flow)
    process3 = Process(target=deploy_data_import_flow)
    process4 = Process(target=deploy_budget_update_flow)

    process1.start()
    process2.start()
    process3.start()
    process4.start()

    # 等待进程完成（实际上 serve() 会一直运行，所以这里会一直等待）
    try:
        process1.join()
        process2.join()
        process3.join()
        process4.join()
    except KeyboardInterrupt:
        print("\n正在停止部署...")
        process1.terminate()
        process2.terminate()
        process3.terminate()
        process4.terminate()
        process1.join()
        process2.join()
        process3.join()
        process4.join()
        print("部署已停止")
