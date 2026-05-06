"""从本地推送到远程 Prefect Server 的部署脚本"""

import os
import sys
import time
from datetime import datetime
from multiprocessing import Process

from modules import (
    ai_data_etl_flow,
    budget_update_flow,
    business_line_profit_flow,
    calculate_shared_rate_flow,
    data_import_flow,
    fetch_budget_shared_rate_flow,
    profit_refresh_flow,
    profit_report_flow,
    recon_flow,
)
from modules.bus_line_staging import bus_line_staging_flow

# 添加当前目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# ========== 部署目标：远程 Prefect Server 地址 ==========
# 若已设置环境变量 PREFECT_API_URL，则优先使用环境变量；否则使用下面地址
PREFECT_SERVER_URL = "http://10.18.8.191:4200"
# API 地址（Prefect 要求带 /api 后缀）
PREFECT_API_URL = os.environ.get("PREFECT_API_URL") or f"{PREFECT_SERVER_URL.rstrip('/')}/api"


def _serve_business_line(process_year: int, months: list):
    """模块级函数，供 Process 调用（Windows 要求可 pickle，不能是嵌套函数）"""
    business_line_profit_flow.serve(
        name="主流程-业务线损益计算",
        parameters={"year": process_year, "months": months},
        tags=["业务线核算", "月度任务", "自动执行"],
        description="业务线损益计算流程：生成收入、费用、利润、应收、存货、在途存货明细表，并刷新利润表",
    )


def _serve_shared_rate(last_month_year: int, last_month: int):
    """模块级函数，供 Process 调用"""
    calculate_shared_rate_flow.serve(
        name="主流程-综合比例年底重算",
        parameters={"year": last_month_year, "month": last_month},
        tags=["业务线核算", "月度任务", "自动执行", "综合比例"],
        description="综合比例计算流程：计算业务线综合比例（收入、毛利润、净利润、人数的加权平均）",
    )


def _serve_data_import(last_month_year: int, last_month: int):
    """模块级函数，供 Process 调用"""
    data_import_flow.serve(
        name="主流程-数据导入",
        parameters={
            "year": last_month_year,
            "month": last_month,
            "replace_existing": False,
        },
        tags=["数据导入", "月度任务", "自动执行"],
        description="数据导入流程：从 Excel 文件导入数据到数据库（默认不替换已存在数据）",
    )


def _serve_ai_data_etl():
    """模块级函数，供 Process 调用"""
    ai_data_etl_flow.serve(
        name="主流程-AI数据ETL",
        tags=["AI数据", "ETL", "手动触发"],
        description="为AI平台生成业务数据视图（业务线/业报数据）",
    )


def _serve_budget_update():
    """模块级函数，供 Process 调用（预算更新为手动触发，参数按当前月份设默认值）"""
    from modules.budget_update.flows.budget_update_flow import _get_budget_defaults_by_date

    budget_defaults = _get_budget_defaults_by_date()
    budget_update_flow.serve(
        name="主流程-预算更新",
        tags=["预算更新", "手动触发"],
        description="从 FONE 拉取预算、严格映射检查、写库；未映射则中断并导出 CSV。参数可留空，按运行时的当前月份自动填默认值。",
        parameters=budget_defaults,
    )


def _serve_profit_refresh():
    """模块级函数，供 Process 调用"""
    profit_refresh_flow.serve(
        name="子流程-利润表刷新",
        tags=["业务线核算", "手动触发", "自动执行"],
        description="利润表刷新流程：处理所有已计算的月份数据，生成 fact_profit 和 fact_bus_profit 表",
    )


def _serve_fetch_budget_shared_rate():
    """模块级函数，供 Process 调用"""
    from modules.shared_rate.flows.fetch_budget_shared_rate_flow import _get_default_dates

    defaults = _get_default_dates()
    fetch_budget_shared_rate_flow.serve(
        name="子流程-拉取预算综合比例",
        parameters=defaults,
        tags=["预算更新", "手动触发", "自动执行", "综合比例"],
        description="获取预算表中最新1号的综合比例，并写入业务线实际比例表中覆盖年初至上月底。",
    )


def _serve_recon():
    """模块级函数，供 Process 调用"""
    recon_flow.serve(
        name="主流程-往来对账",
        tags=["往来对账", "月度任务", "自动执行"],
        description="内部往来对账流程：自动从 MySQL + Excel 采集上月数据，写入 PostgreSQL，再生成往来/销售/现金流三类对账结果并导出 Excel。",
    )


def _serve_bus_line_staging():
    """模块级函数，供 Process 调用"""
    bus_line_staging_flow.serve(
        name="主流程-业务线Staging抽取",
        tags=["Staging", "业务线核算", "自动执行", "月度任务"],
        description="将业务线拆分1-4步骤数据以EAV格式存入PostgreSQL系统待填报",
    )


def _serve_profit_report():
    """模块级函数，供 Process 调用"""
    profit_report_flow.serve(
        name="子流程-利润表收集汇总",
        tags=["报表收集", "利润表", "手动触发", "月度任务"],
        description="利润表收集、汇总流程：按PQ逻辑收集数据源并汇总生成2-1利润拆分，导出CSV。默认执行上个月。",
    )


def deploy_to_remote_server():
    """
    从本地推送流程到远程 Prefect Server

    使用前需要：
    1. 配置 Prefect Server 地址：
       - Windows: set PREFECT_API_URL=http://your-server:4200/api
       - Linux/Mac: export PREFECT_API_URL=http://your-server:4200/api
       - 或使用: prefect config set PREFECT_API_URL=http://your-server:4200/api
    2. 确保网络可以访问远程服务器
    3. 确保远程服务器上已启动 Prefect Server
    """
    print("=" * 60)
    print("从本地推送流程到远程 Prefect Server")
    print("=" * 60)

    # 使用脚本顶部配置的 API 地址，并让 Prefect 使用该地址
    api_url = PREFECT_API_URL
    os.environ["PREFECT_API_URL"] = api_url
    print(f"部署目标 Prefect API: {api_url}")
    print(f"部署目标 UI 地址: {PREFECT_SERVER_URL}")
    if "127.0.0.1" in api_url or "localhost" in api_url:
        print("\n⚠️  当前为本地地址；若需推送到远程服务器，请修改本文件顶部 PREFECT_SERVER_URL")
        # systemd 等非交互环境没有 stdin，直接继续；仅在有终端时询问
        if sys.stdin.isatty():
            response = input("是否继续部署？(y/n): ")
            if response.lower() != "y":
                print("部署已取消")
                return
        else:
            print("非交互环境，自动继续部署")

    # 计算自动运行日期范围：1月到上个月；1月份则为上年全部
    now = datetime.now()
    if now.month == 1:
        process_year = now.year - 1
        months = list(range(1, 13))
    else:
        process_year = now.year
        months = list(range(1, now.month))

    print("\n开始部署流程...")
    print("=" * 60)

    # 使用多进程同时部署多个流程（serve() 会持续运行）
    # 目标必须是模块级函数，否则 Windows 下 multiprocessing 无法 pickle 嵌套函数
    process1 = Process(target=_serve_business_line, args=(process_year, months))
    process2 = Process(target=_serve_shared_rate, args=(process_year, months[-1]))
    process3 = Process(target=_serve_data_import, args=(process_year, months[-1]))
    process4 = Process(target=_serve_ai_data_etl)
    process5 = Process(target=_serve_budget_update)
    process6 = Process(target=_serve_fetch_budget_shared_rate)
    process7 = Process(target=_serve_profit_refresh)
    process8 = Process(target=_serve_recon)
    process9 = Process(target=_serve_bus_line_staging)
    process10 = Process(target=_serve_profit_report)

    process1.start()
    time.sleep(1)
    process2.start()
    time.sleep(1)
    process3.start()
    time.sleep(1)
    process4.start()
    time.sleep(1)
    process5.start()
    time.sleep(1)
    process6.start()
    time.sleep(1)
    process7.start()
    time.sleep(1)
    process8.start()
    time.sleep(1)
    process9.start()
    time.sleep(1)
    process10.start()

    print("\n✓ 流程已开始部署...")
    print("流程会持续运行并保持与服务器的连接")
    print("可以在 Prefect UI 中查看：", api_url.replace("/api", ""))
    print("\n按 Ctrl+C 停止部署")

    try:
        process1.join()
        process2.join()
        process3.join()
        process4.join()
        process5.join()
        process6.join()
        process7.join()
    except KeyboardInterrupt:
        print("\n\n正在停止部署...")
        for p in [
            process1,
            process2,
            process3,
            process4,
            process5,
            process6,
            process7,
            process8,
            process9,
            process10,
        ]:
            p.terminate()
            p.join()
        print("部署已停止")


if __name__ == "__main__":
    try:
        deploy_to_remote_server()
    except KeyboardInterrupt:
        print("\n\n部署已停止")
    except Exception as e:
        print(f"\n部署失败: {str(e)}")
        import traceback

        traceback.print_exc()
