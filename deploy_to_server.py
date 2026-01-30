"""从本地推送到远程 Prefect Server 的部署脚本"""
from modules import business_line_profit_flow, calculate_shared_rate_flow, data_import_flow
import sys
import os
from datetime import datetime
from multiprocessing import Process

# 添加当前目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


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
    
    # 检查 Prefect API URL
    api_url = os.getenv("PREFECT_API_URL")
    if not api_url:
        # 尝试从 Prefect 配置读取
        try:
            from prefect.settings import PREFECT_API_URL
            api_url = str(PREFECT_API_URL.value()) if PREFECT_API_URL.value() else "未设置"
        except:
            api_url = "未设置"
    
    print(f"当前 Prefect API URL: {api_url}")
    if api_url == "未设置" or "127.0.0.1" in api_url or "localhost" in api_url:
        print("\n⚠️  警告：当前连接到本地服务器")
        print("如需推送到远程服务器，请先设置 PREFECT_API_URL 环境变量")
        print("示例：")
        print("  Windows: set PREFECT_API_URL=http://your-server:4200/api")
        print("  Linux/Mac: export PREFECT_API_URL=http://your-server:4200/api")
        print("  或使用: prefect config set PREFECT_API_URL=http://your-server:4200/api")
        print()
        response = input("是否继续部署到当前服务器？(y/n): ")
        if response.lower() != 'y':
            print("部署已取消")
            return
    
    # 计算上个月的年份和月份
    now = datetime.now()
    if now.month == 1:
        last_month_year = now.year - 1
        last_month = 12
    else:
        last_month_year = now.year
        last_month = now.month - 1
    
    print("\n开始部署流程...")
    print("=" * 60)
    
    # 使用多进程同时部署三个流程（serve() 会持续运行）
    def deploy_business_line():
        business_line_profit_flow.serve(
            name="业务线损益计算流程",
            parameters={
                "year": last_month_year,
                "month": last_month,
            },
            tags=["业务线核算", "月度任务", "自动执行"],
            description="业务线损益计算流程：生成收入、费用、利润、应收、存货、在途存货明细表，并刷新利润表",
        )
    
    def deploy_shared_rate():
        calculate_shared_rate_flow.serve(
            name="综合比例计算流程",
            parameters={
                "year": last_month_year,
                "month": last_month,
            },
            tags=["业务线核算", "月度任务", "自动执行", "综合比例"],
            description="综合比例计算流程：计算业务线综合比例（收入、毛利润、净利润、人数的加权平均）",
        )
    
    def deploy_data_import():
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
    
    # 启动三个进程
    process1 = Process(target=deploy_business_line)
    process2 = Process(target=deploy_shared_rate)
    process3 = Process(target=deploy_data_import)
    
    process1.start()
    process2.start()
    process3.start()
    
    print("\n✓ 流程已开始部署...")
    print("流程会持续运行并保持与服务器的连接")
    print("可以在 Prefect UI 中查看：", api_url.replace("/api", ""))
    print("\n按 Ctrl+C 停止部署")
    
    try:
        process1.join()
        process2.join()
        process3.join()
    except KeyboardInterrupt:
        print("\n\n正在停止部署...")
        process1.terminate()
        process2.terminate()
        process3.terminate()
        process1.join()
        process2.join()
        process3.join()
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
