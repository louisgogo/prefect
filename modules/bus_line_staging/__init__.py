from prefect import flow, task, get_run_logger
import os
import sys

# 将业务线核算目录加入环境变量以便可以引用里面的包和配置
sys.path.append(r"D:\PSQL\业务线核算")

@task(name="1-前中后台费用数据拆分", retries=1)
def run_stage_1(custom_date_range=None):
    from staging_data_split_job import run_expense_split_to_staging
    run_expense_split_to_staging(custom_date_range=custom_date_range)

@task(name="2-特定部门收入及其他拆分", retries=1)
def run_stage_2(custom_date_range=None):
    from scripts.staging_revenue_other_split_job import run_split_and_insert
    run_split_and_insert(custom_date_range=custom_date_range)

@task(name="3-无归属业务线拆分", retries=1)
def run_stage_3(custom_date_range=None):
    from scripts.staging_unassigned_split_job import run_unassigned_split_and_insert
    run_unassigned_split_and_insert(custom_date_range=custom_date_range)

@task(name="4-存货应收拆分", retries=1)
def run_stage_4(custom_date_range=None):
    from scripts.staging_inventory_receivable_split_job import run_inv_ar_split
    run_inv_ar_split(custom_date_range=custom_date_range)

@flow(name="业务线数据中间库抽取流程(Staging)", description="将业务线拆分1-4步骤数据以EAV格式打竖并存入PostgreSQL系统待填报")
def bus_line_staging_flow(start_date: str = None, end_date: str = None):
    logger = get_run_logger()
    logger.info("开始执行业务线数据打平入库(Staging)全流程...")
    
    custom_date_range = None
    if start_date and end_date:
        import pandas as pd
        custom_date_range = pd.date_range(start=start_date, end=end_date, freq='D').date
        logger.info(f"使用自定义日期范围: {start_date} 到 {end_date}")
    else:
        logger.info("未提供自定义日期范围，将使用默认规则(上个月)。")
    
    # 按照先后顺序逐步提取并清洗存储数据
    run_stage_1(custom_date_range=custom_date_range)
    logger.info("第1步：费用数据入库已完成。")
    
    run_stage_2(custom_date_range=custom_date_range)
    logger.info("第2步：特定收入及其他数据入库已完成。")
    
    run_stage_3(custom_date_range=custom_date_range)
    logger.info("第3步：无归属业务兜底数据入库已完成。")
    
    run_stage_4(custom_date_range=custom_date_range)
    logger.info("第4步：特定存货及应收数据入库已完成。")
    
    logger.info("✅ 所有的业务线Staging数据拆分提取工作流已顺利完成！")

if __name__ == "__main__":
    bus_line_staging_flow()
