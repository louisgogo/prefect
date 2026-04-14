from prefect import flow, get_run_logger

from ..config import get_date_range
from ..tasks.asset_tasks import run_inv_ar_split_task
from ..tasks.expense_tasks import run_expense_split_to_staging_task
from ..tasks.revenue_tasks import run_revenue_other_split_task
from ..tasks.unassigned_tasks import run_unassigned_split_task


@flow(name="业务线数据中间库抽取流程(Staging)", description="将业务线拆分1-4步骤数据以EAV格式打平并存入PostgreSQL系统待填报")
def bus_line_staging_flow(start_date: str | None = None, end_date: str | None = None):
    logger = get_run_logger()
    logger.info("开始执行业务线数据打平入库(Staging)全流程...")

    # 获取日期范围
    date_range = get_date_range(start_date, end_date)
    if start_date and end_date:
        logger.info(f"使用自定义日期范围: {start_date} 到 {end_date}")
    else:
        logger.info(f"使用默认日期范围 (上月): {date_range[0]} 到 {date_range[-1]}")

    # 1. 费用数据拆分
    run_expense_split_to_staging_task(date_range)
    logger.info("第1步：费用数据入库已完成。")

    # 2. 特定部门收入及其他拆分
    run_revenue_other_split_task(date_range)
    logger.info("第2步：特定收入及其他数据入库已完成。")

    # 3. 无归属业务线拆分
    run_unassigned_split_task(date_range)
    logger.info("第3步：无归属业务兜底数据入库已完成。")

    # 4. 存货应收拆分
    run_inv_ar_split_task(date_range)
    logger.info("第4步：特定存货及应收数据入库已完成。")

    logger.info("✅ 所有的业务线Staging数据拆分提取工作流已顺利完成！")


if __name__ == "__main__":
    # 本地测试入口
    bus_line_staging_flow()
