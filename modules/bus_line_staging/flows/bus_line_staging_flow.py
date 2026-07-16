from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..batch import (
    batch_summary,
    compare_batch_to_previous,
    complete_batch,
    fail_batch,
    inherit_previous_values,
    start_batch,
)
from ..config import get_bus_lines, get_date_range
from ..tasks.asset_tasks import run_inv_ar_split_task
from ..tasks.expense_tasks import run_expense_split_to_staging_task
from ..tasks.ratio_fill_tasks import run_revenue_ratio_fill_task
from ..tasks.revenue_tasks import run_revenue_other_split_task
from ..tasks.unassigned_tasks import run_unassigned_split_task


@flow(
    name="业务线数据中间库抽取流程(Staging)",
    description="按批次生成业务线拆分底稿，并从上一填报批次继承比例和审核状态",
)
def bus_line_staging_flow(start_date: str | None = None, end_date: str | None = None):
    """
    业务线Staging抽取流程。

    前置依赖：运行前请确保 fact_bus_wage_rate 表已更新目标月份的预算工资比例，
    否则后台人力及行政/人力中心分摊费用（含房租）将因缺少比例数据而中断报错。
    """
    logger = get_run_logger()
    logger.info("开始执行业务线数据打平入库(Staging)全流程...")

    date_range = get_date_range(start_date, end_date)
    if start_date and end_date:
        logger.info(f"使用自定义日期范围: {start_date} 到 {end_date}")
        date_label = f"{start_date} 到 {end_date}"
    else:
        logger.info(f"使用默认日期范围 (上月): {date_range[0]} 到 {date_range[-1]}")
        date_label = f"{date_range[0]} 到 {date_range[-1]}"

    notify_hermes_task(
        event="started",
        flow_name="业务线Staging抽取",
        payload={
            "start_date": start_date,
            "end_date": end_date,
            "date_range": date_label,
        },
    )

    batch_id = None
    batch_completed = False
    try:
        runtime_flow_run_id = str(flow_run.id) if flow_run.id else None
        batch_id = start_batch(date_range, flow_run_id=runtime_flow_run_id)
        logger.info(f"本次Staging抽取批次: {batch_id}")

        # 1. 费用数据拆分
        run_expense_split_to_staging_task(date_range, batch_id)
        logger.info("第1步：费用数据入库已完成。")

        # 2. 特定部门收入及其他拆分
        run_revenue_other_split_task(date_range, batch_id)
        logger.info("第2步：特定收入及其他数据入库已完成。")

        # 3. 无归属业务线拆分
        run_unassigned_split_task(date_range, batch_id)
        logger.info("第3步：无归属业务兜底数据入库已完成。")

        # 4. 收入比例自动填充
        run_revenue_ratio_fill_task(date_range, batch_id)
        logger.info("第4步：收入比例自动填充已完成。")

        # 5. 存货应收拆分
        run_inv_ar_split_task(date_range, batch_id)
        logger.info("第5步：特定存货及应收数据入库已完成。")

        bus_lines = get_bus_lines()
        inherited_counts = inherit_previous_values(batch_id, bus_lines)
        inherited_total = sum(inherited_counts.values())
        comparison = compare_batch_to_previous(batch_id, bus_lines)
        batch_status = complete_batch(batch_id)
        batch_completed = True
        result = batch_summary(batch_id)
        batch_no = result["batch_no"]
        logger.info(f"批次 {batch_id} 已生成，继承 {inherited_total} 条旧比例/审核状态，" f"当前状态: {batch_status}")
        comparison_totals = comparison["totals"]
        logger.info(
            f"批次对比 {comparison['previous_batch_no'] or '无历史批次'} -> {batch_no}: "
            f"旧记录 {comparison_totals['old_records']}，"
            f"新记录 {comparison_totals['new_records']}，"
            f"新增 {comparison_totals['added']}，"
            f"删除 {comparison_totals['removed']}，"
            f"来源变化 {comparison_totals['source_changed']}，"
            f"未变化 {comparison_totals['unchanged']}"
        )
        for table_name, table_comparison in comparison["tables"].items():
            logger.info(
                f"批次对比明细 {table_name}: "
                f"新增 {table_comparison['added']}，"
                f"删除 {table_comparison['removed']}，"
                f"来源变化 {table_comparison['source_changed']}，"
                f"未变化 {table_comparison['unchanged']}"
            )

        logger.info("✅ 所有的业务线Staging数据拆分提取工作流已顺利完成！")

        notify_hermes_task(
            event="completed",
            flow_name="业务线Staging抽取",
            payload={
                "start_date": start_date,
                "end_date": end_date,
                "date_range": date_label,
                "batch_id": batch_id,
                "batch_no": batch_no,
                "batch_status": batch_status,
                "inherited_records": inherited_total,
                "comparison_totals": comparison_totals,
                "summary": f"业务线Staging抽取完成，范围: {date_label}，批次: {batch_no}",
            },
        )
        result["inherited_records"] = inherited_total
        result["inherited_by_table"] = inherited_counts
        result["comparison"] = comparison
        return result
    except Exception as e:
        if batch_id and not batch_completed:
            try:
                fail_batch(batch_id, e)
            except Exception as cleanup_error:
                logger.error(f"标记失败批次 {batch_id} 时发生错误: {cleanup_error}")
        error_msg = f"业务线Staging抽取流程失败: {str(e)}"
        logger.error(f"\n{error_msg}")
        notify_hermes_task(
            event="failed",
            flow_name="业务线Staging抽取",
            payload={
                "error": str(e),
                "error_type": type(e).__name__,
                "start_date": start_date,
                "end_date": end_date,
                "date_range": date_label,
                "batch_id": batch_id,
            },
        )
        raise Exception(error_msg) from e


if __name__ == "__main__":
    # 本地测试入口
    bus_line_staging_flow()
