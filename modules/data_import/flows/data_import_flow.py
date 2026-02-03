"""数据导入流程"""
import platform

from ..tasks.data_import_tasks import (
    read_excel_data_task,
    update_production_data_task,
    update_rd_data_task,
    update_purchase_data_task,
    update_inventory_data_task,
    update_cost_control_data_task,
    update_business_data_task,
    update_personnel_data_task,
    update_manual_refresh_data_task,
)
from utils.date_utils import get_date_range_by_month, get_date_range_by_months, get_date_range_by_lastmonth
from prefect import flow
from typing import List, Optional
from datetime import datetime
import pandas as pd
import sys
import os

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

# Windows 与 Rocky Linux 下 Excel 根目录默认路径（Rocky 路径请按实际挂载或数据目录修改）
if platform.system() == "Windows":
    DEFAULT_ROOT_DIRECTORY = r"Z:\11-业务报表\1.补充数据"
else:
    DEFAULT_ROOT_DIRECTORY = "/mnt/xgd_share/11-业务报表/1.补充数据"  # Rocky Linux，可按实际挂载点修改


def get_last_month() -> tuple:
    """获取上个月的年份和月份"""
    now = datetime.now()
    if now.month == 1:
        last_month_year = now.year - 1
        last_month = 12
    else:
        last_month_year = now.year
        last_month = now.month - 1
    return last_month_year, last_month


@flow(name="data_import_flow", log_prints=True)
def data_import_flow(
    year: Optional[int] = None,
    month: Optional[int] = None,
    months: Optional[List[int]] = None,
    replace_existing: bool = False,
    root_directory: Optional[str] = None,
) -> None:
    """
    数据导入流程

    从 Excel 文件导入数据到数据库，支持按年、按月或批量处理多个月份。

    Args:
        year: 年份，如果不提供则使用上个月的年份
        month: 单个月份（1-12），如果提供则只处理该月
        months: 月份列表（1-12），如果提供则按月循环处理多个月份，例如 [10, 11, 12]
        replace_existing: 是否替换已存在的数据，默认 False（不替换）。如果为 True，则替换已存在的数据
        root_directory: Excel 文件根目录路径；不传则按系统自动选择（Windows: Z:\\11-业务报表\\1.补充数据，Rocky: /mnt/业务报表/1.补充数据）

    Examples:
        # 处理上个月的数据（默认）
        data_import_flow()

        # 处理单个月份（只处理 12 月）
        data_import_flow(year=2025, month=12)

        # 批量处理多个月份（按月循环执行）
        data_import_flow(year=2025, months=[10, 11, 12])

        # 替换已存在的数据
        data_import_flow(year=2025, month=12, replace_existing=True)
    """
    if root_directory is None:
        root_directory = DEFAULT_ROOT_DIRECTORY

    print("=" * 60)
    print("开始数据导入流程")
    print("=" * 60)

    # 打印当前使用的参数（用于调试和确认）
    print(f"\n当前参数设置:")
    print(f"  - year: {year if year is not None else '未指定（将使用上个月）'}")
    print(f"  - month: {month if month is not None else '未指定'}")
    print(f"  - months: {months if months is not None else '未指定'}")
    print(f"  - replace_existing: {replace_existing}")
    print(f"  - root_directory: {root_directory}")
    print()

    # 确定要处理的年份和月份
    if year is None:
        # 如果没有指定年份，使用上个月
        process_year, process_month = get_last_month()
        if month is None and months is None:
            # 如果也没有指定月份，使用上个月
            month_list = [(process_year, process_month)]
            print(f"未指定参数，使用上个月: {process_year}年{process_month}月")
        elif month is not None:
            month_list = [(process_year, month)]
            print(f"使用指定月份: {process_year}年{month}月")
        elif months is not None:
            month_list = [(process_year, m) for m in months]
            print(f"批量处理模式，月份数: {len(months)}，将按月循环执行")
            print(
                f"处理月份: {', '.join([f'{process_year}年{m}月' for m in months])}")
    else:
        # 如果指定了年份
        if months is not None:
            # 批量处理多个月份
            month_list = [(year, m) for m in months]
            print(f"批量处理模式，年份: {year}，月份数: {len(months)}")
            print(f"处理月份: {', '.join([f'{year}年{m}月' for m in months])}")
        elif month is not None:
            # 只处理单个月份
            month_list = [(year, month)]
            print(f"处理模式：只处理 {year}年{month}月")
        else:
            # 如果只指定了年份，使用上个月
            process_year, process_month = get_last_month()
            if process_year == year:
                month_list = [(year, process_month)]
                print(f"只指定了年份 {year}，使用上个月: {process_month}月")
            else:
                raise ValueError(
                    f"只指定了年份 {year}，但上个月是 {process_year}年{process_month}月，请同时指定月份")

    print(f"replace_existing 参数: {replace_existing}")
    if not replace_existing:
        print("注意: 如果数据已存在，将跳过更新")
    print(f"Excel 文件目录: {root_directory}")
    print("=" * 60)

    # 读取 Excel 数据（只读取一次，所有月份共享）
    print("\n开始读取 Excel 数据...")
    dfs = read_excel_data_task(root_directory)

    # 按月循环执行数据导入
    for idx, (process_year, process_month) in enumerate(month_list, 1):
        print(f"\n{'='*60}")
        print(
            f"开始处理第 {idx}/{len(month_list)} 个月：{process_year}年{process_month}月")
        print(f"{'='*60}")

        # 获取该月的日期范围
        date_range = get_date_range_by_month(process_year, process_month)
        start_date = date_range.min().strftime('%Y-%m-%d')
        end_date = date_range.max().strftime('%Y-%m-%d')
        print(f"日期范围: {start_date} 到 {end_date}")

        try:
            # 1. 更新生产数据
            update_production_data_task(
                dfs, start_date, end_date, replace_existing
            )

            # 2. 更新研发数据
            update_rd_data_task(
                dfs, start_date, end_date, replace_existing
            )

            # 3. 更新采购数据
            update_purchase_data_task(
                dfs, start_date, end_date, replace_existing
            )

            # 4. 更新存货数据
            update_inventory_data_task(
                dfs, start_date, end_date, replace_existing
            )

            # 5. 更新费控数据
            update_cost_control_data_task(
                dfs, start_date, end_date, replace_existing
            )

            # 6. 更新业务数据
            update_business_data_task(
                dfs, start_date, end_date, replace_existing, root_directory
            )

            # 7. 更新人力费用数据
            update_personnel_data_task(
                dfs, start_date, end_date, replace_existing
            )

            # 8. 更新手工刷新数据
            update_manual_refresh_data_task(
                dfs, start_date, end_date, replace_existing
            )

            print(f"\n✓ {process_year}年{process_month}月 数据导入完成")
        except Exception as e:
            error_msg = f"{process_year}年{process_month}月 数据导入失败: {str(e)}"
            print(f"\n❌ {error_msg}")
            # 打印完整的错误堆栈信息
            import traceback
            print(f"错误详情:\n{traceback.format_exc()}")
            # 重新抛出异常，让 Prefect UI 能够捕获并显示为失败状态
            raise Exception(error_msg) from e

    print(f"\n{'='*60}")
    print(f"数据导入流程全部完成，共处理 {len(month_list)} 个月")
    print(f"{'='*60}")
