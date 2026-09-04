"""报表数据收集、校对和上传主流程

整合三个阶段：
  阶段1 - 数据上报：从 1.上报数据 同步当月修改的文件到 1.补充数据/0.数据源
  阶段2 - 数据汇总：按收集汇总表的PQ结构，分表读取、过滤、汇总
  阶段3 - 名称转换：分表转换列名并导出为CSV
"""

import os
import sys
from typing import Optional

from prefect import flow

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ..tasks.report_tasks import (
    collect_by_sheet_pq_task,
    load_map_translate_task,
    sync_report_data_source_task,
    translate_and_export_pq_task,
)


@flow(name="report_collection_flow", log_prints=True)
def report_collection_flow(target_date: Optional[str] = None) -> None:
    """
    报表数据收集、校对和上传完整流程

    Args:
        target_date: 目标月份，格式 YYYY-MM-DD（如 "2026-04-01"）。
                     不传则自动使用当前月份。

    流程说明：
        阶段1 - 数据上报：
          1. 从 \\11-业务报表\\1.上报数据 扫描当月修改的文件
          2. 按原目录结构复制到 \\11-业务报表\\1.补充数据\\0.数据源

        阶段2 - 数据汇总（分表处理）：
          3. 读取收集汇总表，获取各子表的目标结构（PQ逻辑）
          4. 对每个目标子表（收入成本明细、收单明细、应收明细、存货明细、在途存货、费用明细、其他项目拆分）
          5. 从各公司文件中读取对应原始子表
          6. 按上个月日期过滤（会计期间/日期）
          7. 对齐到汇总表模板结构，分表汇总

        阶段3 - 名称转换与导出：
          8. 读取 map_translate 映射表
          9. 将各子表的列名从中文转换为英文
         10. 导出每个子表为独立CSV文件到 9.手工刷新 目录
    """
    print("=" * 70)
    print(f"报表数据收集流程启动，目标月份: {target_date or '当前月份（自动计算）'}")
    print("=" * 70)

    # ──── 阶段1：数据上报 ───────────────────────────────────
    print("\n【阶段1】同步上报数据到数据源...")
    sync_result = sync_report_data_source_task()
    if not sync_result.get("success"):
        raise RuntimeError(f"阶段1失败: {sync_result.get('message')}")
    print(f"【阶段1】完成，{sync_result.get('message')}")

    # ──── 阶段2：数据汇总（分表处理，基于PQ逻辑）───────────────────────────
    print("\n【阶段2】分表收集数据（基于收集汇总表PQ逻辑）...")
    results = collect_by_sheet_pq_task()

    total_rows = sum(len(df) for df in results.values() if not df.empty)
    print(f"\n【阶段2】完成，共收集 {total_rows} 行数据")

    # ──── 阶段3：名称转换与导出 ─────────────────────────────
    print("\n【阶段3】转换列名并导出CSV...")
    df_map = load_map_translate_task()
    exported_files = translate_and_export_pq_task(results=results, df_map=df_map)

    # 输出汇总
    print("\n" + "=" * 70)
    print("报表数据收集流程全部完成！")
    print("=" * 70)
    print(f"\n各子表汇总结果：")
    for sheet_name, df in results.items():
        row_count = len(df) if not df.empty else 0
        file_path = exported_files.get(sheet_name, "未导出")
        print(
            f"  {sheet_name}: {row_count} 行 -> {os.path.basename(file_path) if file_path else 'N/A'}"
        )
    print("=" * 70)


if __name__ == "__main__":
    report_collection_flow()
