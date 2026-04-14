"""利润表收集、汇总流程

专注处理利润拆分(2-1利润拆分)的端到端流程：
  阶段1 - 数据上报：同步当月修改的上报文件到数据源
  阶段2 - 数据汇总：按PQ逻辑分表收集，汇总生成 2-1利润拆分
  阶段3 - 导出：转换列名为英文并导出CSV

默认执行上个月的数据范围。
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


@flow(name="profit_report_flow", log_prints=True)
def profit_report_flow(target_date: Optional[str] = None) -> dict:
    """
    利润表收集、汇总流程

    Args:
        target_date: 目标月份，格式 YYYY-MM-DD（如 "2026-04-01"）。
                     不传则自动使用上个月。

    流程说明：
        阶段1 - 数据上报：
          1. 从 \\11-业务报表\\1.上报数据 扫描当月修改的文件
          2. 按原目录结构复制到 \\11-业务报表\\1.补充数据\\0.数据源

        阶段2 - 数据汇总：
          3. 读取各公司文件，按PQ逻辑分表收集
          4. 汇总收入成本明细 → 营业收入、营业成本
          5. 汇总费用明细 → 研发费用、管理费用、财务费用、销售费用
          6. 合并生成 2-1利润拆分

        阶段3 - 导出：
          7. 读取 map_translate 映射表
          8. 将 2-1利润拆分 列名转换为英文
          9. 导出为 CSV 到 9.手工刷新 目录
    """
    print("=" * 70)
    print(f"利润表收集汇总流程启动，目标月份: {target_date or '上个月（自动计算）'}")
    print("=" * 70)

    # ──── 阶段1：数据上报 ───────────────────────────────────
    print("\n【阶段1】同步上报数据到数据源...")
    sync_result = sync_report_data_source_task()
    if not sync_result.get("success"):
        raise RuntimeError(f"阶段1失败: {sync_result.get('message')}")
    print(f"【阶段1】完成，{sync_result.get('message')}")

    # ──── 阶段2：数据汇总 ───────────────────────────────────
    print("\n【阶段2】分表收集数据并汇总利润拆分...")
    results = collect_by_sheet_pq_task()

    df_profit = results.get("2-1利润拆分", None)
    if df_profit is None or df_profit.empty:
        print("[WARN] 2-1利润拆分 无数据")
        profit_rows = 0
    else:
        profit_rows = len(df_profit)
        print(f"\n【阶段2】完成，2-1利润拆分 共 {profit_rows} 行")
        # 打印一级科目汇总
        if "一级科目" in df_profit.columns and "本月金额" in df_profit.columns:
            summary = df_profit.groupby("一级科目", as_index=False, dropna=False)["本月金额"].sum()
            print("\n一级科目汇总:")
            for _, row in summary.iterrows():
                print(f"  {row['一级科目']}: {row['本月金额']:,.2f}")
            total = df_profit["本月金额"].sum()
            print(f"  合计: {total:,.2f}")

    # ──── 阶段3：转换列名并导出 ─────────────────────────────
    print("\n【阶段3】转换列名并导出CSV...")
    df_map = load_map_translate_task()
    exported_files = translate_and_export_pq_task(results=results, df_map=df_map)

    profit_file = exported_files.get("2-1利润拆分", "未导出")

    # 输出汇总
    print("\n" + "=" * 70)
    print("利润表收集汇总流程全部完成！")
    print("=" * 70)
    print(
        f"\n2-1利润拆分: {profit_rows} 行 -> {os.path.basename(profit_file) if profit_file else 'N/A'}"
    )
    print("=" * 70)

    return {
        "profit_rows": profit_rows,
        "profit_file": profit_file,
        "all_files": exported_files,
        "sync_result": sync_result,
    }


if __name__ == "__main__":
    profit_report_flow()
