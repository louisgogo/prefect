"""往来对账主流程

整合两个阶段：
  阶段1 - 采集：从 MySQL + 共享盘 Excel 获取原始数据，写入 PostgreSQL
  阶段2 - 核对：从 PostgreSQL 读取数据，加载映射配置表，生成三类对账结果

默认处理"上个自然月"数据，也可通过 target_date 参数指定月份。
"""
import os
import sys
from typing import Optional

from prefect import flow

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ..tasks.recon_calc_tasks import (
    load_mapping_config_task,
    load_recon_raw_task,
    process_cashflow_task,
    process_sales_purchases_task,
    reconcile_wanglai_task,
    save_recon_results_task,
)
from ..tasks.recon_fetch_tasks import (
    collect_recon_from_excel_task,
    delete_old_recon_data_task,
    fetch_recon_from_mysql_task,
    insert_recon_data_task,
)


@flow(name="recon_flow", log_prints=True)
def recon_flow(target_date: Optional[str] = None) -> None:
    """
    内部往来对账完整流程（阶段1采集 + 阶段2核对）

    Args:
        target_date: 目标月份，格式 YYYY-MM-DD（如 "2026-02-01"）。
                     不传则自动使用上个自然月（相对于运行日期）。

    流程说明：
        阶段1 - 数据采集与存库：
          1. 从 MySQL Fone2BI_IntCommCheck 读取当月数据
          2. 从共享盘 Excel（/9-数据源）扫描采集数据
          3. 删除 PostgreSQL excel_account_recon 中目标月旧数据
          4. 合并 MySQL + Excel 数据写入 PostgreSQL

        阶段2 - 对账核对与结果输出：
          5. 加载共享盘 映射配置表.xlsx（参数表/差异说明）
          6. 从 PostgreSQL 读取目标月原始数据
          7. 往来余额 三向核对（应收 vs 应付）
          8. 销售/采购 发生额核对
          9. 现金流量 收入 vs 支付核对
         10. 写入结果表（PostgreSQL）+ 导出备份 Excel
    """
    print("=" * 60)
    print(f"往来对账流程启动，目标月份: {target_date or '上个自然月（自动计算）'}")
    print("=" * 60)

    # ──── 阶段1：数据采集 ────────────────────────────────────
    print("\n【阶段1】开始数据采集...")

    # Step 1: 从 MySQL 读取
    df_mysql = fetch_recon_from_mysql_task(target_date=target_date)

    # Step 2: 从 Excel 扫描（失败不中断）
    df_excel = collect_recon_from_excel_task(target_date=target_date)

    # Step 3: 删除旧数据
    del_result = delete_old_recon_data_task(target_date=target_date)
    if not del_result.get("success"):
        print(f"[WARN] 删除旧数据返回异常: {del_result.get('error')}，继续写入")

    # Step 4: 写入新数据
    insert_result = insert_recon_data_task(df_mysql=df_mysql, df_excel=df_excel)
    if not insert_result.get("success"):
        raise RuntimeError(f"阶段1失败，写库错误: {insert_result.get('error')}")
    print(f"【阶段1】完成，共写入 {insert_result.get('count', 0)} 条记录")

    # ──── 阶段2：对账核对 ────────────────────────────────────
    print("\n【阶段2】开始对账核对...")

    # Step 5: 加载映射配置表
    (
        df_params,
        df_unit_map,
        df_yebao_unit_map,
        df_diff_wanglai,
        df_diff_xiaoshou,
        df_diff_xianjinliu,
    ) = load_mapping_config_task()

    # Step 6: 读取原始数据
    df_raw = load_recon_raw_task(target_date=target_date)

    # Step 7: 往来核对
    res_wanglai = reconcile_wanglai_task(
        df_raw=df_raw,
        df_params=df_params,
        df_diff_wanglai=df_diff_wanglai,
    )

    # Step 8: 销售/采购核对
    res_transaction = process_sales_purchases_task(
        df_raw=df_raw,
        df_diff_xiaoshou=df_diff_xiaoshou,
    )

    # Step 9: 现金流核对
    res_cashflow = process_cashflow_task(
        df_raw=df_raw,
        df_params=df_params,
        df_diff_xianjinliu=df_diff_xianjinliu,
    )

    # Step 10: 保存结果
    output_path = save_recon_results_task(
        res_wanglai=res_wanglai,
        res_transaction=res_transaction,
        res_cashflow=res_cashflow,
        target_date=target_date,
    )

    print("\n" + "=" * 60)
    print("【AI 对账结果分析专用数据源】")
    print("--- 往来差异 (recon_result_wanglai) ---")
    print(res_wanglai.to_string(index=False) if not res_wanglai.empty else "无差异")
    print("\n--- 销售/采购差异 (recon_result_sales) ---")
    print(res_transaction.to_string(index=False) if not res_transaction.empty else "无差异")
    print("\n--- 现金流差异 (recon_result_cashflow) ---")
    print(res_cashflow.to_string(index=False) if not res_cashflow.empty else "无差异")
    print("=" * 60)

    print("\n" + "=" * 60)
    print(f"往来对账流程全部完成！")
    print(f"  往来差异:     {len(res_wanglai)} 条")
    print(f"  销售/采购差异: {len(res_transaction)} 条")
    print(f"  现金流差异:   {len(res_cashflow)} 条")
    print(f"  备份 Excel:   {output_path}")
    print("=" * 60)
