"""往来对账主流程（staging_recon 数据源）

流程逻辑与 Excel 版往来对账一致，但填报数据从 PostgreSQL
staging_recon 表读取，不再扫描共享盘 Excel。
"""
import os
import sys
from typing import Optional

from prefect import flow

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..tasks.recon_auto_fill_tasks import check_and_fill_recon_data_task
from ..tasks.recon_calc_tasks import (
    load_mapping_config_task,
    load_recon_raw_task,
    process_cashflow_task,
    process_sales_purchases_task,
    reconcile_wanglai_task,
    save_recon_results_task,
)
from ..tasks.recon_fetch_tasks import (
    delete_old_recon_data_task,
    fetch_recon_from_mysql_task,
    fetch_recon_from_staging_recon_task,
    insert_recon_data_task,
)
from .fone_recon_flow import fone_recon_flow


@flow(name="staging_recon_flow", log_prints=True)
def staging_recon_flow(target_date: Optional[str] = None, use_fone: bool = False) -> None:
    """
    内部往来对账完整流程（FONE 可选同步 + staging 采集 + 核对）。

    Args:
        target_date: 目标月份，格式 YYYY-MM-DD（如 "2026-02-01"）。
                     不传则自动使用上个自然月。
        use_fone: 是否触发从 FONE 获取往来数据子流程。默认 False。
    """
    print("=" * 60)
    print(f"往来对账流程启动，目标月份: {target_date or '上个自然月（自动计算）'}")
    print("填报数据源: PostgreSQL public.staging_recon")
    print("=" * 60)

    notify_hermes_task(event="started", flow_name="往来对账")

    try:
        if use_fone:
            print("\n【前置阶段】触发从 FONE 获取往来数据脚本，获取 ERP 科目余额表...")
            if target_date:
                year = int(target_date.split("-")[0])
                month = int(target_date.split("-")[1])
                fone_recon_flow(year=year, month=month)
            else:
                fone_recon_flow()
            print("【前置阶段】FONE 数据获取完成，继续执行对账流程...")
        else:
            print("\n【前置阶段】跳过 FONE 数据获取（use_fone=False），如需重新拉取请设为 True")

        print("\n【阶段1】开始数据采集...")

        df_mysql = fetch_recon_from_mysql_task(target_date=target_date)
        df_staging = fetch_recon_from_staging_recon_task(target_date=target_date)

        del_result = delete_old_recon_data_task(target_date=target_date)
        if not del_result.get("success"):
            print(f"[WARN] 删除旧数据返回异常: {del_result.get('error')}，继续写入")

        insert_result = insert_recon_data_task(df_mysql=df_mysql, df_excel=df_staging)
        if not insert_result.get("success"):
            raise RuntimeError(f"阶段1失败，写库错误: {insert_result.get('error')}")
        print(f"【阶段1】完成，共写入 {insert_result.get('count', 0)} 条记录")

        print("\n【阶段2】开始对账核对...")

        auto_fill_result = check_and_fill_recon_data_task(target_date=target_date)
        if auto_fill_result.get("action") == "filled":
            print(f"【自动填充】{auto_fill_result.get('message')}")
        elif auto_fill_result.get("action") == "skipped":
            print(f"【自动填充】{auto_fill_result.get('message')}")
        else:
            print(f"[WARN] 自动填充检测异常: {auto_fill_result.get('message')}")

        df_params = load_mapping_config_task()
        df_raw = load_recon_raw_task(target_date=target_date)

        res_wanglai = reconcile_wanglai_task(df_raw=df_raw, df_params=df_params)
        res_transaction = process_sales_purchases_task(df_raw=df_raw)
        res_cashflow = process_cashflow_task(df_raw=df_raw, df_params=df_params)

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
        print("往来对账流程全部完成！")
        print(f"  往来差异:     {len(res_wanglai)} 条")
        print(f"  销售/采购差异: {len(res_transaction)} 条")
        print(f"  现金流差异:   {len(res_cashflow)} 条")
        print(f"  备份 Excel:   {output_path}")
        print("=" * 60)

        notify_hermes_task(
            event="completed",
            flow_name="往来对账",
            payload={
                "target_date": target_date,
                "source": "staging_recon",
                "output_path": output_path,
                "wanglai_count": len(res_wanglai),
                "transaction_count": len(res_transaction),
                "cashflow_count": len(res_cashflow),
                "summary": f"往来差异 {len(res_wanglai)} 条，销售/采购差异 {len(res_transaction)} 条，现金流差异 {len(res_cashflow)} 条",
            },
        )

    except Exception as e:
        notify_hermes_task(
            event="failed",
            flow_name="往来对账",
            payload={
                "target_date": target_date,
                "source": "staging_recon",
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        raise
