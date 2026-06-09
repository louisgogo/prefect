"""组织架构同步对比流程

对比 FONE (XGD_MRPT_ENTITY) 和 mydb (map_org) 的组织架构差异，
生成差异报告并写入 org_diff_log，帮助维护 map_org 与 dim_org_struc 的一致性。
"""
import os
import sys
from typing import Optional

from prefect import flow

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..tasks.org_sync_tasks import (
    build_unique_lvl_mapping_task,
    compare_org_task,
    fetch_fone_org_task,
    fetch_map_org_task,
    generate_org_diff_report_task,
    save_org_diff_to_db_task,
)


@flow(name="org_sync_flow", log_prints=True)
def org_sync_flow(
    only_last_stage: bool = True,
    output_dir: Optional[str] = None,
    save_to_db: bool = True,
    generate_excel: bool = True,
) -> Optional[str]:
    """
    组织架构同步对比流程

    Args:
        only_last_stage: 是否只对比 LastStage='是' 的组织（默认 True）
        output_dir: Excel 报告输出目录，默认当前工作目录
        save_to_db: 是否将差异写入 mydb.org_diff_log（默认 True）
        generate_excel: 是否生成 Excel 报告（默认 True）

    Returns:
        Excel 报告路径（如果生成），否则 None
    """
    print("=" * 60)
    print("组织架构同步对比流程启动")
    print("=" * 60)

    notify_hermes_task(event="started", flow_name="组织架构同步对比")

    try:
        # 阶段1: 获取两端数据
        print("\n--- 阶段1: 获取 FONE 和 mydb 数据 ---")
        df_fone = fetch_fone_org_task()
        df_map, df_dim = fetch_map_org_task()

        # 阶段2: 对比分析
        print("\n--- 阶段2: 对比分析差异 ---")
        diff_result = compare_org_task(
            df_fone=df_fone,
            df_map=df_map,
            df_dim=df_dim,
            only_last_stage=only_last_stage,
        )

        # 阶段3: unique_lvl 映射分析（额外辅助信息）
        print("\n--- 阶段3: unique_lvl 映射分析 ---")
        mapping_df = build_unique_lvl_mapping_task(df_fone=df_fone, df_dim=df_dim)

        # 阶段4: 保存结果
        report_path = None
        if save_to_db:
            print("\n--- 阶段4a: 写入数据库 ---")
            save_org_diff_to_db_task(diff_result=diff_result)

        if generate_excel:
            print("\n--- 阶段4b: 生成 Excel 报告 ---")
            report_path = generate_org_diff_report_task(
                diff_result=diff_result,
                output_dir=output_dir,
            )
            # 将映射分析也追加到 Excel 的单独 sheet
            if report_path and len(mapping_df) > 0:
                import pandas as pd

                with pd.ExcelWriter(report_path, engine="openpyxl", mode="a") as writer:
                    mapping_df.to_excel(writer, sheet_name="unique_lvl映射建议", index=False)
                print(f"已追加 unique_lvl 映射建议到报告")

        # 完成通知
        summary = diff_result["summary"]
        added = summary.loc[summary["差异类型"] == "新增", "数量"].values[0]
        modified = summary.loc[summary["差异类型"] == "层级变更", "数量"].values[0]
        removed = summary.loc[summary["差异类型"] == "停用/缺失", "数量"].values[0]

        notify_hermes_task(
            event="completed",
            flow_name="组织架构同步对比",
            payload={
                "新增": int(added),
                "层级变更": int(modified),
                "停用或缺失": int(removed),
                "报告路径": report_path or "",
            },
        )

        print("\n" + "=" * 60)
        print("组织架构同步对比流程完成")
        print(f"新增: {added} | 层级变更: {modified} | 停用/缺失: {removed}")
        print("=" * 60)

        return report_path

    except Exception as e:
        notify_hermes_task(event="failed", flow_name="组织架构同步对比", payload={"error": str(e)})
        raise
