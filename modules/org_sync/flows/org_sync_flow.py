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
    compare_org_task,
    fetch_fone_org_task,
    fetch_map_org_task,
    generate_org_diff_report_task,
)


def _print_df_to_logs(title: str, df, max_rows: int = 200) -> None:
    """将 DataFrame 以文本表格形式打印到日志（供 AI 读取）"""
    print(f"\n{'='*60}")
    print(f"【{title}】 共 {len(df)} 条")
    print("=" * 60)
    if len(df) == 0:
        print("(无数据)")
        return
    # 限制输出行数，避免日志过大
    df_out = df.head(max_rows)
    print(df_out.to_string(index=False))
    if len(df) > max_rows:
        print(f"... 省略 {len(df) - max_rows} 条，共 {len(df)} 条")


@flow(name="org_sync_flow", log_prints=True)
def org_sync_flow(
    only_last_stage: bool = True,
    output_dir: Optional[str] = None,
    generate_excel: bool = False,
) -> Optional[str]:
    """
    组织架构同步对比流程

    Args:
        only_last_stage: 是否只对比 LastStage='是' 的组织（默认 True）
        output_dir: Excel 报告输出目录，默认当前工作目录
        generate_excel: 是否生成 Excel 报告（默认 False）

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

        # 阶段2: 对比分析（仅核对新增 ID）
        print("\n--- 阶段2: 对比分析差异（仅新增） ---")
        diff_result = compare_org_task(
            df_fone=df_fone,
            df_map=df_map,
            df_dim=df_dim,
            only_last_stage=only_last_stage,
        )

        # 阶段3: 输出新增组织到日志
        print("\n--- 阶段3: 差异详情输出到日志 ---")
        _print_df_to_logs("差异汇总", diff_result["summary"])
        if len(diff_result["added"]) > 0:
            _print_df_to_logs(
                "新增组织",
                diff_result["added"][
                    [
                        "fone_id",
                        "fone_name",
                        "fone_path",
                        "lvl1",
                        "lvl2",
                        "lvl3",
                        "suggested_db_corr_rel",
                        "matched_db_corr_rel",
                        "match_status",
                        "BusinessLine",
                        "LastStage",
                    ]
                ],
            )
        else:
            print("\n无新增组织")

        # 阶段4: 可选生成 Excel 报告
        report_path = None
        if generate_excel:
            print("\n--- 阶段4: 生成 Excel 报告 ---")
            report_path = generate_org_diff_report_task(
                diff_result=diff_result,
                output_dir=output_dir,
            )

        # 完成通知
        summary = diff_result["summary"]
        added = summary.loc[summary["差异类型"] == "新增", "数量"].values[0]

        notify_hermes_task(
            event="completed",
            flow_name="组织架构同步对比",
            payload={
                "新增": int(added),
                "报告路径": report_path or "",
            },
        )

        print("\n" + "=" * 60)
        print("组织架构同步对比流程完成")
        print(f"新增: {added}")
        print("=" * 60)

        return report_path

    except Exception as e:
        notify_hermes_task(event="failed", flow_name="组织架构同步对比", payload={"error": str(e)})
        raise
