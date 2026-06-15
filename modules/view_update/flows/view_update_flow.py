"""数据库视图更新主流程

整合三个阶段：
  阶段1 - 更新映射表：将 mypackage 中的列/表映射写入 map_translate
  阶段2 - 刷新中文视图：遍历所有 base table，生成中文列名视图（无映射则跳过）
  阶段3 - FONE 授权：对 9-/7-/4-/1- 开头视图授予 fone_group SELECT 权限（可选）
"""
import os
import sys
from typing import Optional

from prefect import flow

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ...common.tasks.notify_hermes_task import notify_hermes_task
from ..tasks.view_update_tasks import (
    grant_fone_permissions_task,
    refresh_views_task,
    update_map_translate_task,
)


@flow(name="view_update_flow", log_prints=True)
def view_update_flow(skip_fone_grant: bool = False) -> None:
    """
    数据库视图更新主流程

    Args:
        skip_fone_grant: 是否跳过 FONE 视图授权阶段，默认 False（执行授权）

    流程说明：
        阶段1 - 更新映射表：
          1. 将 combined_column_mapping + combined_table_mapping 写入 map_translate

        阶段2 - 刷新中文视图：
          2. 删除现有旧视图（保留业报/FONE/ai相关）
          3. 遍历所有 base table，排除系统表
          4. 表无中文映射则跳过，不生成视图
          5. 列无中文映射则保留英文名
          6. 执行 CREATE OR REPLACE VIEW

        阶段3 - FONE 授权（可选）：
          7. 对名称以 9- 开头的视图授予 fone_group SELECT 权限
    """
    print("=" * 70)
    print("【主流程】数据库视图更新")
    print("=" * 70)

    notify_hermes_task(
        event="started",
        flow_name="数据库视图更新",
        payload={"skip_fone_grant": skip_fone_grant},
    )

    try:
        # 阶段1：更新映射表
        print("\n【阶段1/3】更新映射表...")
        map_result = update_map_translate_task()
        print(f"【阶段1】完成: {map_result.get('message')}")

        # 阶段2：刷新中文视图
        print("\n【阶段2/3】刷新中文视图...")
        view_result = refresh_views_task()
        created = view_result.get("created", 0)
        skipped = view_result.get("skipped", 0)
        skipped_tables = view_result.get("skipped_tables", [])
        print(f"【阶段2】完成: 新建 {created} 个视图, 跳过 {skipped} 个无映射表")
        if skipped_tables:
            print(f"【阶段2】跳过表: {', '.join(skipped_tables)}")

        # 阶段3：FONE 视图授权
        if not skip_fone_grant:
            print("\n【阶段3/3】FONE 视图授权...")
            grant_result = grant_fone_permissions_task()
            granted = grant_result.get("granted", 0)
            failed = grant_result.get("failed", [])
            if failed:
                print(f"【阶段3】完成: 授权 {granted} 个, 失败 {len(failed)} 个")
            else:
                print(f"【阶段3】完成: 授权 {granted} 个")
        else:
            print("\n【阶段3/3】已跳过")

        print("\n" + "=" * 70)
        print("【主流程】完成")
        print("=" * 70)

        notify_hermes_task(
            event="completed",
            flow_name="数据库视图更新",
            payload={
                "skip_fone_grant": skip_fone_grant,
                "created": created,
                "skipped": skipped,
                "summary": f"数据库视图更新完成，新建 {created} 个视图",
            },
        )
    except Exception as e:
        error_msg = f"数据库视图更新流程失败: {str(e)}"
        print(f"\n{error_msg}")
        notify_hermes_task(
            event="failed",
            flow_name="数据库视图更新",
            payload={
                "error": str(e),
                "error_type": type(e).__name__,
                "skip_fone_grant": skip_fone_grant,
            },
        )
        raise Exception(error_msg) from e
