"""数据库视图更新主流程

整合两个阶段：
  阶段1 - 更新映射表：将 mypackage 中的列/表映射写入 map_translate
  阶段2 - 刷新中文视图：遍历所有 base table，生成中文列名视图（无映射则跳过）
  阶段3 - FONE 授权：对 9- 开头视图授予 fone_group SELECT 权限（可选）
"""
import os
import sys
from typing import Optional

from prefect import flow

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

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
    print("=" * 60)
    print("数据库视图更新流程启动")
    print("=" * 60)

    # ──── 阶段1：更新映射表 ───────────────────────────────────
    print("\n【阶段1】更新 map_translate 映射表...")
    map_result = update_map_translate_task()
    print(f"【阶段1】完成，{map_result.get('message')}")

    # ──── 阶段2：刷新中文视图 ─────────────────────────────────
    print("\n【阶段2】刷新中文视图...")
    view_result = refresh_views_task()
    print(
        f"【阶段2】完成，新建 {view_result.get('created', 0)} 个视图，"
        f"跳过 {view_result.get('skipped', 0)} 个无映射表"
    )
    skipped_tables = view_result.get("skipped_tables", [])
    if skipped_tables:
        print(f"跳过的表: {', '.join(skipped_tables)}")

    # ──── 阶段3：FONE 视图授权 ────────────────────────────────
    if not skip_fone_grant:
        print("\n【阶段3】FONE 视图授权...")
        grant_result = grant_fone_permissions_task()
        print(f"【阶段3】完成，授权 {grant_result.get('granted', 0)} 个视图")
    else:
        print("\n【阶段3】已跳过 FONE 视图授权")

    print("\n" + "=" * 60)
    print("数据库视图更新流程全部完成！")
    print("=" * 60)
