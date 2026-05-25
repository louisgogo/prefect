"""数据库视图更新主流程

整合三个阶段：
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
    print("=" * 70)
    print("【主流程】数据库视图更新流程启动")
    print(f"  参数: skip_fone_grant={skip_fone_grant}")
    print("=" * 70)

    # ──── 阶段1：更新映射表 ───────────────────────────────────
    print("\n" + "─" * 70)
    print("【阶段1/3】更新 map_translate 映射表")
    print("─" * 70)
    try:
        map_result = update_map_translate_task()
        print(f"\n【阶段1】结果: {map_result.get('message')}")
    except Exception as e:
        print(f"\n【阶段1】FAILED: 映射表更新失败: {e}")
        print("【主流程】因阶段1失败，终止执行")
        raise

    # ──── 阶段2：刷新中文视图 ─────────────────────────────────
    print("\n" + "─" * 70)
    print("【阶段2/3】刷新中文视图")
    print("─" * 70)
    try:
        view_result = refresh_views_task()
        created = view_result.get("created", 0)
        skipped = view_result.get("skipped", 0)
        skipped_tables = view_result.get("skipped_tables", [])
        print(f"\n【阶段2】结果: 新建 {created} 个视图, 跳过 {skipped} 个无映射表")
        if skipped_tables:
            print(f"【阶段2】跳过的表: {', '.join(skipped_tables)}")
    except Exception as e:
        print(f"\n【阶段2】FAILED: 视图刷新失败: {e}")
        print("【主流程】因阶段2失败，终止执行")
        raise

    # ──── 阶段3：FONE 视图授权 ────────────────────────────────
    if not skip_fone_grant:
        print("\n" + "─" * 70)
        print("【阶段3/3】FONE 视图授权")
        print("─" * 70)
        try:
            grant_result = grant_fone_permissions_task()
            granted = grant_result.get("granted", 0)
            views = grant_result.get("views", [])
            failed = grant_result.get("failed", [])
            print(f"\n【阶段3】结果: 授权 {granted}/{len(views)} 个 FONE 视图")
            if failed:
                print(f"【阶段3】授权失败的视图: {', '.join(failed)}")
        except Exception as e:
            print(f"\n【阶段3】FAILED: FONE 授权失败: {e}")
            print("【主流程】阶段3失败，但前两个阶段已成功")
            raise
    else:
        print("\n" + "─" * 70)
        print("【阶段3/3】已跳过 FONE 视图授权 (skip_fone_grant=True)")
        print("─" * 70)

    print("\n" + "=" * 70)
    print("【主流程】数据库视图更新流程全部完成！")
    print(f"  - 映射表: 已更新")
    print(f"  - 视图: 新建 {created} 个, 跳过 {skipped} 个")
    if not skip_fone_grant:
        print(f"  - FONE授权: {granted} 个视图")
    print("=" * 70)
