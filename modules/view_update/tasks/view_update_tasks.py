"""视图更新相关 Tasks

包含：
1. 更新 map_translate 映射表
2. 刷新数据库中文视图（表无映射则跳过）
3. FONE 视图授权
"""

import os
import sys
from typing import Any, Dict, List, Tuple

from prefect import task

# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mypackage.mapping import (
    combined_column_mapping,
    combined_table_mapping,
    reverse_combined_column_mapping,
    reverse_combined_table_mapping,
)
from mypackage.utilities import connect_to_db

# 排除不生成视图的系统表
EXCLUDE_TABLES = {
    "map_translate",
    "jwtauth_profile",
    "django_admin_log",
    "django_session",
    "django_migrations",
    "django_content_type",
    "auth_permission",
    "auth_group",
    "auth_group_permissions",
    "auth_user_groups",
    "auth_user_user_permissions",
    "auth_user",
    "access_roles_fin",
    "access_roles_bus",
    "access_roles_org",
    "ai_bud_expense",
    "ai_bud_income",
    "ai_bud_profit",
    "ai_bus_expense",
    "ai_bus_revenue",
    "ai_bus_profit",
    "excel_account_recon",
    "fone_project",
}


@task(name="update_map_translate", log_prints=True)
def update_map_translate_task() -> Dict[str, Any]:
    """
    更新 map_translate 映射表

    将 combined_column_mapping 和 combined_table_mapping 合并后写入数据库，
    每次执行前先清空旧数据。

    Returns:
        {"success": bool, "count": int, "message": str}
    """
    field_mapping = {**combined_column_mapping, **combined_table_mapping}
    print(
        f"[map_translate] 准备写入映射数据，column_mapping={len(combined_column_mapping)}条, table_mapping={len(combined_table_mapping)}条, 总计={len(field_mapping)}条"
    )

    conn = None
    cur = None
    try:
        conn, cur = connect_to_db()

        # 确保表存在（兼容不同数据库方言）
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_translate (
                name_en VARCHAR(255),
                name_ch VARCHAR(255)
            )
            """
        )

        # 清空旧数据
        cur.execute("DELETE FROM map_translate")

        # 插入映射数据
        inserted = 0
        for name_ch, name_en in field_mapping.items():
            cur.execute(
                "INSERT INTO map_translate (name_en, name_ch) VALUES (%s, %s)",
                (name_en, name_ch),
            )
            inserted += 1

        conn.commit()
        print(
            f"[map_translate] 完成: 写入 {inserted} 条映射 ({len(combined_column_mapping)}列+{len(combined_table_mapping)}表)"
        )
        return {"success": True, "count": inserted, "message": f"写入 {inserted} 条映射"}

    except Exception as e:
        print(f"[map_translate] ERROR: 更新 map_translate 失败: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.rollback()
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@task(name="refresh_views", log_prints=True)
def refresh_views_task() -> Dict[str, Any]:
    """
    刷新数据库中文视图

    逻辑：
    1. 删除现有视图（保留名称含"业报"、"FONE"、"ai"的视图）
    2. 遍历所有 base table，排除系统表
    3. 对每个表查找 reverse_combined_table_mapping 中文名：
       - 无中文映射 → 跳过，不生成视图
    4. 对每个列查找 reverse_combined_column_mapping 中文名：
       - 无中文映射 → 保留英文列名（不 AS）
    5. 执行 CREATE OR REPLACE VIEW

    Returns:
        {"success": bool, "created": int, "skipped": int, "skipped_tables": List[str]}
    """
    conn = None
    cur = None
    try:
        conn, cur = connect_to_db()
        print(f"[refresh_views] 数据库连接成功")

        # ---------- 1. 删除现有视图 ----------
        print("[refresh_views] 阶段1: 查询现有视图...")
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'VIEW'
            """
        )
        views = cur.fetchall()
        print(f"[refresh_views] 发现 {len(views)} 个现有视图")
        dropped = 0
        protected = 0
        for view in views:
            view_name = view[0]
            if "业报" not in view_name and "FONE" not in view_name and "ai" not in view_name:
                drop_sql = f'DROP VIEW IF EXISTS "{view_name}" CASCADE'
                cur.execute(drop_sql)
                dropped += 1
                print(f"[refresh_views] 已删除视图: {view_name}")
            else:
                protected += 1
                print(f"[refresh_views] 保留视图: {view_name}")
        conn.commit()
        print(f"[refresh_views] 阶段1完成: 删除 {dropped} 个, 保留 {protected} 个")

        # ---------- 2. 获取所有 base table ----------
        print("[refresh_views] 阶段2: 查询 base table 列表...")
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """
        )
        all_tables = [t[0] for t in cur.fetchall()]
        tables = [t for t in all_tables if t not in EXCLUDE_TABLES]
        excluded = [t for t in all_tables if t in EXCLUDE_TABLES]
        print(
            f"[refresh_views] 发现 {len(all_tables)} 个表, 排除 {len(excluded)} 个系统表, 待处理 {len(tables)} 个"
        )
        if excluded:
            print(f"[refresh_views] 排除的系统表: {', '.join(excluded)}")

        created = 0
        skipped = 0
        skipped_tables: List[str] = []

        for idx, table_name in enumerate(tables, 1):
            # ---------- 3. 表中文映射检查 ----------
            table_chinese = reverse_combined_table_mapping.get(table_name)
            if table_chinese is None:
                skipped += 1
                skipped_tables.append(table_name)
                continue

            # ---------- 4. 获取列并映射 ----------
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            columns = cur.fetchall()

            column_parts: List[str] = []
            missing_cols: List[str] = []
            for col in columns:
                english_name = col[0]
                chinese_name = reverse_combined_column_mapping.get(english_name)
                if chinese_name:
                    column_parts.append(f'{english_name} AS "{chinese_name}"')
                else:
                    missing_cols.append(english_name)
                    column_parts.append(english_name)

            column_str = ", ".join(column_parts)
            view_sql = f'CREATE OR REPLACE VIEW "{table_chinese}" AS SELECT {column_str} FROM "{table_name}";'

            try:
                cur.execute(view_sql)
                created += 1
                log_msg = (
                    f"[{idx}/{len(tables)}] OK: {table_name} -> {table_chinese} ({len(columns)}列"
                )
                if missing_cols:
                    log_msg += f", {len(missing_cols)}列无映射: {','.join(missing_cols[:3])}"
                    if len(missing_cols) > 3:
                        log_msg += f"...等"
                    log_msg += ")"
                else:
                    log_msg += ")"
                print(f"[refresh_views] {log_msg}")
            except Exception as inner_e:
                print(
                    f"[refresh_views] [{idx}/{len(tables)}] ERROR: {table_name} -> {table_chinese} 失败: {inner_e}"
                )
                print(f"[refresh_views] ERROR SQL: {view_sql}")
                raise

        conn.commit()
        print(f"[refresh_views] 阶段2完成: 新建 {created} 个视图, 跳过 {skipped} 个无映射表")
        if skipped_tables:
            print(f"[refresh_views] 跳过的表列表: {', '.join(skipped_tables)}")
        return {
            "success": True,
            "created": created,
            "skipped": skipped,
            "skipped_tables": skipped_tables,
        }

    except Exception as e:
        print(f"[refresh_views] ERROR: 刷新视图失败: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.rollback()
            print("[refresh_views] 事务已回滚")
        raise

    finally:
        if cur:
            cur.close()
            print("[refresh_views] cursor 已关闭")
        if conn:
            conn.close()
            print("[refresh_views] 数据库连接已关闭")


@task(name="grant_fone_permissions", log_prints=True)
def grant_fone_permissions_task() -> Dict[str, Any]:
    """
    对 FONE 相关视图（名称以 9- 开头）授予 fone_group SELECT 权限

    Returns:
        {"success": bool, "granted": int, "views": List[str]}
    """
    conn = None
    cur = None
    try:
        conn, cur = connect_to_db()
        conn.autocommit = True
        print("[fone_grant] 数据库连接成功，autocommit=True")

        cur.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'public' AND table_name LIKE '9-%%'
            """
        )
        views = cur.fetchall()
        view_names = [v[0] for v in views]
        print(f"[fone_grant] 发现 {len(view_names)} 个 FONE 视图: {', '.join(view_names)}")

        granted = 0
        failed_views: List[str] = []
        for view_name in view_names:
            grant_sql = f'GRANT SELECT ON "{view_name}" TO fone_group;'
            try:
                cur.execute(grant_sql)
                granted += 1
            except Exception as grant_e:
                failed_views.append(view_name)

        if failed_views:
            print(
                f"[fone_grant] 完成: 授权 {granted}/{len(view_names)} 个, 失败 {len(failed_views)} 个: {', '.join(failed_views)}"
            )
        else:
            print(f"[fone_grant] 完成: 授权 {granted}/{len(view_names)} 个")
        return {"success": True, "granted": granted, "views": view_names, "failed": failed_views}

    except Exception as e:
        print(f"[fone_grant] ERROR: FONE 视图授权失败: {e}")
        import traceback

        traceback.print_exc()
        raise

    finally:
        if conn:
            conn.close()
            print("[fone_grant] 数据库连接已关闭")
