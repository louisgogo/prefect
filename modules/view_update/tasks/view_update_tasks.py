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

    conn = None
    cur = None
    try:
        conn, cur = connect_to_db()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_translate (
                name_en VARCHAR(255),
                name_ch VARCHAR(255)
            )
            """
        )
        cur.execute("DELETE FROM map_translate")

        inserted = 0
        for name_ch, name_en in field_mapping.items():
            cur.execute(
                "INSERT INTO map_translate (name_en, name_ch) VALUES (%s, %s)",
                (name_en, name_ch),
            )
            inserted += 1

        conn.commit()
        return {
            "success": True,
            "count": inserted,
            "message": f"写入 {inserted} 条映射 ({len(combined_column_mapping)}列+{len(combined_table_mapping)}表)",
        }

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

        # ---------- 1. 删除现有视图 ----------
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'VIEW'
            """
        )
        views = cur.fetchall()
        dropped = 0
        protected = 0
        for view in views:
            view_name = view[0]
            if "业报" not in view_name and "FONE" not in view_name and "ai" not in view_name:
                cur.execute(f'DROP VIEW IF EXISTS "{view_name}" CASCADE')
                dropped += 1
            else:
                protected += 1
        conn.commit()

        # ---------- 2. 获取所有 base table ----------
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
        print(f"[refresh_views] 待处理 {len(tables)} 个表, 排除 {len(excluded)} 个系统表, 清理 {dropped} 个旧视图")

        created = 0
        skipped = 0
        skipped_tables: List[str] = []

        for idx, table_name in enumerate(tables, 1):
            # ---------- 3. 表中文映射检查 ----------
            table_chinese = reverse_combined_table_mapping.get(table_name)
            if table_chinese is None:
                skipped += 1
                skipped_tables.append(table_name)
                print(f"[refresh_views] SKIP: {table_name} 无中文映射")
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
                if missing_cols:
                    print(
                        f"[refresh_views] WARN: {table_name} -> {table_chinese} 有 {len(missing_cols)} 列无映射: {', '.join(missing_cols[:5])}"
                    )
            except Exception as inner_e:
                print(f"[refresh_views] ERROR: {table_name} -> {table_chinese} 失败: {inner_e}")
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
        if conn:
            conn.close()


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

        cur.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'public' AND table_name LIKE '9-%%'
            """
        )
        views = cur.fetchall()
        view_names = [v[0] for v in views]

        granted = 0
        failed_views: List[str] = []
        for view_name in view_names:
            grant_sql = f'GRANT SELECT ON "{view_name}" TO fone_group;'
            try:
                cur.execute(grant_sql)
                granted += 1
            except Exception:
                failed_views.append(view_name)

        if failed_views:
            print(f"[fone_grant] 授权 {granted}/{len(view_names)} 个, 失败: {', '.join(failed_views)}")
        return {"success": True, "granted": granted, "views": view_names, "failed": failed_views}

    except Exception as e:
        print(f"[fone_grant] ERROR: FONE 视图授权失败: {e}")
        import traceback

        traceback.print_exc()
        raise

    finally:
        if conn:
            conn.close()
