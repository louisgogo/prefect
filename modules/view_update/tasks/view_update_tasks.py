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
        for name_ch, name_en in field_mapping.items():
            cur.execute(
                "INSERT INTO map_translate (name_en, name_ch) VALUES (%s, %s)",
                (name_en, name_ch),
            )

        conn.commit()
        count = len(field_mapping)
        print(f"map_translate 更新完成，共写入 {count} 条映射")
        return {"success": True, "count": count, "message": f"写入 {count} 条映射"}

    except Exception as e:
        print(f"更新 map_translate 失败: {e}")
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
        for view in views:
            view_name = view[0]
            if "业报" not in view_name and "FONE" not in view_name and "ai" not in view_name:
                cur.execute(f'DROP VIEW IF EXISTS "{view_name}" CASCADE')
                dropped += 1
                print(f"已删除视图: {view_name}")
        conn.commit()
        print(f"共删除 {dropped} 个旧视图")

        # ---------- 2. 获取所有 base table ----------
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """
        )
        tables = [t[0] for t in cur.fetchall() if t[0] not in EXCLUDE_TABLES]
        print(f"待处理表数量: {len(tables)}")

        created = 0
        skipped = 0
        skipped_tables: List[str] = []

        for table_name in tables:
            # ---------- 3. 表中文映射检查 ----------
            table_chinese = reverse_combined_table_mapping.get(table_name)
            if table_chinese is None:
                print(f"跳过表 {table_name}: 无中文映射")
                skipped += 1
                skipped_tables.append(table_name)
                continue

            # ---------- 4. 获取列并映射 ----------
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table_name,),
            )
            columns = cur.fetchall()

            column_parts: List[str] = []
            for col in columns:
                english_name = col[0]
                chinese_name = reverse_combined_column_mapping.get(english_name)
                if chinese_name:
                    column_parts.append(f'{english_name} AS "{chinese_name}"')
                else:
                    # 列无中文映射，保留英文名
                    column_parts.append(english_name)

            column_str = ", ".join(column_parts)
            view_sql = f'CREATE OR REPLACE VIEW "{table_chinese}" AS SELECT {column_str} FROM "{table_name}";'

            cur.execute(view_sql)
            created += 1
            print(f"已创建视图: {table_chinese} -> {table_name} ({len(columns)} 列)")

        conn.commit()
        print(f"视图刷新完成: 新建 {created} 个, 跳过 {skipped} 个")
        return {
            "success": True,
            "created": created,
            "skipped": skipped,
            "skipped_tables": skipped_tables,
        }

    except Exception as e:
        print(f"刷新视图失败: {e}")
        if conn:
            conn.rollback()
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
        {"success": bool, "granted": int}
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

        granted = 0
        for view in views:
            view_name = view[0]
            cur.execute(f'GRANT SELECT ON "{view_name}" TO fone_group;')
            granted += 1

        print(f"FONE 视图授权完成，共授权 {granted} 个视图")
        return {"success": True, "granted": granted}

    except Exception as e:
        print(f"FONE 视图授权失败: {e}")
        raise

    finally:
        if conn:
            conn.close()
