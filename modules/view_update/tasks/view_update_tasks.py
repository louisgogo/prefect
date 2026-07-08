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

CUSTOM_TABLE_MAPPING = {
    "往来对账映射配置": "recon_mapping_config",
}

CUSTOM_COLUMN_MAPPING = {
    "对账映射项目": "recon_item",
    "对账统一名称": "recon_unified_name",
}

CUSTOM_REVERSE_TABLE_MAPPING = {v: k for k, v in CUSTOM_TABLE_MAPPING.items()}
CUSTOM_REVERSE_COLUMN_MAPPING = {v: k for k, v in CUSTOM_COLUMN_MAPPING.items()}

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
    "fone_project",
}


@task(name="update_map_translate", log_prints=True)
def update_map_translate_task() -> Dict[str, Any]:
    """
    更新 map_translate 映射表

    将 combined/custom column/table mapping 合并后写入数据库，
    每次执行前先清空旧数据。

    Returns:
        {"success": bool, "count": int, "message": str}
    """
    field_mapping = {
        **combined_column_mapping,
        **combined_table_mapping,
        **CUSTOM_COLUMN_MAPPING,
        **CUSTOM_TABLE_MAPPING,
    }

    seen_en: Dict[str, str] = {}
    seen_ch: Dict[str, str] = {}
    for name_ch, name_en in field_mapping.items():
        if name_en in seen_en and seen_en[name_en] != name_ch:
            raise ValueError(f"map_translate 英文字段重复: {name_en} -> {seen_en[name_en]}, {name_ch}")
        if name_ch in seen_ch and seen_ch[name_ch] != name_en:
            raise ValueError(f"map_translate 中文字段重复: {name_ch} -> {seen_ch[name_ch]}, {name_en}")
        seen_en[name_en] = name_ch
        seen_ch[name_ch] = name_en

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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS recon_mapping_config (
                recon_item VARCHAR(255) PRIMARY KEY,
                recon_unified_name VARCHAR(255) NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE recon_mapping_config
            ADD COLUMN IF NOT EXISTS recon_item VARCHAR(255)
            """
        )
        cur.execute(
            """
            ALTER TABLE recon_mapping_config
            ADD COLUMN IF NOT EXISTS recon_unified_name VARCHAR(255) NOT NULL DEFAULT ''
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_recon_mapping_config_recon_item
            ON recon_mapping_config (recon_item)
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

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_map_translate_name_en
            ON map_translate (name_en)
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_map_translate_name_ch
            ON map_translate (name_ch)
            """
        )

        conn.commit()
        return {
            "success": True,
            "count": inserted,
            "message": (
                f"写入 {inserted} 条映射 "
                f"({len(combined_column_mapping) + len(CUSTOM_COLUMN_MAPPING)}列+"
                f"{len(combined_table_mapping) + len(CUSTOM_TABLE_MAPPING)}表)"
            ),
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
        table_mapping = {**reverse_combined_table_mapping, **CUSTOM_REVERSE_TABLE_MAPPING}
        column_mapping = {**reverse_combined_column_mapping, **CUSTOM_REVERSE_COLUMN_MAPPING}

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
            table_chinese = table_mapping.get(table_name)
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
                chinese_name = column_mapping.get(english_name)
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

        # ---------- 3. 创建自定义业务视图 ----------
        custom_views = [
            (
                "业报预测表",
                """CREATE OR REPLACE VIEW "业报预测表" AS
SELECT
    row_number() OVER () AS id,
    "预测层级",
    "日期",
    "一级科目",
    "金额"
FROM (
    WITH CombinedResults AS (
        SELECT
            "一级组织" || '-' || "二级组织" AS "预测层级",
            "日期",
            "一级科目",
            SUM("预测数") AS "金额"
        FROM
            "3-8预测数"
        WHERE
            "一级科目" NOT IN ('营业利润', '利润总额', '净利润') AND "填报日期" >= (SELECT MAX("日期") FROM "业报利润表")  AND "填报日期" = (SELECT MAX("填报日期") FROM "3-8预测数")
        group by "预测层级","日期","一级科目","一级组织","二级组织"
    ),
    ProfitCalculation AS (
        SELECT
            "预测层级",
            "日期",
            SUM(CASE WHEN "一级科目" = '营业收入' THEN "金额" ELSE 0 END) AS Total_Revenue,
            SUM(CASE WHEN "一级科目" IN ('营业成本', '税金及附加', '管理费用', '销售费用', '财务费用', '研发费用') THEN "金额" ELSE 0 END) AS Total_Cost,
            SUM(CASE WHEN "一级科目" IN ('其他收益', '公允价值变动收益', '投资收益', '资产减值损失', '资产处置收益', '资产处置损失', '信用减值损失') THEN "金额" ELSE 0 END) AS Total_Others,
            SUM(CASE WHEN "一级科目" IN ('营业外收入') THEN "金额" ELSE 0 END) AS Non_Income,
            SUM(CASE WHEN "一级科目" IN ('营业外支出') THEN "金额" ELSE 0 END) AS Non_Expense,
            SUM(CASE WHEN "一级科目" = '所得税费用' THEN "金额" ELSE 0 END) AS Total_tax
        FROM
            CombinedResults
        GROUP BY
            "预测层级", "日期"
    )
    SELECT
        "预测层级",
        "日期",
        '营业利润' AS "一级科目",
        Total_Revenue - Total_Cost + Total_Others AS "金额"
    FROM
        ProfitCalculation

    UNION ALL

    SELECT
        "预测层级",
        "日期",
        '利润总额' AS "一级科目",
        Total_Revenue - Total_Cost + Total_Others + Non_Income - Non_Expense AS "金额"
    FROM
        ProfitCalculation

    UNION ALL

    SELECT
        "预测层级",
        "日期",
        '净利润' AS "一级科目",
        Total_Revenue - Total_Cost + Total_Others + Non_Income - Non_Expense - Total_tax AS "金额"
    FROM
        ProfitCalculation

    UNION ALL

    SELECT
        "预测层级",
        "日期",
        "一级科目",
        "金额"
    FROM
        CombinedResults

    UNION ALL

    SELECT
        b."一级组织映射" || '-' || b."映射关系" AS "预测层级",
        a."日期",
        a."一级科目",
        a."金额"
    FROM
        "业报利润表" a
        JOIN "1-1组织架构" b ON a."唯一层级" = b."唯一层级"

) AS FinalResults""",
            ),
            (
                "业报预测表年度",
                """CREATE OR REPLACE VIEW "业报预测表年度" AS
SELECT
    row_number() OVER () AS id,
    "预测层级",
    "显示层级",
    "一级科目",
    EXTRACT(YEAR FROM "日期") AS "年份",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 1 THEN "金额" ELSE 0 END) AS "1月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 2 THEN "金额" ELSE 0 END) AS "2月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 3 THEN "金额" ELSE 0 END) AS "3月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 4 THEN "金额" ELSE 0 END) AS "4月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 5 THEN "金额" ELSE 0 END) AS "5月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 6 THEN "金额" ELSE 0 END) AS "6月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 7 THEN "金额" ELSE 0 END) AS "7月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 8 THEN "金额" ELSE 0 END) AS "8月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 9 THEN "金额" ELSE 0 END) AS "9月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 10 THEN "金额" ELSE 0 END) AS "10月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 11 THEN "金额" ELSE 0 END) AS "11月",
    SUM(CASE WHEN EXTRACT(MONTH FROM "日期") = 12 THEN "金额" ELSE 0 END) AS "12月"
FROM (
    SELECT a."预测层级",b."显示层级",a."一级科目",a."日期",a."金额"
    FROM "业报预测表" a
    JOIN "1-3一级科目" b
    ON a."一级科目"=b."一级科目")
GROUP BY "预测层级", EXTRACT(YEAR FROM "日期"),"一级科目","显示层级"
ORDER BY "年份" DESC,"预测层级" ASC,"显示层级" ASC""",
            ),
        ]

        custom_created = 0
        for view_name, view_sql in custom_views:
            try:
                cur.execute(view_sql)
                custom_created += 1
            except Exception as inner_e:
                print(f"[refresh_views] ERROR: 自定义视图 {view_name} 创建失败: {inner_e}")
                raise

        conn.commit()
        print(
            f"[refresh_views] 阶段2完成: 新建 {created} 个映射视图, 跳过 {skipped} 个无映射表, 新建 {custom_created} 个自定义视图"
        )
        if skipped_tables:
            print(f"[refresh_views] 跳过的表列表: {', '.join(skipped_tables)}")
        return {
            "success": True,
            "created": created,
            "skipped": skipped,
            "skipped_tables": skipped_tables,
            "custom_created": custom_created,
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
    对 FONE 相关视图（名称以 9-/7-/4-/1- 开头）授予 fone_group SELECT 权限

    Returns:
        {"success": bool, "granted": int, "views": List[str]}
    """
    from psycopg2 import sql

    conn = None
    cur = None
    try:
        conn, cur = connect_to_db()
        conn.autocommit = True

        cur.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'public'
              AND (table_name LIKE '9-%%'
                   OR table_name LIKE '7-%%'
                   OR table_name LIKE '4-%%'
                   OR table_name LIKE '1-%%')
            """
        )
        views = cur.fetchall()
        view_names = [v[0] for v in views]

        granted = 0
        failed_views: List[str] = []
        for view_name in view_names:
            grant_sql = sql.SQL("GRANT SELECT ON {} TO fone_group;").format(
                sql.Identifier(view_name)
            )
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
