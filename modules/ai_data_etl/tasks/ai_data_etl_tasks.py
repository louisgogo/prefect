"""AI数据ETL任务 - 创建视图和数据转换"""

import os
import sys
from typing import Dict, List

from prefect import task

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
)
from mypackage.utilities import connect_to_db


@task(name="create_revenue_calc_view_task")
def create_revenue_calc_view_task() -> bool:
    """
    创建"7-4收入计算表"视图（ai_bus_revenue的依赖视图）

    Returns:
        bool: 是否成功创建
    """
    print("开始创建'7-4收入计算表'视图（ai_bus_revenue的依赖视图）...")

    revenue_calc_view_sql = """
        SELECT
            *,
            SUM(COALESCE(amt_tax_exc_loc, 0)) AS amt,
            '营业收入' AS prim_subj
        FROM fact_revenue
        GROUP BY fact_revenue.id
        UNION ALL
        SELECT
            *,
            SUM(COALESCE(cost_amt, 0) + COALESCE(freight_cost, 0) + COALESCE(soft_cost, 0) + COALESCE(tariff_cost, 0)) AS amt,
            '营业成本' AS prim_subj
        FROM fact_revenue
        GROUP BY fact_revenue.id
    """

    conn, cur = connect_to_db()

    try:
        cur.execute('DROP VIEW IF EXISTS "7-4收入计算表" CASCADE')
        print("已删除旧视图 '7-4收入计算表'（如果存在）")

        create_sql = f'CREATE VIEW "7-4收入计算表" AS {revenue_calc_view_sql}'
        cur.execute(create_sql)

        conn.commit()
        print("'7-4收入计算表'视图创建成功")
        return True

    except Exception as e:
        conn.rollback()
        print(f"创建'7-4收入计算表'视图失败: {e}")
        raise Exception(f"创建'7-4收入计算表'视图失败，无法继续: {e}")
    finally:
        cur.close()
        conn.close()


@task(name="create_ai_view_task")
def create_ai_view_task(view_name: str, custom_sql: str, source_table: str) -> bool:
    """
    使用自定义SQL创建AI数据视图（支持复杂字段转换和临时字段清理）

    Args:
        view_name: 视图名称（如 ai_bus_revenue）
        custom_sql: 自定义SQL查询语句（可以包含temp_前缀字段）
        source_table: 源表名（用于日志）

    Returns:
        bool: 是否成功创建
    """
    print(f"开始创建视图: {source_table} -> {view_name}")

    conn, cur = connect_to_db()

    try:
        temp_sql = f"SELECT * FROM ({custom_sql}) AS temp_query LIMIT 0"
        cur.execute(temp_sql)

        columns = [desc[0] for desc in cur.description] if cur.description else []

        if not columns:
            raise Exception("无法获取查询字段信息")

        final_fields = []
        final_fields_set = set()

        temp_fields_map = {}
        for col in columns:
            if col.startswith("temp_"):
                final_name = col[5:]
                temp_fields_map[final_name] = col
                final_fields_set.add(final_name)

        for final_name, temp_col in temp_fields_map.items():
            final_fields.append(f"source_query.{temp_col} AS {final_name}")

        for col in columns:
            if not col.startswith("temp_") and col not in final_fields_set:
                if col != "rd_proj":
                    final_fields.append(f"source_query.{col}")
                    final_fields_set.add(col)

        final_select = "SELECT " + ",\n                          ".join(final_fields)

        drop_view_sql = f"DROP VIEW IF EXISTS {view_name} CASCADE"
        try:
            cur.execute(drop_view_sql)
            print(f"已删除旧视图 {view_name}（如果存在）")
        except Exception as drop_error:
            print(f"删除视图 {view_name} 时出现错误（可能视图不存在）: {drop_error}")

        final_sql = f"""CREATE VIEW {view_name} AS
                          {final_select}
                          FROM ({custom_sql}) AS source_query"""

        cur.execute(final_sql)
        conn.commit()

        print(f"视图 {view_name} 创建成功，字段数: {len(final_fields)}")
        return True

    except Exception as e:
        conn.rollback()
        print(f"创建视图 {view_name} 失败: {e}")
        raise Exception(f"创建视图 {view_name} 失败: {e}")
    finally:
        cur.close()
        conn.close()
