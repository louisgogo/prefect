#!/usr/bin/env python3
"""检查业务线拆分表结构"""
import sys

sys.path.insert(0, "/root/prefect")

from mypackage.utilities import connect_to_db


def check_table_schema(cur, table_name):
    """检查指定表的列结构"""
    try:
        cur.execute(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        )
        columns = cur.fetchall()
        return columns
    except Exception as e:
        return [(f"Error: {e}", "")]


if __name__ == "__main__":
    print("=" * 70)
    print("检查业务线拆分 Staging 表结构")
    print("=" * 70)

    conn, cur = connect_to_db()

    tables = [
        "staging_bus_expense",
        "staging_bus_revenue",
        "staging_bus_profit_bd",
        "staging_revenue_unassigned",
        "staging_expense_unassigned",
        "staging_profit_unassigned",
        "staging_bus_inventory",
        "staging_bus_receivable",
        "staging_bus_in_transit_inventory",
    ]

    for table_name in tables:
        print(f"\n【{table_name}】")
        columns = check_table_schema(cur, table_name)
        if columns:
            for col_name, data_type in columns[:10]:  # 只显示前10列
                print(f"  - {col_name}: {data_type}")
            if len(columns) > 10:
                print(f"  ... 还有 {len(columns) - 10} 列")
        else:
            print("  ⚠️ 表不存在或无列信息")

    conn.close()
    print("\n" + "=" * 70)
