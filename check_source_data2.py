#!/usr/bin/env python3
"""检查源表数据情况 - 使用正确列名"""
import sys

sys.path.insert(0, "/root/prefect")

from mypackage.mapping import reverse_combined_column_mapping
from mypackage.utilities import connect_to_db


def check_source_table(cur, table_name, date_column_cn):
    """检查源表的数据量"""
    try:
        # 获取列名
        cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
        columns = [desc[0] for desc in cur.description]

        # 找到日期列（可能是中文映射后的）
        date_col = None
        for col in columns:
            mapped = reverse_combined_column_mapping.get(col, col)
            if mapped == date_column_cn:
                date_col = col
                break

        if not date_col:
            date_col = columns[5] if len(columns) > 5 else columns[0]  # 默认取第6列

        # 获取总记录数
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = cur.fetchone()[0]

        # 获取2026年1月记录数
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE {date_col} BETWEEN '2026-01-01' AND '2026-01-31'
        """
        )
        jan2026 = cur.fetchone()[0]

        # 获取日期范围
        cur.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table_name}")
        date_range = cur.fetchone()

        return {"total": total, "jan2026": jan2026, "range": date_range, "date_col": date_col}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    print("=" * 70)
    print("检查源表数据情况")
    print("=" * 70)

    conn, cur = connect_to_db()

    tables = [
        ("fact_expense", "会计期间"),
        ("fact_revenue", "会计期间"),
        ("fact_profit_bd", "日期"),
        ("fact_inventory", "会计期间"),
        ("fact_receivable", "会计期间"),
        ("fact_inventory_on_way", "会计期间"),
        ("fact_personnel", "日期"),
        ("fact_bus_wage_rate", "日期"),
    ]

    for table_name, date_col_cn in tables:
        print(f"\n【{table_name}】")
        result = check_source_table(cur, table_name, date_col_cn)
        if result.get("error"):
            print(f"  ❌ 错误: {result['error']}")
        else:
            print(f"  日期列: {result['date_col']}")
            print(f"  总记录数: {result['total']:,}")
            print(f"  2026年1月: {result['jan2026']:,}")
            print(f"  日期范围: {result['range'][0]} ~ {result['range'][1]}")

    conn.close()
    print("\n" + "=" * 70)
