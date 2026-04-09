#!/usr/bin/env python3
"""检查业务线拆分结果 - 修复版"""
import sys

sys.path.insert(0, "/root/prefect")

import pandas as pd
from mypackage.utilities import connect_to_db


def check_table(cur, table_name, date_column, date_filter=("2026-01-01", "2026-01-31")):
    """检查指定表的数据量"""
    try:
        # 检查表是否存在
        cur.execute(
            f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = '{table_name}'
            )
        """
        )
        exists = cur.fetchone()[0]
        if not exists:
            return {"exists": False, "count": 0, "sample": None}

        # 获取记录数
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE {date_column} BETWEEN '{date_filter[0]}' AND '{date_filter[1]}'
        """
        )
        count = cur.fetchone()[0]

        # 获取样本数据
        cur.execute(
            f"""
            SELECT *
            FROM {table_name}
            WHERE {date_column} BETWEEN '{date_filter[0]}' AND '{date_filter[1]}'
            LIMIT 3
        """
        )
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        sample = pd.DataFrame(rows, columns=columns) if rows else None

        return {"exists": True, "count": count, "sample": sample}
    except Exception as e:
        return {"exists": True, "count": -1, "error": str(e), "sample": None}


if __name__ == "__main__":
    print("=" * 70)
    print("检查业务线拆分 Staging 表数据 (2026年1月)")
    print("=" * 70)

    conn, cur = connect_to_db()

    tables = [
        ("staging_bus_expense", "费用数据", "acct_period"),
        ("staging_bus_revenue", "收入数据", "acct_period"),
        ("staging_bus_profit_bd", "其他利润表数据", "date"),
        ("staging_revenue_unassigned", "非业务线收入", "acct_period"),
        ("staging_expense_unassigned", "非业务线费用", "acct_period"),
        ("staging_profit_unassigned", "非业务线利润", "date"),
        ("staging_bus_inventory", "存货数据", "acct_period"),
        ("staging_bus_receivable", "应收数据", "acct_period"),
        ("staging_bus_in_transit_inventory", "在途存货数据", "acct_period"),
    ]

    for table_name, desc, date_col in tables:
        print(f"\n【{desc}】表: {table_name}")
        result = check_table(cur, table_name, date_col)
        if not result["exists"]:
            print(f"  ⚠️ 表不存在")
        elif result.get("error"):
            print(f"  ❌ 查询错误: {result['error']}")
        else:
            print(f"  ✅ 记录数: {result['count']}")
            if result["sample"] is not None and not result["sample"].empty:
                print(f"  样本数据预览:")
                # 只显示关键列
                key_cols = [
                    c
                    for c in result["sample"].columns
                    if any(
                        k in c.lower()
                        for k in ["source", "unique", "acct", "date", "amt", "bus_line", "alloc"]
                    )
                ]
                if key_cols:
                    print(result["sample"][key_cols[:6]].to_string(index=False))
                else:
                    print(result["sample"].iloc[:, :6].to_string(index=False))

    conn.close()
    print("\n" + "=" * 70)
