#!/usr/bin/env python3
"""检查业务线拆分结果"""
import sys

sys.path.insert(0, "/root/prefect")

import pandas as pd
from mypackage.utilities import connect_to_db


def check_table(cur, table_name, date_filter="'2026-01-01' AND '2026-01-31'"):
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
        cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE acct_period BETWEEN {date_filter}")
        count = cur.fetchone()[0]

        # 获取样本数据
        cur.execute(f"SELECT * FROM {table_name} WHERE acct_period BETWEEN {date_filter} LIMIT 3")
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
        ("staging_bus_expense", "费用数据"),
        ("staging_bus_revenue", "收入数据"),
        ("staging_bus_profit_bd", "其他利润表数据"),
        ("staging_revenue_unassigned", "非业务线收入"),
        ("staging_expense_unassigned", "非业务线费用"),
        ("staging_profit_unassigned", "非业务线利润"),
        ("staging_bus_inventory", "存货数据"),
        ("staging_bus_receivable", "应收数据"),
        ("staging_bus_in_transit_inventory", "在途存货数据"),
    ]

    for table_name, desc in tables:
        print(f"\n【{desc}】表: {table_name}")
        result = check_table(cur, table_name)
        if not result["exists"]:
            print(f"  ⚠️ 表不存在")
        elif result.get("error"):
            print(f"  ❌ 查询错误: {result['error']}")
        else:
            print(f"  ✅ 记录数: {result['count']}")
            if result["sample"] is not None and not result["sample"].empty:
                print(f"  样本数据预览:")
                print(result["sample"].to_string(index=False))

    conn.close()
    print("\n" + "=" * 70)
