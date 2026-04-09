#!/usr/bin/env python3
"""分析性能瓶颈"""
import sys

sys.path.insert(0, "/root/prefect")

import time

import pandas as pd
from mypackage.utilities import connect_to_db, engine_to_db


# 检查表索引
def check_indexes():
    conn, cur = connect_to_db()
    print("=" * 70)
    print("检查 Staging 表索引")
    print("=" * 70)

    tables = [
        "staging_bus_expense",
        "staging_bus_revenue",
        "staging_bus_profit_bd",
        "staging_bus_inventory",
        "staging_bus_receivable",
        "staging_bus_in_transit_inventory",
    ]

    for table in tables:
        print(f"\n【{table}】")
        cur.execute(
            f"""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = '{table}'
        """
        )
        indexes = cur.fetchall()
        if indexes:
            for idx_name, idx_def in indexes:
                print(f"  - {idx_name}")
        else:
            print("  ⚠️ 无索引")

    conn.close()


def check_data_volume():
    print("\n" + "=" * 70)
    print("预估数据量（2026年1月）")
    print("=" * 70)

    conn, cur = connect_to_db()

    # 费用数据
    cur.execute(
        "SELECT COUNT(*) FROM fact_expense WHERE acct_period BETWEEN '2026-01-01' AND '2026-01-31'"
    )
    expense_count = cur.fetchone()[0]
    print(f"\n费用数据: {expense_count:,} 条")
    print(f"  打竖后预估: {expense_count * 15:,} 条 (15条业务线)")

    # 收入数据
    cur.execute(
        "SELECT COUNT(*) FROM fact_revenue WHERE acct_period BETWEEN '2026-01-01' AND '2026-01-31'"
    )
    revenue_count = cur.fetchone()[0]
    print(f"\n收入数据: {revenue_count:,} 条")
    print(f"  打竖后预估: {revenue_count * 15:,} 条")

    # 存货数据
    cur.execute(
        "SELECT COUNT(*) FROM fact_inventory WHERE acct_period BETWEEN '2026-01-01' AND '2026-01-31'"
    )
    inv_count = cur.fetchone()[0]
    print(f"\n存货数据: {inv_count:,} 条")
    print(f"  打竖后预估: {inv_count * 15:,} 条")

    conn.close()


def test_insert_speed():
    print("\n" + "=" * 70)
    print("测试插入速度")
    print("=" * 70)

    engine = engine_to_db()

    # 创建测试数据
    test_df = pd.DataFrame(
        {
            "source_no": ["TEST"] * 1000,
            "acct_period": ["2026-01-01"] * 1000,
            "bus_line": ["TEST"] * 1000,
            "rate": [0.5] * 1000,
        }
    )

    # 测试不同插入方式
    print("\n测试插入 1000 条数据:")

    # 方法1: 默认 to_sql
    start = time.time()
    test_df.to_sql("staging_bus_expense", con=engine, if_exists="append", index=False)
    print(f"  默认 to_sql: {time.time() - start:.2f} 秒")

    # 方法2: method='multi'
    start = time.time()
    test_df.to_sql(
        "staging_bus_expense", con=engine, if_exists="append", index=False, method="multi"
    )
    print(f"  method='multi': {time.time() - start:.2f} 秒")

    # 方法3: chunksize
    start = time.time()
    test_df.to_sql(
        "staging_bus_expense", con=engine, if_exists="append", index=False, chunksize=100
    )
    print(f"  chunksize=100: {time.time() - start:.2f} 秒")

    # 清理测试数据
    conn, cur = connect_to_db()
    cur.execute("DELETE FROM staging_bus_expense WHERE source_no = 'TEST'")
    conn.commit()
    conn.close()
    print("\n测试数据已清理")


if __name__ == "__main__":
    check_indexes()
    check_data_volume()
    # test_insert_speed()  # 可选：测试插入速度
