"""
Update existing staging_bus_in_transit_inventory records:
在途订单金额 = 未入库数量 * unit_price * exchange_rate (from fact_inventory_on_way)

This script only updates the amount column and leaves business-line ratio columns untouched.
"""
from mypackage.utilities import connect_to_db


def update_staging_in_transit_amount():
    conn, cur = connect_to_db()
    try:
        # 1. Identify rows where staging has no matching fact_inventory_on_way
        cur.execute(
            """
            SELECT COUNT(*) FROM staging_bus_in_transit_inventory s
            LEFT JOIN fact_inventory_on_way f
                ON s."来源编号" = f.source_no
            WHERE f.source_no IS NULL
            """
        )
        missing_match = cur.fetchone()[0]
        if missing_match:
            print(f"WARN: {missing_match} staging rows have no matching fact_inventory_on_way row")

        # 2. Identify rows where unit_price or exchange_rate is null
        cur.execute(
            """
            SELECT COUNT(*) FROM staging_bus_in_transit_inventory s
            JOIN fact_inventory_on_way f
                ON s."来源编号" = f.source_no
            WHERE f.unit_price IS NULL OR f.exchange_rate IS NULL
            """
        )
        null_components = cur.fetchone()[0]
        if null_components:
            print(f"WARN: {null_components} matched rows have NULL unit_price or exchange_rate")

        # 3. Perform the UPDATE only where all components are present
        cur.execute(
            """
            UPDATE staging_bus_in_transit_inventory s
            SET "在途订单金额" = ROUND(
                (s."未入库数量" * f.unit_price * f.exchange_rate)::numeric, 3
            )
            FROM fact_inventory_on_way f
            WHERE s."来源编号" = f.source_no
              AND f.unit_price IS NOT NULL
              AND f.exchange_rate IS NOT NULL
              AND s."未入库数量" IS NOT NULL
            """
        )
        updated = cur.rowcount
        conn.commit()
        print(f"Updated {updated} rows in staging_bus_in_transit_inventory.")
    except Exception as e:
        conn.rollback()
        print(f"Update failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    update_staging_in_transit_amount()
