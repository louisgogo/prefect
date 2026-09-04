import pandas as pd
from mypackage.utilities import connect_to_db
from prefect import task


@task(name="5-收入比例自动填充", retries=1, log_prints=True)
def run_revenue_ratio_fill_task(date_range, batch_id):
    """
    计算当月国际业务中心(icbc_center)和国内市场中心(dcmc_center)的收入比例，
    并将该比例批量填入智造事业群-智造管理中心(smmc_center)和国际渠道事业群-运营中心(phoc_center)
    在 staging_bus_revenue 表中的收入记录。

    只更新"国际业务"和"国内硬件"两列，其他业务线列保持NULL不变。
    若当月两中心收入总和为0，则跳过该月不更新。
    """
    print("开始执行: 5-收入比例自动填充")

    conn, cur = connect_to_db()

    try:
        date_list = ",".join([f"'{d}'" for d in date_range])

        # 1. 计算国际业务中心和国内市场中心当月的收入总额
        print("正在计算国际业务中心和国内市场中心的收入比例...")
        cur.execute(
            f"""
            SELECT
                acct_period,
                SUM(CASE WHEN unique_lvl LIKE '%国际业务中心%' THEN amt_tax_exc_loc ELSE 0 END) AS intl_revenue,
                SUM(CASE WHEN unique_lvl LIKE '%国内市场中心%' THEN amt_tax_exc_loc ELSE 0 END) AS domestic_revenue
            FROM fact_revenue
            WHERE acct_period IN ({date_list})
            GROUP BY acct_period
            ORDER BY acct_period
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("未找到国际业务中心或国内市场中心的收入数据，跳过比例填充。")
            return

        df_ratio = pd.DataFrame(rows, columns=["会计期间", "国际业务收入", "国内市场收入"])
        df_ratio["总和"] = df_ratio["国际业务收入"] + df_ratio["国内市场收入"]

        update_count_total = 0

        for _, row in df_ratio.iterrows():
            acct_period = row["会计期间"]
            total = row["总和"]

            if total == 0:
                print(f"  {acct_period}: 两中心收入总和为0，跳过该月。")
                continue

            intl_rate = round(float(row["国际业务收入"]) / float(total), 2)
            domestic_rate = round(1.0 - intl_rate, 2)

            print(f"  {acct_period}: 国际业务={intl_rate:.2f}, 国内硬件={domestic_rate:.2f}")

            # 2. 仅给尚无任何比例的记录写入规范化比例长表。
            cur.execute(
                """
                INSERT INTO staging_bus_line_ratio(class, record_id, bus_line, rate)
                SELECT '收入', staging.record_id, ratio.bus_line, ratio.rate
                FROM staging_bus_revenue AS staging
                CROSS JOIN (VALUES (%s::text, %s::numeric), (%s::text, %s::numeric))
                    AS ratio(bus_line, rate)
                WHERE staging."会计期间" = %s
                  AND staging.batch_id = %s
                  AND (staging."唯一层级" LIKE '%%智造事业群-智造管理中心%%'
                       OR staging."唯一层级" LIKE '%%国际渠道事业群-运营中心%%')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM staging_bus_line_ratio AS existing
                      WHERE existing.class = '收入'
                        AND existing.record_id = staging.record_id
                  )
                ON CONFLICT (class, record_id, bus_line) DO NOTHING
                """,
                (
                    "国际业务",
                    intl_rate,
                    "国内硬件",
                    domestic_rate,
                    acct_period,
                    batch_id,
                ),
            )
            update_count_total += cur.rowcount // 2

        conn.commit()
        print(f"✅ 收入比例自动填充完成，共更新 {update_count_total} 条记录。")

    finally:
        cur.close()
        conn.close()
