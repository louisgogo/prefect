"""AI数据ETL流程 - 为AI平台生成业务数据视图"""

import os
import sys
from typing import Literal

from prefect import flow

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
)
from ..tasks.ai_data_etl_tasks import create_ai_view_task, create_revenue_calc_view_task


@flow(name="ai_data_etl_flow", log_prints=True)
def ai_data_etl_flow(type: Literal["业务线数据", "业报数据"] = "业务线数据") -> None:
    """
    AI数据ETL流程 - 为AI平台生成业务数据视图

    负责：
    1. 创建"7-4收入计算表"视图（ai_bus_revenue的依赖）
    2. 创建6个AI数据视图（ai_bus_*, ai_bud_*）
    3. 支持'业务线数据'和'业报数据'两种模式

    Args:
        type: 数据类型，默认为'业务线数据'
            - '业务线数据': 使用业务线分表（fact_bus_*）
            - '业报数据': 使用业报主表（fact_*）

    Examples:
        # 业务线数据模式（默认）
        ai_data_etl_flow()

        # 业报数据模式
        ai_data_etl_flow(type='业报数据')
    """
    # 验证参数
    if type not in ["业务线数据", "业报数据"]:
        raise ValueError(f"type参数必须是'业务线数据'或'业报数据'，当前值: {type}")

    print(f"开始执行AI数据ETL流程，数据类型: {type}")
    print(f"{'='*60}")

    # ========== 第一步：创建依赖视图 "7-4收入计算表" ==========
    print("步骤1: 创建依赖视图'7-4收入计算表'（ai_bus_revenue依赖此视图）")
    try:
        create_revenue_calc_view_task()
        print("✓ '7-4收入计算表'视图创建成功")
    except Exception as e:
        print(f"✗ '7-4收入计算表'视图创建失败: {str(e)}")
        raise

    # ========== 第二步：定义表转换配置 ==========
    print(f"\n步骤2: 定义数据转换配置（{type}模式）")

    if type == "业务线数据":
        table_configs = [
            {
                "source": "fact_bus_revenue",
                "target": "ai_bus_revenue",
                "custom_sql": """
                      SELECT
                          fbr.*,
                          cg.cust_region AS temp_cust_region,
                          cg.cust_cat AS temp_cust_cat,
                          CASE
                              WHEN fbr.unique_lvl IS NOT NULL THEN split_part(fbr.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN fbr.unique_lvl IS NOT NULL THEN split_part(fbr.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN fbr.unique_lvl IS NOT NULL THEN split_part(fbr.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN fbr.mo_amt IS NOT NULL THEN fbr.mo_amt
                              ELSE NULL
                          END AS amt
                      FROM fact_bus_revenue fbr
                      LEFT JOIN fone_cust_group cg ON fbr.custgp_code = cg.custgp_code
                  """,
            },
            {
                "source": "fact_bus_profit",
                "target": "ai_bus_profit",
                "custom_sql": """
                      SELECT
                          fbp.*,
                          CASE
                              WHEN fbp.unique_lvl IS NOT NULL THEN split_part(fbp.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN fbp.unique_lvl IS NOT NULL THEN split_part(fbp.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN fbp.unique_lvl IS NOT NULL THEN split_part(fbp.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org
                      FROM fact_bus_profit fbp
                  """,
            },
            {
                "source": "fact_bus_expense",
                "target": "ai_bus_expense",
                "custom_sql": """
                      SELECT
                          fbe.*,
                          fbe.rd_proj AS project,
                          ei.exp_nature AS temp_exp_nature,
                          ei.exp_item AS temp_exp_item,
                          ei.exp_major_cat AS temp_exp_major_cat,
                          CASE
                              WHEN fbe.unique_lvl IS NOT NULL THEN split_part(fbe.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN fbe.unique_lvl IS NOT NULL THEN split_part(fbe.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN fbe.unique_lvl IS NOT NULL THEN split_part(fbe.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN fbe.exp_amt IS NOT NULL THEN fbe.exp_amt
                              ELSE NULL
                          END AS amt
                      FROM fact_bus_expense fbe
                      LEFT JOIN dim_exp_item ei ON fbe.exp_item_code = ei.encoding
                  """,
            },
            {
                "source": "bud_profit",
                "target": "ai_bud_profit",
                "custom_sql": """
                      SELECT
                          bp.*,
                          CASE
                              WHEN bp.unique_lvl IS NOT NULL THEN split_part(bp.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN bp.unique_lvl IS NOT NULL THEN split_part(bp.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN bp.unique_lvl IS NOT NULL THEN split_part(bp.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN bp.bud_sys_amt IS NOT NULL THEN bp.bud_sys_amt
                              ELSE NULL
                          END AS amt,
                          CASE
                              WHEN bp.date IS NOT NULL THEN bp.date
                          END AS acct_period
                      FROM bud_profit bp
                  """,
            },
            {
                "source": "bud_income",
                "target": "ai_bud_income",
                "custom_sql": """
                      SELECT
                          bi.*,
                          cg.cust_region AS temp_cust_region,
                          cg.cust_cat AS temp_cust_cat,
                          CASE
                              WHEN bi.unique_lvl IS NOT NULL THEN split_part(bi.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN bi.unique_lvl IS NOT NULL THEN split_part(bi.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN bi.unique_lvl IS NOT NULL THEN split_part(bi.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN bi.mo_amt IS NOT NULL THEN bi.mo_amt
                              ELSE NULL
                          END AS amt
                      FROM bud_income bi
                      LEFT JOIN fone_cust_group cg ON bi.custgp_code = cg.custgp_code
                  """,
            },
            {
                "source": "bud_expense",
                "target": "ai_bud_expense",
                "custom_sql": """
                      SELECT
                          be.*,
                          ei.exp_nature AS temp_exp_nature,
                          ei.exp_item AS temp_exp_item,
                          ei.exp_major_cat AS temp_exp_major_cat,
                          CASE
                              WHEN be.unique_lvl IS NOT NULL THEN split_part(be.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN be.unique_lvl IS NOT NULL THEN split_part(be.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN be.unique_lvl IS NOT NULL THEN split_part(be.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN be.bud_sys_amt IS NOT NULL THEN be.bud_sys_amt
                              ELSE NULL
                          END AS amt,
                          CASE
                              WHEN be.date IS NOT NULL THEN be.date
                          ELSE NULL
                          END AS acct_period
                      FROM bud_expense be
                      LEFT JOIN dim_exp_item ei ON be.exp_item_code = ei.encoding
                  """,
            },
        ]

    elif type == "业报数据":
        table_configs = [
            {
                "source": "7-4收入计算表",
                "target": "ai_bus_revenue",
                "custom_sql": """
                      SELECT
                          fbr.*,
                          cg.cust_region AS temp_cust_region,
                          cg.cust_cat AS temp_cust_cat,
                          CASE
                              WHEN fbr.unique_lvl IS NOT NULL THEN split_part(fbr.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN fbr.unique_lvl IS NOT NULL THEN split_part(fbr.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN fbr.unique_lvl IS NOT NULL THEN split_part(fbr.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          '无' AS bus_line
                      FROM "7-4收入计算表" fbr
                      LEFT JOIN fone_cust_group cg ON fbr.custgp_code = cg.custgp_code
                  """,
            },
            {
                "source": "fact_profit",
                "target": "ai_bus_profit",
                "custom_sql": """
                      SELECT
                          fbp.*,
                          CASE
                              WHEN fbp.unique_lvl IS NOT NULL THEN split_part(fbp.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN fbp.unique_lvl IS NOT NULL THEN split_part(fbp.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN fbp.unique_lvl IS NOT NULL THEN split_part(fbp.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          '无' AS bus_line
                      FROM fact_profit fbp
                  """,
            },
            {
                "source": "fact_expense",
                "target": "ai_bus_expense",
                "custom_sql": """
                      SELECT
                          fbe.*,
                          fbe.exp_amt AS amt,
                          fbe.rd_proj AS project,
                          ei.exp_nature AS temp_exp_nature,
                          ei.exp_item AS temp_exp_item,
                          ei.exp_major_cat AS temp_exp_major_cat,
                          CASE
                              WHEN fbe.unique_lvl IS NOT NULL THEN split_part(fbe.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN fbe.unique_lvl IS NOT NULL THEN split_part(fbe.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN fbe.unique_lvl IS NOT NULL THEN split_part(fbe.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          '无' AS bus_line
                      FROM fact_expense fbe
                      LEFT JOIN dim_exp_item ei ON fbe.exp_item_code = ei.encoding
                  """,
            },
            {
                "source": "bud_profit",
                "target": "ai_bud_profit",
                "custom_sql": """
                      SELECT
                          bp.*,
                          CASE
                              WHEN bp.unique_lvl IS NOT NULL THEN split_part(bp.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN bp.unique_lvl IS NOT NULL THEN split_part(bp.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN bp.unique_lvl IS NOT NULL THEN split_part(bp.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN bp.bud_sys_amt IS NOT NULL THEN bp.bud_sys_amt
                              ELSE NULL
                          END AS amt,
                          CASE
                              WHEN bp.date IS NOT NULL THEN bp.date
                          END AS acct_period
                      FROM bud_profit bp
                  """,
            },
            {
                "source": "bud_income",
                "target": "ai_bud_income",
                "custom_sql": """
                      SELECT
                          bi.*,
                          cg.cust_region AS temp_cust_region,
                          cg.cust_cat AS temp_cust_cat,
                          CASE
                              WHEN bi.unique_lvl IS NOT NULL THEN split_part(bi.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN bi.unique_lvl IS NOT NULL THEN split_part(bi.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN bi.unique_lvl IS NOT NULL THEN split_part(bi.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN bi.mo_amt IS NOT NULL THEN bi.mo_amt
                              ELSE NULL
                          END AS amt
                      FROM bud_income bi
                      LEFT JOIN fone_cust_group cg ON bi.custgp_code = cg.custgp_code
                  """,
            },
            {
                "source": "bud_expense",
                "target": "ai_bud_expense",
                "custom_sql": """
                      SELECT
                          be.*,
                          ei.exp_nature AS temp_exp_nature,
                          ei.exp_item AS temp_exp_item,
                          ei.exp_major_cat AS temp_exp_major_cat,
                          CASE
                              WHEN be.unique_lvl IS NOT NULL THEN split_part(be.unique_lvl, '-', 1)
                              ELSE NULL
                          END AS temp_prim_org,
                          CASE
                              WHEN be.unique_lvl IS NOT NULL THEN split_part(be.unique_lvl, '-', 2)
                              ELSE NULL
                          END AS temp_sec_org,
                          CASE
                              WHEN be.unique_lvl IS NOT NULL THEN split_part(be.unique_lvl, '-', 3)
                              ELSE NULL
                          END AS temp_third_org,
                          CASE
                              WHEN be.bud_sys_amt IS NOT NULL THEN be.bud_sys_amt
                              ELSE NULL
                          END AS amt,
                          CASE
                              WHEN be.date IS NOT NULL THEN be.date
                          ELSE NULL
                          END AS acct_period
                      FROM bud_expense be
                      LEFT JOIN dim_exp_item ei ON be.exp_item_code = ei.encoding
                  """,
            },
        ]

    print(f"共 {len(table_configs)} 个视图需要创建")

    # ========== 第三步：批量创建视图 ==========
    print(f"\n{'='*60}")
    print("步骤3: 开始批量创建AI数据视图")
    print(f"{'='*60}")

    success_count = 0
    failed_count = 0

    for idx, config in enumerate(table_configs, 1):
        source_table = config["source"]
        target_view = config["target"]
        custom_sql = config["custom_sql"]

        print(f"\n[{idx}/{len(table_configs)}] 创建视图: {source_table} -> {target_view}")

        try:
            create_ai_view_task(
                view_name=target_view, custom_sql=custom_sql, source_table=source_table
            )
            success_count += 1
            print(f"✓ 视图 {target_view} 创建成功")
        except Exception as e:
            failed_count += 1
            print(f"✗ 视图 {target_view} 创建失败: {str(e)}")
            raise

    # ========== 完成 ==========
    print(f"\n{'='*60}")
    print(f"AI数据ETL流程完成")
    print(f"成功创建: {success_count} 个视图")
    if failed_count > 0:
        print(f"失败: {failed_count} 个视图")
    print(f"{'='*60}")
