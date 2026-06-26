"""预算利润计算任务 - 融合收入、成本、费用表，重新计算利润指标"""

import os
import sys
from datetime import datetime
from typing import Optional

import pandas as pd

from prefect import task

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
)
from mypackage.utilities import connect_to_db, delete_data_add_data_by_DateRange


@task(name="load_budget_data", log_prints=True)
def load_budget_data_task(
    year: int,
    month: Optional[int] = None,
    budget_version: Optional[str] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    加载预算收入、费用、利润基础数据

    Args:
        year: 年份
        month: 月份（可选，不指定则加载全年）
        budget_version: 预算版本（如 '2026-07-01'），只加载每月 1 日的最终定稿版本

    Returns:
        (df_income, df_expense, df_profit_base) 三个DataFrame
    """
    print(f"开始加载预算数据，年份: {year}，月份: {month if month else '全年'}，版本: {budget_version or '全部每月1日版本'}")

    conn, cur = connect_to_db()

    try:
        # 构建日期条件
        if month:
            # 处理12月跨年问题
            if month == 12:
                next_year = year + 1
                next_month = 1
            else:
                next_year = year
                next_month = month + 1
            date_filter = (
                f"AND date >= '{year}-{month:02d}-01' AND date < '{next_year}-{next_month:02d}-01'"
            )
        else:
            date_filter = f"AND date >= '{year}-01-01' AND date < '{year+1}-01-01'"

        # 只取每月 1 日的最终定稿版本
        version_filter = "AND EXTRACT(DAY FROM report_date) = 1"
        if budget_version:
            version_filter += f" AND report_date = '{budget_version}'"

        # 1. 加载预算收入表 - 包含营业收入和营业成本
        print("加载预算收入表...")
        cur.execute(
            f"""
            SELECT
                id, org_id, org, cust_id, custgp_name,
                prod_major_cat_code, product, ind_id, indicator,
                date, mo_amt as amt, prim_subj, unique_lvl,
                cust_region, cust_cat, prod_major_cat, inc_major_cat,
                bus_line_code, bus_line, acct_period, custgp_code,
                report_date
            FROM bud_income
            WHERE prim_subj IN ('营业收入', '营业成本')
            {date_filter}
            {version_filter}
        """
        )
        df_income = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        print(f"✓ 预算收入表加载完成: {len(df_income)} 条")

        # 2. 加载预算费用表 - 包含管理费用、研发费用、销售费用、财务费用
        print("加载预算费用表...")
        cur.execute(
            f"""
            SELECT
                id, identifier, prim_org, sec_org, third_org,
                ind_code, prim_subj, project, exp_desc,
                acc_proj, exp_major_cat, date, unique_lvl,
                bud_sys_amt as amt, exp_item_code,
                bus_line_code, bus_line, proj_code,
                report_date
            FROM bud_expense
            WHERE prim_subj IN ('管理费用', '研发费用', '销售费用', '财务费用')
            {date_filter}
            {version_filter}
        """
        )
        df_expense = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        print(f"✓ 预算费用表加载完成: {len(df_expense)} 条")

        # 3. 加载预算利润基础表 - 排除需要重新计算的指标
        print("加载预算利润基础表（排除收入、成本、费用）...")
        cur.execute(
            f"""
            SELECT
                id, identifier, prim_org, sec_org, third_org,
                prim_subj, date, unique_lvl, bud_sys_amt as amt,
                bus_line, bus_line_code,
                report_date
            FROM bud_profit
            WHERE prim_subj NOT IN (
                '营业收入', '营业成本', '管理费用', '研发费用',
                '销售费用', '财务费用', '毛利润', '营业利润', '利润总额', '净利润'
            )
            {date_filter}
            {version_filter}
        """
        )
        df_profit_base = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        print(f"✓ 预算利润基础表加载完成: {len(df_profit_base)} 条")

        return df_income, df_expense, df_profit_base

    except Exception as e:
        print(f"加载预算数据失败: {e}")
        raise
    finally:
        cur.close()
        conn.close()


@task(name="merge_budget_data", log_prints=True)
def merge_budget_data_task(df_income: pd.DataFrame, df_expense: pd.DataFrame) -> pd.DataFrame:
    """
    融合预算收入表和费用表

    Args:
        df_income: 预算收入数据
        df_expense: 预算费用数据

    Returns:
        融合后的基础数据
    """
    print("开始融合预算收入、成本、费用数据...")

    try:
        # 标准化收入表结构
        income_cols = {
            "id": "source_id",
            "date": "date",
            "prim_subj": "prim_subj",
            "amt": "amt",
            "unique_lvl": "unique_lvl",
            "bus_line": "bus_line",
            "bus_line_code": "bus_line_code",
            "org": "org_name",
            "custgp_name": "cust_name",
            "product": "product_name",
            "indicator": "indicator_name",
            "report_date": "report_date",
        }

        # 选择并重命名收入表列
        df_income_std = df_income[list(income_cols.keys())].copy()
        df_income_std = df_income_std.rename(columns=income_cols)
        df_income_std["data_source"] = "income"

        # 标准化费用表结构
        expense_cols = {
            "id": "source_id",
            "date": "date",
            "prim_subj": "prim_subj",
            "amt": "amt",
            "unique_lvl": "unique_lvl",
            "bus_line": "bus_line",
            "bus_line_code": "bus_line_code",
            "prim_org": "org_name",
            "exp_desc": "cust_name",  # 费用描述作为名称
            "project": "product_name",
            "ind_code": "indicator_name",
            "report_date": "report_date",
        }

        # 选择并重命名费用表列
        df_expense_std = df_expense[list(expense_cols.keys())].copy()
        df_expense_std = df_expense_std.rename(columns=expense_cols)
        df_expense_std["data_source"] = "expense"

        # 合并收入表和费用表
        df_merged = pd.concat([df_income_std, df_expense_std], ignore_index=True)

        # 确保金额列为数值类型
        df_merged["amt"] = pd.to_numeric(df_merged["amt"], errors="coerce").fillna(0)

        # 转换日期格式
        df_merged["date"] = pd.to_datetime(df_merged["date"])

        print(f"✓ 预算数据融合完成: {len(df_merged)} 条")
        print(f"  - 收入/成本: {len(df_income_std)} 条")
        print(f"  - 费用: {len(df_expense_std)} 条")

        return df_merged

    except Exception as e:
        print(f"融合预算数据失败: {e}")
        raise


@task(name="calculate_budget_profit_indicators", log_prints=True)
def calculate_budget_profit_indicators_task(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    计算预算利润指标：毛利润、营业利润、利润总额、净利润

    Args:
        df_merged: 融合后的预算基础数据

    Returns:
        包含利润指标的完整预算数据
    """
    print("开始计算预算利润指标...")

    try:
        # 确保日期是datetime类型
        df_merged["date"] = pd.to_datetime(df_merged["date"])

        # 定义需要计算的维度列（分组键）
        group_cols = ["date", "bus_line", "bus_line_code", "unique_lvl", "report_date"]

        # 实际数（1-actual_through_month 月）从 fact_bus_* 读取，bus_line_code 可能为 NULL；
        # pivot_table 默认 dropna=True 会丢弃 index 中含 NaN 的组合，导致 1-5 月利润数据缺失。
        df_merged["bus_line_code"] = df_merged["bus_line_code"].fillna("UNKNOWN")

        # 按维度透视，计算各科目合计
        df_pivot = df_merged.pivot_table(
            index=group_cols, columns="prim_subj", values="amt", aggfunc="sum", fill_value=0
        ).reset_index()

        print(f"数据透视完成，维度组合数: {len(df_pivot)}")

        # 确保所有必需的科目列都存在（不存在则填充0）
        required_subjects = ["营业收入", "营业成本", "管理费用", "研发费用", "销售费用", "财务费用"]
        for subj in required_subjects:
            if subj not in df_pivot.columns:
                df_pivot[subj] = 0
                print(f"  补充科目列: {subj} = 0")

        # 计算利润指标
        print("计算利润指标...")

        # 1. 毛利润 = 营业收入 - 营业成本
        df_pivot["毛利润"] = df_pivot["营业收入"] - df_pivot["营业成本"]

        # 2. 营业利润 = 毛利润 - 税金及附加 - 销售费用 - 管理费用 - 研发费用 - 财务费用 + 其他收益 + 投资收益等
        # 简化版：营业利润 = 营业收入 - 营业成本 - 销售费用 - 管理费用 - 研发费用 - 财务费用
        df_pivot["营业利润"] = (
            df_pivot["营业收入"]
            - df_pivot["营业成本"]
            - df_pivot.get("税金及附加", 0)
            - df_pivot["销售费用"]
            - df_pivot["管理费用"]
            - df_pivot["研发费用"]
            - df_pivot["财务费用"]
        )

        # 3. 利润总额 = 营业利润 + 营业外收入 - 营业外支出 + 其他收益等
        # 简化版：利润总额 = 营业利润
        df_pivot["利润总额"] = df_pivot["营业利润"] + df_pivot.get("营业外收入", 0) - df_pivot.get("营业外支出", 0)

        # 4. 净利润 = 利润总额 - 所得税费用
        df_pivot["净利润"] = df_pivot["利润总额"] - df_pivot.get("所得税费用", 0)

        print(f"✓ 利润指标计算完成")
        print(f"  - 毛利润合计: {df_pivot['毛利润'].sum():,.2f}")
        print(f"  - 营业利润合计: {df_pivot['营业利润'].sum():,.2f}")
        print(f"  - 利润总额合计: {df_pivot['利润总额'].sum():,.2f}")
        print(f"  - 净利润合计: {df_pivot['净利润'].sum():,.2f}")

        # 将利润指标逆透视为长表格式
        profit_indicators = [
            "营业收入",
            "营业成本",
            "管理费用",
            "研发费用",
            "销售费用",
            "财务费用",
            "毛利润",
            "营业利润",
            "利润总额",
            "净利润",
        ]

        id_vars = group_cols.copy()
        df_melted = pd.melt(
            df_pivot,
            id_vars=id_vars,
            value_vars=profit_indicators,
            var_name="prim_subj",
            value_name="amt",
        )

        # 添加标准字段
        df_melted["source_id"] = "CALC_" + df_melted.index.astype(str)
        df_melted["data_source"] = "calculated"
        df_melted["org_name"] = "预算计算"
        df_melted["cust_name"] = "系统自动"

        # 重新排列列顺序
        final_cols = [
            "source_id",
            "date",
            "prim_subj",
            "amt",
            "unique_lvl",
            "bus_line",
            "bus_line_code",
            "org_name",
            "cust_name",
            "data_source",
            "report_date",
        ]
        df_melted = df_melted[final_cols]

        print(f"✓ 数据逆透视完成，输出记录数: {len(df_melted)}")

        return df_melted

    except Exception as e:
        print(f"计算预算利润指标失败: {e}")
        raise


@task(name="save_budget_profit", log_prints=True)
def save_budget_profit_task(
    df_calculated: pd.DataFrame,
    year: int,
    month: Optional[int] = None,
    budget_version: Optional[str] = None,
) -> str:
    """
    保存计算后的预算利润数据到数据库

    Args:
        df_calculated: 计算后的预算利润数据
        year: 年份
        month: 月份（可选）
        budget_version: 预算版本（可选），用于精确删除该版本的历史数据

    Returns:
        保存的表名
    """
    table_name = "bud_profit_calc"
    print(f"开始保存预算利润计算结果到 {table_name} 表...")

    try:
        # 准备要保存的数据
        df_save = df_calculated.copy()

        # 添加计算时间戳
        df_save["calc_time"] = datetime.now()

        # 确保金额不为null
        df_save["amt"] = df_save["amt"].fillna(0)

        # 先确保表存在
        conn, cur = connect_to_db()
        try:
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    source_id TEXT,
                    date DATE,
                    prim_subj TEXT,
                    amt NUMERIC(20,2),
                    unique_lvl TEXT,
                    bus_line TEXT,
                    bus_line_code TEXT,
                    org_name TEXT,
                    cust_name TEXT,
                    data_source TEXT,
                    report_date DATE,
                    calc_time TIMESTAMP
                )
            """
            cur.execute(create_table_sql)
            # 兼容旧表：添加 report_date 列
            cur.execute(
                f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS report_date DATE
            """
            )
            conn.commit()
            print(f"  表 {table_name} 检查/创建完成")
        finally:
            cur.close()
            conn.close()

        # 删除该月份/版本的历史计算数据（避免重复）
        conn, cur = connect_to_db()
        try:
            where_parts = [f"EXTRACT(YEAR FROM date) = {year}"]
            if month:
                where_parts.append(f"EXTRACT(MONTH FROM date) = {month}")
            if budget_version:
                where_parts.append(f"report_date = '{budget_version}'")

            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE {' AND '.join(where_parts)}
            """

            cur.execute(delete_sql)
            deleted_rows = cur.rowcount
            conn.commit()
            print(f"  已删除历史数据: {deleted_rows} 条")
        finally:
            cur.close()
            conn.close()

        # 使用COPY方式保存数据（性能更好，且使用已有连接）
        from io import StringIO

        conn, cur = connect_to_db()
        try:
            # 准备数据，只保留需要的列，并添加calc_time
            columns = [
                "source_id",
                "date",
                "prim_subj",
                "amt",
                "unique_lvl",
                "bus_line",
                "bus_line_code",
                "org_name",
                "cust_name",
                "data_source",
                "report_date",
            ]

            # 转换日期格式为字符串
            df_save["date"] = df_save["date"].dt.strftime("%Y-%m-%d")
            df_save["report_date"] = pd.to_datetime(df_save["report_date"]).dt.strftime("%Y-%m-%d")
            df_save["calc_time_str"] = df_save["calc_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # 使用COPY批量插入
            buffer = StringIO()
            for _, row in df_save.iterrows():
                line = (
                    "\t".join(
                        [
                            str(row["source_id"]),
                            str(row["date"]),
                            str(row["prim_subj"]),
                            str(row["amt"]),
                            str(row["unique_lvl"]) if pd.notna(row["unique_lvl"]) else "",
                            str(row["bus_line"]) if pd.notna(row["bus_line"]) else "",
                            str(row["bus_line_code"]) if pd.notna(row["bus_line_code"]) else "",
                            str(row["org_name"]) if pd.notna(row["org_name"]) else "",
                            str(row["cust_name"]) if pd.notna(row["cust_name"]) else "",
                            str(row["data_source"]),
                            str(row["report_date"]) if pd.notna(row["report_date"]) else "",
                            str(row["calc_time_str"]),
                        ]
                    )
                    + "\n"
                )
                buffer.write(line)

            buffer.seek(0)
            cur.copy_from(buffer, table_name, columns=columns + ["calc_time"])
            conn.commit()

            print(f"✓ 预算利润数据保存完成: {len(df_save)} 条")
        finally:
            cur.close()
            conn.close()
        return table_name

    except Exception as e:
        print(f"保存预算利润数据失败: {e}")
        raise


@task(name="create_budget_profit_view", log_prints=True)
def create_budget_profit_view_task(year: int, month: Optional[int] = None) -> bool:
    """
    创建预算利润视图，融合原始利润数据和计算数据

    Args:
        year: 年份
        month: 月份（可选）

    Returns:
        是否成功创建
    """
    view_name = "ai_bud_profit_calc"
    print(f"开始创建预算利润视图 {view_name}...")

    # 构建日期条件
    if month:
        date_condition = f"EXTRACT(YEAR FROM date) = {year} AND EXTRACT(MONTH FROM date) = {month}"
    else:
        date_condition = f"EXTRACT(YEAR FROM date) = {year}"

    view_sql = f"""
        SELECT
            calc.source_id as identifier,
            calc.date as date,
            calc.prim_subj as prim_subj,
            calc.amt as bud_sys_amt,
            calc.unique_lvl as unique_lvl,
            calc.bus_line as bus_line,
            calc.bus_line_code as bus_line_code,
            calc.org_name as prim_org,
            calc.org_name as sec_org,
            calc.org_name as third_org,
            calc.report_date as report_date,
            '计算' as data_type,
            calc.calc_time as calc_time
        FROM bud_profit_calc calc
        WHERE calc.date IS NOT NULL AND {date_condition.replace('date', 'calc.date')}
    """

    conn, cur = connect_to_db()

    try:
        # 删除旧视图
        cur.execute(f"DROP VIEW IF EXISTS {view_name} CASCADE")
        print(f"  已删除旧视图 {view_name}")

        # 创建新视图
        create_sql = f"CREATE VIEW {view_name} AS {view_sql}"
        cur.execute(create_sql)
        conn.commit()

        print(f"✓ 视图 {view_name} 创建成功")
        return True

    except Exception as e:
        conn.rollback()
        print(f"创建视图失败: {e}")
        raise
    finally:
        cur.close()
        conn.close()
