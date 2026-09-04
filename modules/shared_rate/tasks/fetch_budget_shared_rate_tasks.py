"""获取预算综合比例的相关 Tasks"""
import os
import sys
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from prefect import task

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from mypackage.utilities import connect_to_db


@task(name="fetch_latest_budget_rate", log_prints=True)
def fetch_latest_budget_rate_task(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    从 bud_bus_shared_rate 获取最新月份（日为1号）的综合比例，并填充到整个日期范围
    """
    conn, cur = connect_to_db()

    try:
        # 1. 查找最新的一号日期
        query_latest_date = """
            SELECT MAX(report_date)
            FROM bud_bus_shared_rate
            WHERE EXTRACT(DAY FROM report_date) = 1
              AND indicator = '综合比例'
        """
        cur.execute(query_latest_date)
        latest_date = cur.fetchone()[0]

        if not latest_date:
            print(f"警告：未在 bud_bus_shared_rate 中找到任何'综合比例'记录！")
            return pd.DataFrame()

        print(f"获取到最新的比例日期为: {latest_date}")

        # 2. 获取该日期的所有比例
        query_rates = f"""
            SELECT bus_line, amt as rate
            FROM bud_bus_shared_rate
            WHERE report_date = '{latest_date}'
              AND indicator = '综合比例'
        """
        cur.execute(query_rates)
        df_latest = pd.DataFrame(cur.fetchall(), columns=["bus_line", "rate"])

        if df_latest.empty:
            print(f"警告：日期 {latest_date} 下没有找到比例数据。")
            return pd.DataFrame()

        # 3. 构造 1 号日期序列
        # 生成各月1号的列表
        date_list = []
        curr = start_date.replace(day=1)
        while curr <= end_date:
            date_list.append(curr)
            curr += relativedelta(months=1)

        # 4. 填充数据
        # 为每个日期生成一份最新的比例副本
        all_dfs = []
        for d in date_list:
            df_temp = df_latest.copy()
            df_temp["date"] = d
            all_dfs.append(df_temp)

        df_final = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

        # 将 rate 列的 null 自动填充为 0
        if not df_final.empty:
            df_final["rate"] = df_final["rate"].fillna(0)

        print(
            f"成功填充综合比例，从 {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}，共 {len(df_final)} 条记录。"
        )
        return df_final
    finally:
        cur.close()
        conn.close()


@task(name="update_fact_bus_shared_rate", log_prints=True)
def update_fact_bus_shared_rate_task(
    df_final: pd.DataFrame, start_date: datetime, end_date: datetime
) -> None:
    """
    将获取到的综合比例直接写入 fact_bus_shared_rate
    范围：今年1月1日 到 上一个月的最后一天
    """

    if start_date > end_date:
        print(
            f"计算出的日期范围无效 (起: {start_date.strftime('%Y-%m-%d')}, 止: {end_date.strftime('%Y-%m-%d')})，通常在1月发生。暂不更新。"
        )
        return

    conn, cur = connect_to_db()
    try:
        # 第一步：删除旧的重复区间数据
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        delete_query = f"DELETE FROM fact_bus_shared_rate WHERE date >= '{start_date_str}' AND date <= '{end_date_str}'"
        cur.execute(delete_query)
        deleted_count = cur.rowcount
        print(f"已删除 {start_date_str} 至 {end_date_str} 期间的历史记录 {deleted_count} 条。")
        conn.commit()
    except Exception as e:
        print(f"删除历史数据失败: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    if not df_final.empty:
        # 第二步：使用封装好的 add_data 插入新数据
        from mypackage.utilities import add_data

        # 确保 date 列如果是 datetime 对象可以转换为字符串
        df_final["date"] = pd.to_datetime(df_final["date"]).dt.strftime("%Y-%m-%d")
        add_data("fact_bus_shared_rate", df_final)
        print(f"已成功写入 {len(df_final)} 条新综合比例数据。")
    else:
        print("无有效数据插入。")
