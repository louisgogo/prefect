"""获取预算综合比例的相关 Tasks"""
from prefect import task
import pandas as pd
import sys
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from mypackage.utilities import connect_to_db

@task(name="fetch_latest_budget_rate", log_prints=True)
def fetch_latest_budget_rate_task(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    从 bud_bus_shared_rate 获取指定日期范围内的综合比例
    条件：日是1号，指标为'综合比例'
    """
    conn, cur = connect_to_db()
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    query = f"""
        SELECT bus_line, amt as rate, report_date as date
        FROM bud_bus_shared_rate
        WHERE EXTRACT(DAY FROM report_date) = 1
          AND indicator = '综合比例'
          AND report_date >= '{start_str}' 
          AND report_date <= '{end_str}'
    """
    try:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        if df.empty:
            print(f"警告：未在 {start_str} 至 {end_str} 期间找到任何综合比例记录！")
            return pd.DataFrame()
            
        # 将 rate 列的 null 自动填充为 0
        df['rate'] = df['rate'].fillna(0)
            
        print(f"成功获取 {start_str} 至 {end_str} 的综合比例，共 {len(df)} 条记录。")
        return df
    finally:
        cur.close()
        conn.close()

@task(name="update_fact_bus_shared_rate", log_prints=True)
def update_fact_bus_shared_rate_task(df_final: pd.DataFrame, start_date: datetime, end_date: datetime) -> None:
    """
    将获取到的综合比例直接写入 fact_bus_shared_rate
    范围：今年1月1日 到 上一个月的最后一天
    """
    
    if start_date > end_date:
        print(f"计算出的日期范围无效 (起: {start_date.strftime('%Y-%m-%d')}, 止: {end_date.strftime('%Y-%m-%d')})，通常在1月发生。暂不更新。")
        return

    conn, cur = connect_to_db()
    try:
        # 第一步：删除旧的重复区间数据
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
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
        df_final['date'] = pd.to_datetime(df_final['date']).dt.strftime('%Y-%m-%d')
        add_data('fact_bus_shared_rate', df_final)
        print(f"已成功写入 {len(df_final)} 条新综合比例数据。")
    else:
        print("无有效数据插入。")
