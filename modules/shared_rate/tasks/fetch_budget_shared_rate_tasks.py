"""获取预算综合比例的相关 Tasks"""
from prefect import task
import pandas as pd
import sys
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from mypackage.utilities import connect_to_db, execute_db

@task(name="fetch_latest_budget_rate", log_prints=True)
def fetch_latest_budget_rate_task() -> pd.DataFrame:
    """
    从 bud_bus_shared_rate 获取最新的综合比例
    条件：日是1号，指标为'综合比例'，选择最大日期
    """
    conn, cur = connect_to_db()
    
    query = """
        SELECT bus_line, amt as rate, report_date
        FROM bud_bus_shared_rate
        WHERE EXTRACT(DAY FROM report_date) = 1
          AND indicator = '综合比例'
    """
    try:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        if df.empty:
            print("警告：未在 bud_bus_shared_rate 中找到任何综合比例记录！")
            return pd.DataFrame()
            
        # 找到最大日期，只保留该日期的记录
        max_date = df['report_date'].max()
        df_latest = df[df['report_date'] == max_date].copy()
        print(f"成功获取 {max_date} 的最新综合比例，共 {len(df_latest)} 条记录。")
        return df_latest
    finally:
        cur.close()
        conn.close()

@task(name="update_fact_bus_shared_rate", log_prints=True)
def update_fact_bus_shared_rate_task(df_rates: pd.DataFrame) -> None:
    """
    将获取到的综合比例展开到目标日期范围并写入 fact_bus_shared_rate
    范围：今年1月1日 到 上一个月的最后一天
    """
    if df_rates.empty:
        print("无有效数据写入 fact_bus_shared_rate。")
        return
        
    today = datetime.now()
    # 范围起：今年年初
    start_date = datetime(today.year, 1, 1)
    # 范围止：上个月最后一天
    # 算法：当前日期的 1号 减去 1天
    first_day_this_month = datetime(today.year, today.month, 1)
    end_date = first_day_this_month - relativedelta(days=1)
    
    if start_date > end_date:
        print(f"计算出的日期范围无效 (起: {start_date.strftime('%Y-%m-%d')}, 止: {end_date.strftime('%Y-%m-%d')})，通常在1月发生。暂不更新。")
        return

    # 生成日期序列
    date_range = pd.date_range(start=start_date, end=end_date)
    print(f"准备更新 fact_bus_shared_rate 日期范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    
    # 将 df_rates 扩展到每一天
    # 目前 df_rates 包含 bus_line 和 rate，我们需要交叉合并日期
    df_dates = pd.DataFrame({'date': date_range})
    df_dates['key'] = 1
    df_rates['key'] = 1
    
    df_final = pd.merge(df_dates, df_rates, on='key').drop('key', axis=1)
    # df_final 现在的列：date, bus_line, rate, report_date
    df_final = df_final[['date', 'bus_line', 'rate']]
    
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
        
    # 第二步：使用封装好的 add_data 插入新数据
    from mypackage.utilities import add_data
    df_final['date'] = df_final['date'].dt.strftime('%Y-%m-%d') # 确保日期格式为字符串
    add_data('fact_bus_shared_rate', df_final)
    print(f"已成功写入 {len(df_final)} 条新综合比例数据。")
