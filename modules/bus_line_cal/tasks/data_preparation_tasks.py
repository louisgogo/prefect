"""数据准备相关 Tasks"""
from prefect import task
from typing import Tuple
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from mypackage.utilities import cal_person_weight, connect_to_db, add_data
from utils.date_utils import get_date_range_by_lastmonth


@task(name="calculate_person_weight", log_prints=True)
def calculate_person_weight_task() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    计算人数权重，返回业务线比例数据和组织架构数据
    
    Returns:
        Tuple[df_upload_merge_all, df_org]: 业务线比例数据和组织架构数据
    """
    try:
        df_upload_merge_all, df_org = cal_person_weight()
        print(f"人数权重计算完成，业务线数据行数: {len(df_upload_merge_all)}, 组织架构行数: {len(df_org)}")
        return df_upload_merge_all, df_org
    except Exception as e:
        print(f"计算人数权重时发生错误: {str(e)}")
        raise


@task(name="check_and_update_shared_rate", log_prints=True)
def check_and_update_shared_rate_task() -> None:
    """
    检查公摊费用比例表，如果上个月没有记录，则追加一条记录
    """
    try:
        conn, cur = connect_to_db()
        
        # 获取所有公摊费用比例数据
        cur.execute("SELECT * FROM fact_bus_shared_rate")
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        df['date'] = pd.to_datetime(df['date'])
        
        # 检查上个月是否有记录
        date_range_lastmonth = get_date_range_by_lastmonth()
        df_check = df[df['date'].isin(date_range_lastmonth)]
        
        print(f"检查公摊费用比例表，上个月记录数: {len(df_check)}")
        
        if df_check.empty:
            # 如果没有记录，则插入一条记录（使用最新日期的数据）
            df_max = df[df['date'] == df['date'].max()].copy()
            df_max['date'] = date_range_lastmonth[0].strftime('%Y-%m-%d')
            df_max = df_max.drop(['id'], axis=1)
            
            add_data('fact_bus_shared_rate', df_max)
            print('已追加上个月的公摊费用比例记录')
        else:
            print('上个月的公摊费用比例记录已存在，无需追加')
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"检查并更新公摊费用比例表时发生错误: {str(e)}")
        raise
