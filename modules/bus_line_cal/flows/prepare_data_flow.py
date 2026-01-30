"""数据准备流程"""
from prefect import flow
from typing import Tuple
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ..tasks.data_preparation_tasks import (
    calculate_person_weight_task,
    check_and_update_shared_rate_task,
)


@flow(name="prepare_data_flow", log_prints=True)
def prepare_data_flow() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    数据准备流程
    计算人数权重并检查公摊费用比例表
    
    Returns:
        Tuple[df_upload_merge_all, df_org]: 业务线比例数据和组织架构数据
    """
    print("开始数据准备流程...")
    
    # 计算人数权重
    df_upload_merge_all, df_org = calculate_person_weight_task()
    
    # 检查并更新公摊费用比例表
    check_and_update_shared_rate_task()
    
    print("数据准备流程完成")
    return df_upload_merge_all, df_org
