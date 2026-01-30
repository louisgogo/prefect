"""日期处理工具函数"""
from datetime import datetime, timedelta
from typing import List, Union
import pandas as pd


def get_date_range_by_month(year: int, month: int) -> pd.DatetimeIndex:
    """
    获取指定年月的日期范围
    
    Args:
        year: 年份
        month: 月份 (1-12)
    
    Returns:
        该月的所有日期范围
    """
    # 获取该月的第一天
    first_day = datetime(year, month, 1)
    
    # 计算该月的最后一天
    if month == 12:
        last_day = datetime(year, month, 31)
    else:
        # 下个月的第一天减去1天
        next_month_first = datetime(year, month + 1, 1)
        last_day = next_month_first - timedelta(days=1)
    
    # 生成日期范围
    date_range = pd.date_range(start=first_day, end=last_day, freq='D')
    return date_range


def get_date_range_by_months(months: List[tuple]) -> pd.DatetimeIndex:
    """
    获取多个年月的日期范围（批量执行）
    
    Args:
        months: 月份列表，每个元素为 (year, month) 元组
    
    Returns:
        所有月份的日期范围合并
    """
    all_dates = []
    for year, month in months:
        date_range = get_date_range_by_month(year, month)
        all_dates.extend(date_range.tolist())
    
    # 去重并排序
    unique_dates = sorted(set(all_dates))
    return pd.DatetimeIndex(unique_dates)


def get_date_range_by_year_from_month(year: int, month: int) -> pd.DatetimeIndex:
    """
    从指定年月开始，获取从年初到该月的日期范围
    
    Args:
        year: 年份
        month: 月份 (1-12)
    
    Returns:
        从年初到该月的所有日期范围
    """
    first_day_of_year = datetime(year, 1, 1)
    
    # 计算该月的最后一天
    if month == 12:
        last_day = datetime(year, month, 31)
    else:
        next_month_first = datetime(year, month + 1, 1)
        last_day = next_month_first - timedelta(days=1)
    
    # 生成日期范围
    date_range = pd.date_range(start=first_day_of_year, end=last_day, freq='D')
    return date_range


def get_date_range_by_lastmonth() -> pd.DatetimeIndex:
    """
    获取上个月的日期范围（保持与原有逻辑兼容）
    
    Returns:
        上个月的所有日期范围
    """
    current_date = datetime.now().date()
    
    # 先获取本月的第一天
    first_day_of_current_month = current_date.replace(day=1)
    # 本月第一天往前推一天，即上个月的最后一天
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    
    # 上个月的第一天
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
    
    # 创建日期范围
    last_month_date_range = pd.date_range(
        start=first_day_of_previous_month,
        end=last_day_of_previous_month,
        freq='D'
    )
    
    return last_month_date_range
