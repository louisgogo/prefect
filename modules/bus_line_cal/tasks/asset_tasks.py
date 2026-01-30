"""资产明细生成相关 Tasks（应收、存货、在途存货）"""
from prefect import task
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from mypackage.utilities import connect_to_db, val_dist, delete_data_add_data_by_DateRange


@task(name="load_receivable_data", log_prints=True)
def load_receivable_data_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从数据库加载应收数据
    
    Args:
        date_range: 日期范围
    
    Returns:
        应收数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute("SELECT * FROM fact_receivable WHERE unique_lvl NOT LIKE '%无归属%'")
        df_ar = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        # 按照日期筛选数据范围
        df_ar['acct_period'] = pd.to_datetime(df_ar['acct_period'])
        df_ar = df_ar[df_ar['acct_period'].isin(date_range)]
        
        print(f"加载应收数据完成，共 {len(df_ar)} 条记录")
        cur.close()
        conn.close()
        return df_ar
    except Exception as e:
        print(f"加载应收数据时发生错误: {str(e)}")
        raise


@task(name="process_receivable", log_prints=True)
def process_receivable_task(
    df_ar: pd.DataFrame,
    df_bus_line: pd.DataFrame,
    df_org: pd.DataFrame
) -> pd.DataFrame:
    """
    处理应收数据并应用业务线比例
    
    Args:
        df_ar: 应收数据
        df_bus_line: 业务线比例数据
        df_org: 组织架构数据
    
    Returns:
        处理后的应收数据
    """
    try:
        df_ar_bus = df_bus_line[df_bus_line['class'] == '应收']
        df_ar_bus_list = df_ar_bus['source_no'].tolist()
        
        # 手工分拆的部分
        df_ar_bus_hand = df_ar[df_ar['source_no'].isin(df_ar_bus_list)].merge(
            df_ar_bus[['source_no', 'bus_line', 'unique_lvl', 'category', 'rate']],
            on=['source_no'],
            how='left'
        )
        
        # 需要应用比例的数值列
        ar_num_columns = [
            "unaudited_sales_amt", "unaudited_prepay_amt", "unaudited_inst_amt", "ar_balance", "ovd_amt",
            "undue_amt", "ovd_30d_less_amt", "ovd_30_90d_amt", "ovd_90_180d_amt", "ovd_180_360d_amt",
            "ovd_360d_plus_amt", "acct_age_3m_less", "acct_age_3_6m", "acct_age_6_9m", "acct_age_9_12m",
            "acct_age_1_2y", "acct_age_2_3y", "acct_age_3y_plus",
            "yr_debit_occ", "yr_credit_occ", "last_mo_ovd_amt", "ovd_change", "yr_repay_amt"
        ]
        
        for col in ar_num_columns:
            if col in df_ar_bus_hand.columns:
                print(f'正在转化的列: {col}')
                df_ar_bus_hand[col] = df_ar_bus_hand[col].astype(float)
                df_ar_bus_hand[col] = df_ar_bus_hand[col] * df_ar_bus_hand['rate']
        
        df_ar_bus_hand = df_ar_bus_hand.drop(['id'], axis=1, errors='ignore')
        df_ar_bus_hand = df_ar_bus_hand.rename(
            columns={'unique_lvl_x': 'source_lvl', 'unique_lvl_y': 'unique_lvl'}
        )
        # 删除空列或全 NA 列，避免拼接警告
        df_ar_bus_hand = df_ar_bus_hand.dropna(axis=1, how='all')
        
        # 自动归属的部分
        df_ar_bus_auto = df_ar[~df_ar['source_no'].isin(df_ar_bus_list)].merge(
            df_org[['unique_lvl', 'bus_line']],
            on=['unique_lvl'],
            how='left'
        )
        df_ar_bus_auto['source_lvl'] = df_ar_bus_auto['unique_lvl']
        df_ar_bus_auto['rate'] = 1
        df_ar_bus_auto['category'] = df_ar_bus_auto['bus_line'].apply(
            lambda x: '无' if x == '无' else '直接归属'
        )
        # 删除空列或全 NA 列，避免拼接警告
        df_ar_bus_auto = df_ar_bus_auto.dropna(axis=1, how='all')
        
        # 确保两个 DataFrame 有相同的列，缺失的列用 NaN 填充
        all_columns = list(set(df_ar_bus_hand.columns) | set(df_ar_bus_auto.columns))
        df_ar_bus_hand = df_ar_bus_hand.reindex(columns=all_columns)
        df_ar_bus_auto = df_ar_bus_auto.reindex(columns=all_columns)
        
        df_ar_bus_all = pd.concat([df_ar_bus_hand, df_ar_bus_auto], ignore_index=True)
        df_ar_bus_all = df_ar_bus_all.drop(columns=['id'], axis=1, errors='ignore')
        
        print(f"处理应收数据完成，共 {len(df_ar_bus_all)} 条记录")
        return df_ar_bus_all
    except Exception as e:
        print(f"处理应收数据时发生错误: {str(e)}")
        raise


@task(name="validate_receivable_rate", log_prints=True)
def validate_receivable_rate_task(df: pd.DataFrame) -> None:
    """
    验证应收数据比率
    
    Args:
        df: 应收数据
    """
    try:
        col_agg_name = ['source_no']
        col_sum_name = 'rate'
        val_dist(df, col_agg_name, col_sum_name)
        print("应收数据比率验证完成")
    except Exception as e:
        print(f"验证应收数据比率时发生错误: {str(e)}")
        raise


@task(name="save_receivable_detail", log_prints=True)
def save_receivable_detail_task(
    df: pd.DataFrame,
    date_range: pd.DatetimeIndex
) -> None:
    """
    保存应收明细到数据库
    
    Args:
        df: 应收数据
        date_range: 日期范围
    """
    try:
        table_name = 'fact_bus_receivable'
        date_column = 'acct_period'
        df = df.rename(columns={'unique_lvl': 'sec_dist_lvl', 'source_lvl': 'unique_lvl'})
        df_date_column = 'acct_period'
        
        delete_data_add_data_by_DateRange(
            table_name, date_column, df, df_date_column, date_range
        )
        print(f"保存应收明细到数据库完成，共 {len(df)} 条记录")
    except Exception as e:
        print(f"保存应收明细到数据库时发生错误: {str(e)}")
        raise


@task(name="load_inventory_data", log_prints=True)
def load_inventory_data_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从数据库加载存货数据
    
    Args:
        date_range: 日期范围
    
    Returns:
        存货数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute("SELECT * FROM fact_inventory WHERE unique_lvl NOT LIKE '%无归属%'")
        df_inv = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        # 按照日期筛选数据范围
        df_inv['acct_period'] = pd.to_datetime(df_inv['acct_period'])
        df_inv = df_inv[df_inv['acct_period'].isin(date_range)]
        
        print(f"加载存货数据完成，共 {len(df_inv)} 条记录")
        cur.close()
        conn.close()
        return df_inv
    except Exception as e:
        print(f"加载存货数据时发生错误: {str(e)}")
        raise


@task(name="process_inventory", log_prints=True)
def process_inventory_task(
    df_inv: pd.DataFrame,
    df_bus_line: pd.DataFrame,
    df_org: pd.DataFrame
) -> pd.DataFrame:
    """
    处理存货数据并应用业务线比例
    
    Args:
        df_inv: 存货数据
        df_bus_line: 业务线比例数据
        df_org: 组织架构数据
    
    Returns:
        处理后的存货数据
    """
    try:
        df_inv_bus = df_bus_line[df_bus_line['class'] == '存货']
        df_inv_bus_list = df_inv_bus['source_no'].tolist()
        
        # 手工分拆的部分
        df_inv_bus_hand = df_inv[df_inv['source_no'].isin(df_inv_bus_list)].merge(
            df_inv_bus[['source_no', 'bus_line', 'unique_lvl', 'category', 'rate']],
            on=['source_no'],
            how='left'
        )
        
        # 需要应用比例的数值列
        inv_num_columns = [
            "qty_inv", "ref_amt", "qty_6m_less", "amt_6m_less",
            "qty_6_9m", "amt_6_9m", "qty_9m_1y", "amt_9m_1y", "qty_1_2y", "amt_1_2y",
            "qty_2_3y", "amt_2_3y", "qty_3y_plus", "amt_3y_plus"
        ]
        
        for col in inv_num_columns:
            if col in df_inv_bus_hand.columns:
                df_inv_bus_hand[col] = df_inv_bus_hand[col].astype(float)
                df_inv_bus_hand[col] = df_inv_bus_hand[col] * df_inv_bus_hand['rate']
        
        df_inv_bus_hand = df_inv_bus_hand.drop(['id'], axis=1, errors='ignore')
        df_inv_bus_hand = df_inv_bus_hand.rename(
            columns={'unique_lvl_x': 'source_lvl', 'unique_lvl_y': 'unique_lvl'}
        )
        # 删除空列或全 NA 列，避免拼接警告
        df_inv_bus_hand = df_inv_bus_hand.dropna(axis=1, how='all')
        
        # 自动归属的部分
        df_inv_bus_auto = df_inv[~df_inv['source_no'].isin(df_inv_bus_list)].merge(
            df_org[['unique_lvl', 'bus_line']],
            on=['unique_lvl'],
            how='left'
        )
        df_inv_bus_auto['source_lvl'] = df_inv_bus_auto['unique_lvl']
        df_inv_bus_auto['rate'] = 1
        df_inv_bus_auto['category'] = df_inv_bus_auto['bus_line'].apply(
            lambda x: '无' if x == '无' else '直接归属'
        )
        # 删除空列或全 NA 列，避免拼接警告
        df_inv_bus_auto = df_inv_bus_auto.dropna(axis=1, how='all')
        
        # 确保两个 DataFrame 有相同的列，缺失的列用 NaN 填充
        all_columns = list(set(df_inv_bus_hand.columns) | set(df_inv_bus_auto.columns))
        df_inv_bus_hand = df_inv_bus_hand.reindex(columns=all_columns)
        df_inv_bus_auto = df_inv_bus_auto.reindex(columns=all_columns)
        
        df_inv_bus_all = pd.concat([df_inv_bus_hand, df_inv_bus_auto], ignore_index=True)
        df_inv_bus_all = df_inv_bus_all.drop(columns=['id'], axis=1, errors='ignore')
        
        print(f"处理存货数据完成，共 {len(df_inv_bus_all)} 条记录")
        return df_inv_bus_all
    except Exception as e:
        print(f"处理存货数据时发生错误: {str(e)}")
        raise


@task(name="validate_inventory_rate", log_prints=True)
def validate_inventory_rate_task(df: pd.DataFrame) -> None:
    """
    验证存货数据比率
    
    Args:
        df: 存货数据
    """
    try:
        col_agg_name = ['source_no']
        col_sum_name = 'rate'
        val_dist(df, col_agg_name, col_sum_name)
        print("存货数据比率验证完成")
    except Exception as e:
        print(f"验证存货数据比率时发生错误: {str(e)}")
        raise


@task(name="save_inventory_detail", log_prints=True)
def save_inventory_detail_task(
    df: pd.DataFrame,
    date_range: pd.DatetimeIndex
) -> None:
    """
    保存存货明细到数据库
    
    Args:
        df: 存货数据
        date_range: 日期范围
    """
    try:
        table_name = 'fact_bus_inventory'
        date_column = 'acct_period'
        df = df.rename(columns={'unique_lvl': 'sec_dist_lvl', 'source_lvl': 'unique_lvl'})
        df_date_column = 'acct_period'
        
        delete_data_add_data_by_DateRange(
            table_name, date_column, df, df_date_column, date_range
        )
        print(f"保存存货明细到数据库完成，共 {len(df)} 条记录")
    except Exception as e:
        print(f"保存存货明细到数据库时发生错误: {str(e)}")
        raise


@task(name="load_inventory_on_way_data", log_prints=True)
def load_inventory_on_way_data_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从数据库加载在途存货数据
    
    Args:
        date_range: 日期范围
    
    Returns:
        在途存货数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute("SELECT * FROM fact_inventory_on_way WHERE unique_lvl NOT LIKE '%无归属%'")
        df_inv_on = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        # 按照日期筛选数据范围
        df_inv_on['acct_period'] = pd.to_datetime(df_inv_on['acct_period'])
        df_inv_on = df_inv_on[df_inv_on['acct_period'].isin(date_range)]
        
        print(f"加载在途存货数据完成，共 {len(df_inv_on)} 条记录")
        cur.close()
        conn.close()
        return df_inv_on
    except Exception as e:
        print(f"加载在途存货数据时发生错误: {str(e)}")
        raise


@task(name="process_inventory_on_way", log_prints=True)
def process_inventory_on_way_task(
    df_inv_on: pd.DataFrame,
    df_bus_line: pd.DataFrame,
    df_org: pd.DataFrame
) -> pd.DataFrame:
    """
    处理在途存货数据并应用业务线比例
    
    Args:
        df_inv_on: 在途存货数据
        df_bus_line: 业务线比例数据
        df_org: 组织架构数据
    
    Returns:
        处理后的在途存货数据
    """
    try:
        df_inv_on_bus = df_bus_line[df_bus_line['class'] == '在途存货']
        df_inv_on_bus_list = df_inv_on_bus['source_no'].tolist()
        
        # 手工分拆的部分
        df_inv_on_bus_hand = df_inv_on[df_inv_on['source_no'].isin(df_inv_on_bus_list)].merge(
            df_inv_on_bus[['source_no', 'bus_line', 'unique_lvl', 'category', 'rate']],
            on=['source_no'],
            how='left'
        )
        
        # 需要应用比例的数值列
        inv_num_columns = [
            "order_amount", "total_payment_amount", "order_count",
            "total_inventory_received", "unreceived_inventory"
        ]
        
        for col in inv_num_columns:
            if col in df_inv_on_bus_hand.columns:
                df_inv_on_bus_hand[col] = df_inv_on_bus_hand[col].astype(float)
                df_inv_on_bus_hand[col] = df_inv_on_bus_hand[col] * df_inv_on_bus_hand['rate']
        
        df_inv_on_bus_hand = df_inv_on_bus_hand.drop(['id'], axis=1, errors='ignore')
        df_inv_on_bus_hand = df_inv_on_bus_hand.rename(
            columns={'unique_lvl_x': 'source_lvl', 'unique_lvl_y': 'unique_lvl'}
        )
        # 删除空列或全 NA 列，避免拼接警告
        df_inv_on_bus_hand = df_inv_on_bus_hand.dropna(axis=1, how='all')
        
        # 自动归属的部分
        df_inv_on_bus_auto = df_inv_on[~df_inv_on['source_no'].isin(df_inv_on_bus_list)].merge(
            df_org[['unique_lvl', 'bus_line']],
            on=['unique_lvl'],
            how='left'
        )
        df_inv_on_bus_auto['source_lvl'] = df_inv_on_bus_auto['unique_lvl']
        df_inv_on_bus_auto['rate'] = 1
        df_inv_on_bus_auto['category'] = df_inv_on_bus_auto['bus_line'].apply(
            lambda x: '无' if x == '无' else '直接归属'
        )
        # 删除空列或全 NA 列，避免拼接警告
        df_inv_on_bus_auto = df_inv_on_bus_auto.dropna(axis=1, how='all')
        
        # 确保两个 DataFrame 有相同的列，缺失的列用 NaN 填充
        all_columns = list(set(df_inv_on_bus_hand.columns) | set(df_inv_on_bus_auto.columns))
        df_inv_on_bus_hand = df_inv_on_bus_hand.reindex(columns=all_columns)
        df_inv_on_bus_auto = df_inv_on_bus_auto.reindex(columns=all_columns)
        
        df_inv_on_bus_all = pd.concat([df_inv_on_bus_hand, df_inv_on_bus_auto], ignore_index=True)
        df_inv_on_bus_all = df_inv_on_bus_all.drop(columns=['id'], axis=1, errors='ignore')
        
        print(f"处理在途存货数据完成，共 {len(df_inv_on_bus_all)} 条记录")
        return df_inv_on_bus_all
    except Exception as e:
        print(f"处理在途存货数据时发生错误: {str(e)}")
        raise


@task(name="validate_inventory_on_way_rate", log_prints=True)
def validate_inventory_on_way_rate_task(df: pd.DataFrame) -> None:
    """
    验证在途存货数据比率
    
    Args:
        df: 在途存货数据
    """
    try:
        col_agg_name = ['source_no']
        col_sum_name = 'rate'
        val_dist(df, col_agg_name, col_sum_name)
        print("在途存货数据比率验证完成")
    except Exception as e:
        print(f"验证在途存货数据比率时发生错误: {str(e)}")
        raise


@task(name="save_inventory_on_way_detail", log_prints=True)
def save_inventory_on_way_detail_task(
    df: pd.DataFrame,
    date_range: pd.DatetimeIndex
) -> None:
    """
    保存在途存货明细到数据库
    
    Args:
        df: 在途存货数据
        date_range: 日期范围
    """
    try:
        table_name = 'fact_bus_inventory_on_way'
        date_column = 'acct_period'
        df = df.rename(columns={'unique_lvl': 'sec_dist_lvl', 'source_lvl': 'unique_lvl'})
        df_date_column = 'acct_period'
        
        delete_data_add_data_by_DateRange(
            table_name, date_column, df, df_date_column, date_range
        )
        print(f"保存在途存货明细到数据库完成，共 {len(df)} 条记录")
    except Exception as e:
        print(f"保存在途存货明细到数据库时发生错误: {str(e)}")
        raise
