"""费用明细生成相关 Tasks"""
from mypackage.utilities import connect_to_db, val_dist, delete_data_add_data_by_DateRange
from prefect import task
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@task(name="load_expense_data", log_prints=True)
def load_expense_data_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从数据库加载费用数据

    Args:
        date_range: 日期范围

    Returns:
        费用数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute(
            "SELECT * FROM fact_expense WHERE unique_lvl NOT LIKE '%无归属%'")
        df_expense = pd.DataFrame(cur.fetchall(), columns=[
                                  desc[0] for desc in cur.description])

        # 删除最后更新时间
        if 'last_modified' in df_expense.columns:
            df_expense = df_expense.drop(['last_modified'], axis=1)

        # 按照日期筛选数据范围
        df_expense['acct_period'] = pd.to_datetime(df_expense['acct_period'])
        df_expense = df_expense[df_expense['acct_period'].isin(date_range)]

        print(f"加载费用数据完成，共 {len(df_expense)} 条记录")
        cur.close()
        conn.close()
        return df_expense
    except Exception as e:
        print(f"加载费用数据时发生错误: {str(e)}")
        raise


@task(name="process_gap_expense", log_prints=True)
def process_gap_expense_task(
    df_expense: pd.DataFrame,
    df_upload_merge_all: pd.DataFrame
) -> pd.DataFrame:
    """
    处理比率不完整的费用（公摊费用）

    Args:
        df_expense: 费用数据
        df_upload_merge_all: 业务线比例数据

    Returns:
        处理后的公摊费用数据
    """
    try:
        df_expense_bus = df_upload_merge_all[df_upload_merge_all['class'] == '费用']

        # 找出比率不完整的费用
        df_expense_bus_gap_rate = df_expense_bus.groupby(
            ['source_no']).agg({'rate': 'sum'}).reset_index()
        df_expense_bus_gap_rate = df_expense_bus_gap_rate[df_expense_bus_gap_rate['rate'] < 1]
        df_expense_bus_gap_rate['rate'] = 1 - \
            df_expense_bus_gap_rate['rate'].astype('float')
        df_expense_bus_gap_list = df_expense_bus_gap_rate['source_no'].tolist()

        # 处理公摊费用
        df_expense_bus_gap = df_expense[df_expense['source_no'].isin(df_expense_bus_gap_list)].merge(
            df_expense_bus_gap_rate[['source_no', 'rate']],
            on=['source_no'],
            how='left'
        )
        df_expense_bus_gap = df_expense_bus_gap.astype(
            {'rate': 'float', 'exp_amt': 'float'})
        df_expense_bus_gap['exp_amt'] = df_expense_bus_gap['exp_amt'] * \
            df_expense_bus_gap['rate']
        df_expense_bus_gap['bus_line'] = '无'
        df_expense_bus_gap['source_lvl'] = df_expense_bus_gap['unique_lvl']
        df_expense_bus_gap['category'] = '公摊费用'

        print(f"处理公摊费用完成，共 {len(df_expense_bus_gap)} 条记录")
        return df_expense_bus_gap
    except Exception as e:
        print(f"处理公摊费用时发生错误: {str(e)}")
        raise


@task(name="process_manual_expense", log_prints=True)
def process_manual_expense_task(
    df_expense: pd.DataFrame,
    df_upload_merge_all: pd.DataFrame
) -> pd.DataFrame:
    """
    处理手工分拆的费用数据

    Args:
        df_expense: 费用数据
        df_upload_merge_all: 业务线比例数据

    Returns:
        处理后的手工分拆费用数据
    """
    try:
        df_expense_bus = df_upload_merge_all[df_upload_merge_all['class'] == '费用']
        df_expense_bus_list = df_expense_bus['source_no'].tolist()

        df_expense_bus_hand = df_expense[df_expense['source_no'].isin(df_expense_bus_list)].merge(
            df_expense_bus[['source_no', 'bus_line',
                            'unique_lvl', 'category', 'rate']],
            on=['source_no'],
            how='left'
        )
        df_expense_bus_hand = df_expense_bus_hand.astype(
            {'rate': 'float', 'exp_amt': 'float'})
        df_expense_bus_hand = df_expense_bus_hand.rename(
            columns={'unique_lvl_x': 'source_lvl',
                     'unique_lvl_y': 'unique_lvl'}
        )
        df_expense_bus_hand['exp_amt'] = df_expense_bus_hand['exp_amt'] * \
            df_expense_bus_hand['rate']

        print(f"处理手工分拆费用数据完成，共 {len(df_expense_bus_hand)} 条记录")
        return df_expense_bus_hand
    except Exception as e:
        print(f"处理手工分拆费用数据时发生错误: {str(e)}")
        raise


@task(name="process_auto_expense", log_prints=True)
def process_auto_expense_task(
    df_expense: pd.DataFrame,
    df_upload_merge_all: pd.DataFrame,
    df_org: pd.DataFrame
) -> pd.DataFrame:
    """
    处理自动归属的费用数据

    Args:
        df_expense: 费用数据
        df_upload_merge_all: 业务线比例数据
        df_org: 组织架构数据

    Returns:
        处理后的自动归属费用数据
    """
    try:
        df_expense_bus = df_upload_merge_all[df_upload_merge_all['class'] == '费用']
        df_expense_bus_list = df_expense_bus['source_no'].tolist()

        # 不位于分拆表中的数据
        df_expense_bus_auto = df_expense[~df_expense['source_no'].isin(df_expense_bus_list)].merge(
            df_org[['unique_lvl', 'bus_line']],
            on=['unique_lvl'],
            how='left'
        )
        df_expense_bus_auto['source_lvl'] = df_expense_bus_auto['unique_lvl']
        df_expense_bus_auto['rate'] = 1
        df_expense_bus_auto['category'] = df_expense_bus_auto['bus_line'].apply(
            lambda x: '公摊费用' if x == '无' else '直接归属'
        )

        print(f"处理自动归属费用数据完成，共 {len(df_expense_bus_auto)} 条记录")
        return df_expense_bus_auto
    except Exception as e:
        print(f"处理自动归属费用数据时发生错误: {str(e)}")
        raise


@task(name="merge_expense_data", log_prints=True)
def merge_expense_data_task(
    df_expense_bus_hand: pd.DataFrame,
    df_expense_bus_auto: pd.DataFrame,
    df_expense_bus_gap: pd.DataFrame
) -> pd.DataFrame:
    """
    合并费用数据

    Args:
        df_expense_bus_hand: 手工分拆费用数据
        df_expense_bus_auto: 自动归属费用数据
        df_expense_bus_gap: 公摊费用数据

    Returns:
        合并后的费用数据
    """
    try:
        df_expense_bus_all = pd.concat(
            [df_expense_bus_hand, df_expense_bus_auto, df_expense_bus_gap],
            ignore_index=True
        )
        df_expense_bus_all = df_expense_bus_all.drop(
            ['id'], axis=1, errors='ignore')

        print(f"合并费用数据完成，共 {len(df_expense_bus_all)} 条记录")
        return df_expense_bus_all
    except Exception as e:
        print(f"合并费用数据时发生错误: {str(e)}")
        raise


@task(name="update_energy_hardware_expense", log_prints=True)
def update_energy_hardware_expense_task(df: pd.DataFrame) -> pd.DataFrame:
    """
    更新能源硬件为能源运营（2025-08后）

    Args:
        df: 费用数据

    Returns:
        更新后的数据
    """
    try:
        # 确保日期列为 datetime 格式
        df['acct_period'] = pd.to_datetime(df['acct_period'])

        # 构建掩码：2025-08-01 之后且业务线为'能源硬件'
        mask = (df['acct_period'] >= pd.to_datetime(
            '2025-08-01')) & (df['bus_line'] == '能源硬件')

        rows_to_change = mask.sum()
        if rows_to_change > 0:
            print(f"更新能源硬件为能源运营，共 {rows_to_change} 条记录")
            df.loc[mask, 'bus_line'] = '能源运营'

        return df
    except Exception as e:
        print(f"更新能源硬件时发生错误: {str(e)}")
        raise


@task(name="validate_expense_rate", log_prints=True)
def validate_expense_rate_task(df: pd.DataFrame) -> None:
    """
    验证费用数据比率

    Args:
        df: 费用数据
    """
    try:
        col_agg_name = ['source_no', 'unique_lvl']
        col_sum_name = 'rate'
        val_dist(df, col_agg_name, col_sum_name)
        print("费用数据比率验证完成")
    except Exception as e:
        print(f"验证费用数据比率时发生错误: {str(e)}")
        raise


@task(name="apply_shared_rate_to_expense", log_prints=True)
def apply_shared_rate_to_expense_task(
    df_expense_bus_all: pd.DataFrame,
    date_range: pd.DatetimeIndex
) -> pd.DataFrame:
    """
    将业务线为"无"的数据按照公摊比例进行还原

    Args:
        df_expense_bus_all: 费用数据
        date_range: 日期范围

    Returns:
        应用公摊比例后的费用数据
    """
    try:
        conn, cur = connect_to_db()
        cur.execute("SELECT * FROM fact_bus_shared_rate")
        df_shared_rate = pd.DataFrame(cur.fetchall(), columns=[
                                      desc[0] for desc in cur.description])
        df_shared_rate = df_shared_rate.drop(
            ['id'], axis=1).rename(columns={'date': 'acct_period'})
        df_shared_rate['acct_period'] = pd.to_datetime(
            df_shared_rate['acct_period'])
        cur.close()
        conn.close()

        # 分离业务线为"无"的数据和其他数据
        df_wu = df_expense_bus_all[df_expense_bus_all['bus_line'] == '无'].drop(
            ['bus_line', 'rate'], axis=1
        )
        df_you = df_expense_bus_all[df_expense_bus_all['bus_line'] != '无']

        df_wu['acct_period'] = pd.to_datetime(df_wu['acct_period'])
        df_wu = pd.merge(
            df_wu,
            df_shared_rate[['acct_period', 'rate', 'bus_line']],
            on=['acct_period'],
            how='left'
        )

        # 应用比例
        df_wu['exp_amt'] = df_wu['exp_amt'].astype('float')
        df_wu['exp_amt'] = df_wu['exp_amt'] * df_wu['rate']

        df = pd.concat([df_wu, df_you], ignore_index=True)
        df['acct_period'] = pd.to_datetime(df['acct_period'])

        print(f"应用公摊比例到费用数据完成，共 {len(df)} 条记录")
        return df
    except Exception as e:
        print(f"应用公摊比例到费用数据时发生错误: {str(e)}")
        raise


@task(name="save_expense_detail", log_prints=True)
def save_expense_detail_task(
    df: pd.DataFrame,
    date_range: pd.DatetimeIndex
) -> None:
    """
    保存费用明细到数据库

    Args:
        df: 费用数据
        date_range: 日期范围
    """
    try:
        table_name = 'fact_bus_expense'
        date_column = 'acct_period'
        df = df.rename(
            columns={'unique_lvl': 'sec_dist_lvl', 'source_lvl': 'unique_lvl'})
        # 删除 id 列（如果存在），让数据库自动生成，避免主键冲突
        if 'id' in df.columns:
            df = df.drop(['id'], axis=1)
        df_date_column = 'acct_period'

        delete_data_add_data_by_DateRange(
            table_name, date_column, df, df_date_column, date_range
        )
        print(f"保存费用明细到数据库完成，共 {len(df)} 条记录")
    except Exception as e:
        print(f"保存费用明细到数据库时发生错误: {str(e)}")
        raise
