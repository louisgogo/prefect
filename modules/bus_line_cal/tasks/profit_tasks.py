"""利润明细生成相关 Tasks"""
from mypackage.utilities import connect_to_db, val_dist, delete_data_add_data_by_DateRange, add_data
from prefect import task
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@task(name="load_profit_data", log_prints=True)
def load_profit_data_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从数据库加载利润数据

    Args:
        date_range: 日期范围

    Returns:
        利润数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute(
            "SELECT * FROM fact_profit_bd WHERE prim_subj NOT IN "
            "('营业收入','营业成本','管理费用','销售费用','财务费用','研发费用','营业利润','净利润','利润总额')"
        )
        df_profit = pd.DataFrame(cur.fetchall(), columns=[
                                 desc[0] for desc in cur.description])

        # 删除最后更新时间
        if 'last_modified' in df_profit.columns:
            df_profit = df_profit.drop(['last_modified'], axis=1)

        # 按照日期筛选数据范围
        df_profit['date'] = pd.to_datetime(df_profit['date'])
        df_profit = df_profit[df_profit['date'].isin(date_range)]

        print(f"加载利润数据完成，共 {len(df_profit)} 条记录")
        cur.close()
        conn.close()
        return df_profit
    except Exception as e:
        print(f"加载利润数据时发生错误: {str(e)}")
        raise


@task(name="process_manual_profit", log_prints=True)
def process_manual_profit_task(
    df_profit: pd.DataFrame,
    df_upload_merge_all: pd.DataFrame
) -> pd.DataFrame:
    """
    处理手工分拆的利润数据

    Args:
        df_profit: 利润数据
        df_upload_merge_all: 业务线比例数据

    Returns:
        处理后的手工分拆利润数据
    """
    try:
        df_profit_bus = df_upload_merge_all[df_upload_merge_all['class'] == '其他']
        df_profit_bus_list = df_profit_bus['source_no'].tolist()
        df_profit_bus.loc[:, 'source_lvl'] = df_profit_bus['unique_lvl']

        df_profit_bus_hand = df_profit[df_profit['source_no'].isin(df_profit_bus_list)].merge(
            df_profit_bus[['source_no', 'bus_line',
                           'source_lvl', 'category', 'rate']],
            on=['source_no'],
            how='left'
        )
        df_profit_bus_hand = df_profit_bus_hand.astype(
            {'rate': 'float', 'mo_amt': 'float'})
        df_profit_bus_hand['mo_amt'] = df_profit_bus_hand['mo_amt'] * \
            df_profit_bus_hand['rate']
        df_profit_bus_hand = df_profit_bus_hand.drop(
            ['id'], axis=1, errors='ignore')

        print(f"处理手工分拆利润数据完成，共 {len(df_profit_bus_hand)} 条记录")
        return df_profit_bus_hand
    except Exception as e:
        print(f"处理手工分拆利润数据时发生错误: {str(e)}")
        raise


@task(name="process_auto_profit", log_prints=True)
def process_auto_profit_task(
    df_profit: pd.DataFrame,
    df_upload_merge_all: pd.DataFrame,
    df_org: pd.DataFrame
) -> pd.DataFrame:
    """
    处理自动归属的利润数据

    Args:
        df_profit: 利润数据
        df_upload_merge_all: 业务线比例数据
        df_org: 组织架构数据

    Returns:
        处理后的自动归属利润数据
    """
    try:
        df_profit_bus = df_upload_merge_all[df_upload_merge_all['class'] == '其他']
        df_profit_bus_list = df_profit_bus['source_no'].tolist()

        # 提取业务线比例表格中，类型为预提，唯一层级包含无归属的id号码
        df_profit_bus_nan_list = df_upload_merge_all[
            df_upload_merge_all['class'].str.contains('预提', na=False)
        ]['source_no'].tolist()

        # 不位于分拆表中的数据
        df_profit_bus_auto = df_profit[
            ((~df_profit['source_no'].isin(df_profit_bus_list)) &
             (~df_profit['unique_lvl'].str.contains('无归属', na=False))) |
            (df_profit['source_no'].isin(df_profit_bus_nan_list))
        ].merge(df_org[['unique_lvl', 'bus_line']], on=['unique_lvl'], how='left')

        df_profit_bus_auto['source_lvl'] = df_profit_bus_auto['unique_lvl']
        df_profit_bus_auto['rate'] = 1
        df_profit_bus_auto['category'] = df_profit_bus_auto['bus_line'].apply(
            lambda x: '无' if x == '无' else '直接归属'
        )

        print(f"处理自动归属利润数据完成，共 {len(df_profit_bus_auto)} 条记录")
        return df_profit_bus_auto
    except Exception as e:
        print(f"处理自动归属利润数据时发生错误: {str(e)}")
        raise


@task(name="convert_expense_to_profit", log_prints=True)
def convert_expense_to_profit_task(df_expense_bus_all: pd.DataFrame) -> pd.DataFrame:
    """
    转换费用表为利润表格式

    Args:
        df_expense_bus_all: 费用数据

    Returns:
        转换后的利润表格式数据
    """
    try:
        df_expense_bus_all_to_profit = df_expense_bus_all.drop(['date'], axis=1, errors='ignore').rename(
            columns={'exp_amt': 'mo_amt', 'acct_period': 'date'}
        )
        print(f"转换费用表为利润表格式完成，共 {len(df_expense_bus_all_to_profit)} 条记录")
        return df_expense_bus_all_to_profit
    except Exception as e:
        print(f"转换费用表为利润表格式时发生错误: {str(e)}")
        raise


@task(name="convert_revenue_to_profit", log_prints=True)
def convert_revenue_to_profit_task(df_revenue_bus_all_to_profit: pd.DataFrame) -> pd.DataFrame:
    """
    转换收入表为利润表格式

    Args:
        df_revenue_bus_all_to_profit: 收入数据

    Returns:
        转换后的利润表格式数据
    """
    try:
        df_revenue_bus_all_to_profit = df_revenue_bus_all_to_profit.rename(
            columns={'acct_period': 'date'}
        )
        print(f"转换收入表为利润表格式完成，共 {len(df_revenue_bus_all_to_profit)} 条记录")
        return df_revenue_bus_all_to_profit
    except Exception as e:
        print(f"转换收入表为利润表格式时发生错误: {str(e)}")
        raise


@task(name="load_offset_data", log_prints=True)
def load_offset_data_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    加载抵销数数据

    Args:
        date_range: 日期范围（用于最终筛选，但计算时需要从年初开始）

    Returns:
        抵销数数据 DataFrame
    """
    try:
        conn, cur = connect_to_db()
        cur.execute(
            "SELECT * FROM fact_offset WHERE subj_name NOT IN ('营业利润','净利润','利润总额')"
        )
        df_offset = pd.DataFrame(cur.fetchall(), columns=[
                                 desc[0] for desc in cur.description])
        df_offset['date'] = pd.to_datetime(df_offset['date'])

        # 关键修改：获取从年初到当前月份的所有数据，用于计算 diff()
        # 获取当前处理的年份和月份
        year = date_range[0].year
        max_date = date_range.max()

        # 筛选从年初到当前月份的所有数据（用于计算）
        df_offset_calc = df_offset[
            (df_offset['date'].dt.year == year) &
            (df_offset['date'] <= max_date)
        ].copy()

        # 按科目、日期、唯一层级分组汇总
        df_offset_calc = df_offset_calc.groupby(['subj_name', 'date', 'unique_lvl']).agg(
            {'offset_num': 'sum'}
        ).reset_index()

        # 按年份分组并应用 diff()
        df_offset_calc = df_offset_calc.sort_values(
            ['unique_lvl', 'subj_name', 'date'])
        df_offset_calc['mo_amt'] = df_offset_calc.groupby(
            ['unique_lvl', 'subj_name'])['offset_num'].diff()

        # 处理每年的第一个月
        df_offset_calc.loc[df_offset_calc['date'].dt.month ==
                           1, 'mo_amt'] = df_offset_calc['offset_num']

        # 现在才筛选到目标日期范围（最终结果）
        df_offset = df_offset_calc[df_offset_calc['date'].isin(
            date_range)].copy()

        # 设置其他字段
        df_offset[['fin_con', 'fin_ind']] = '抵销数'
        df_offset[['bus_line', 'category']] = '抵销数'
        df_offset['source_no'] = '抵销数'
        df_offset['rate'] = 1
        df_offset.loc[:, 'year'] = df_offset['date'].dt.year
        df_offset.loc[:, 'source_lvl'] = df_offset['unique_lvl']
        df_offset = df_offset.rename(columns={'subj_name': 'prim_subj'})
        df_offset = df_offset.drop(['offset_num'], axis=1)
        df_offset.loc[:, 'date'] = df_offset['date'].dt.date

        # 过滤零值和空值
        df_offset = df_offset[(df_offset['mo_amt'] != 0)
                              & (df_offset['mo_amt'].notna())]

        # 保存到 fact_offset_by_month 表（用于利润表刷新）
        # 准备保存格式：需要匹配 notebook 的格式
        df_offset_to_save = df_offset.copy()
        df_offset_to_save = df_offset_to_save.rename(
            columns={'date': 'acct_period', 'mo_amt': 'amt'})
        df_offset_to_save['acct_period'] = pd.to_datetime(
            df_offset_to_save['acct_period'])
        df_offset_to_save['class'] = '抵销'
        # source_no 格式：'O' + index.astype(str)
        df_offset_to_save = df_offset_to_save.reset_index(drop=True)
        df_offset_to_save['source_no'] = 'O' + \
            df_offset_to_save.index.astype(str)
        # 只保留需要的列
        df_offset_to_save = df_offset_to_save[[
            'source_no', 'unique_lvl', 'acct_period', 'prim_subj', 'amt', 'class', 'fin_con', 'fin_ind']]

        # 使用 delete_data_add_data_by_DateRange 保存，先删除当前月份的数据，再插入新数据
        from mypackage.utilities import delete_data_add_data_by_DateRange
        delete_data_add_data_by_DateRange(
            table_name='fact_offset_by_month',
            date_column='acct_period',
            df=df_offset_to_save,
            df_date_column='acct_period',
            date_range=date_range
        )
        print(
            f"抵销数数据已保存到 fact_offset_by_month 表，共 {len(df_offset_to_save)} 条记录")

        print(
            f"加载抵销数数据完成，共 {len(df_offset)} 条记录（计算时使用了从 {year}年1月到{max_date.strftime('%Y-%m')}的数据）")
        cur.close()
        conn.close()
        return df_offset
    except Exception as e:
        print(f"加载抵销数数据时发生错误: {str(e)}")
        raise


@task(name="merge_profit_data", log_prints=True)
def merge_profit_data_task(
    df_revenue_bus_all_to_profit: pd.DataFrame,
    df_expense_bus_all_to_profit: pd.DataFrame,
    df_profit_bus_all: pd.DataFrame,
    df_offset: pd.DataFrame
) -> pd.DataFrame:
    """
    合并收入表、费用表和利润表

    Args:
        df_revenue_bus_all_to_profit: 收入数据（利润表格式）
        df_expense_bus_all_to_profit: 费用数据（利润表格式）
        df_profit_bus_all: 利润数据
        df_offset: 抵销数数据

    Returns:
        合并后的利润数据
    """
    try:
        # 获取利润表的列
        profit_columns = df_profit_bus_all.columns.tolist()

        # 合并所有数据
        df_profit_bus_all_to_profit = pd.concat(
            [
                df_revenue_bus_all_to_profit,
                df_expense_bus_all_to_profit,
                df_profit_bus_all,
                df_offset
            ],
            ignore_index=True
        )

        # 只保留利润表的列
        df_profit_bus_all_to_profit = df_profit_bus_all_to_profit[profit_columns]

        # 去除空值
        df_profit_bus_all_to_profit = df_profit_bus_all_to_profit[
            df_profit_bus_all_to_profit['mo_amt'].notna()
        ]

        # 无归属-其他调整-待摊事项统一调整为新国都本部-公共部门-公共部门
        df_profit_bus_all_to_profit['unique_lvl'] = df_profit_bus_all_to_profit['unique_lvl'].replace(
            '无归属-其他调整-待摊事项',
            '新国都本部-公共部门-公共部门'
        )

        print(f"合并利润数据完成，共 {len(df_profit_bus_all_to_profit)} 条记录")
        return df_profit_bus_all_to_profit
    except Exception as e:
        print(f"合并利润数据时发生错误: {str(e)}")
        raise


@task(name="validate_profit_rate", log_prints=True)
def validate_profit_rate_task(df: pd.DataFrame) -> None:
    """
    验证利润数据比率

    Args:
        df: 利润数据
    """
    try:
        col_agg_name = ['source_no', 'unique_lvl', 'prim_subj']
        col_sum_name = 'rate'
        val_dist(df, col_agg_name, col_sum_name)
        print("利润数据比率验证完成")
    except Exception as e:
        print(f"验证利润数据比率时发生错误: {str(e)}")
        raise


@task(name="save_profit_detail", log_prints=True)
def save_profit_detail_task(
    df: pd.DataFrame,
    date_range: pd.DatetimeIndex
) -> None:
    """
    保存利润明细到数据库

    Args:
        df: 利润数据
        date_range: 日期范围
    """
    try:
        table_name = 'fact_bus_profit_bd'
        date_column = 'date'
        df = df.rename(
            columns={'unique_lvl': 'sec_dist_lvl', 'source_lvl': 'unique_lvl'})
        # 删除 id 列（如果存在），让数据库自动生成，避免主键冲突和 NULL 值问题
        if 'id' in df.columns:
            df = df.drop(['id'], axis=1)
        df_date_column = 'date'

        delete_data_add_data_by_DateRange(
            table_name, date_column, df, df_date_column, date_range
        )
        print(f"保存利润明细到数据库完成，共 {len(df)} 条记录")
    except Exception as e:
        print(f"保存利润明细到数据库时发生错误: {str(e)}")
        raise


@task(name="process_shared_profit", log_prints=True)
def process_shared_profit_task(
    date_range: pd.DatetimeIndex
) -> None:
    """
    处理业务线为"无"的利润数据分摊

    Args:
        date_range: 日期范围
    """
    try:
        # 在 task 内部获取业务线比例数据（避免 DataFrame 序列化问题）
        # 直接调用数据准备 tasks，避免导入 flow
        from .data_preparation_tasks import calculate_person_weight_task
        df_upload_merge_all, _ = calculate_person_weight_task()

        conn, cur = connect_to_db()

        # 获取预提类型为综合比例的其他
        df_profit_bus_pro_list_z = df_upload_merge_all[
            df_upload_merge_all['class'] == '预提-综合比例'
        ]
        df_profit_bus_pro_list_z = df_profit_bus_pro_list_z['source_no'].tolist(
        )

        # 获取预提类型为特殊比例的其他
        df_profit_bus_pro_list_t = df_upload_merge_all[
            df_upload_merge_all['class'] == '预提-特殊比例'
        ]
        df_profit_bus_pro_list_t = df_profit_bus_pro_list_t['source_no'].tolist(
        )

        # 获取公摊比例
        print("正在获取公摊比例数据...")
        cur.execute("SELECT * FROM fact_bus_shared_rate")
        df_shared_rate = pd.DataFrame(cur.fetchall(), columns=[
                                      desc[0] for desc in cur.description])
        df_shared_rate = df_shared_rate.drop(['id'], axis=1)

        # 分摊利润表 - 只查询日期范围内的数据以提高性能
        print(f"正在查询利润表数据（日期范围：{date_range.min()} 到 {date_range.max()}）...")
        min_date = date_range.min()
        max_date = date_range.max()
        cur.execute("""
            SELECT * FROM fact_bus_profit_bd 
            WHERE date >= %s AND date <= %s
        """, (min_date, max_date))
        df_all = pd.DataFrame(cur.fetchall(), columns=[
                              desc[0] for desc in cur.description])
        print(f"查询到 {len(df_all)} 条利润数据")
        df_all = df_all.rename(
            columns={'unique_lvl': 'source_lvl', 'sec_dist_lvl': 'unique_lvl'})

        # 定义逆透视的行标签列
        df_index = ['date', 'year', 'source_no',
                    'unique_lvl', 'source_lvl', 'prim_subj', 'mo_amt']
        df_groupby = df_index[:-1]

        # 计算非预提费用的，无的净利润
        df_shared = df_all[
            (df_all['bus_line'] == '无') &
            (~df_all['source_no'].isin(
                df_profit_bus_pro_list_z + df_profit_bus_pro_list_t))
        ]

        if len(df_shared) > 0:
            df_shared_profit = df_shared[df_index].groupby(
                df_groupby).sum().reset_index()
            df_shared_profit = df_shared_profit.merge(
                df_shared_rate, how='left', on='date')

            df_shared_profit['mo_amt'] = df_shared_profit['mo_amt'].astype(
                float)
            df_shared_profit['mo_amt'] = df_shared_profit['mo_amt'] * \
                df_shared_profit['rate']
            df_shared_profit['fin_con'] = '业报调整'
            df_shared_profit['fin_ind'] = '公摊损益分摊'
            df_shared_profit['category'] = '公摊费用'
            df_shared_profit = df_shared_profit[
                (df_shared_profit['mo_amt'] != 0) & (
                    df_shared_profit['mo_amt'].notnull())
            ]

            df_shared_profit_adj = df_shared_profit.copy()
            df_shared_profit_adj['mo_amt'] = df_shared_profit_adj['mo_amt'] * -1
            df_shared_profit_adj['bus_line'] = '无'
            df_shared_profit_adj['fin_ind'] = '公摊损益冲销'

            df_shared_profit_all = pd.concat(
                [df_shared_profit_adj, df_shared_profit], ignore_index=True)
            df_shared_profit_all = df_shared_profit_all.rename(
                columns={'unique_lvl': 'sec_dist_lvl',
                         'source_lvl': 'unique_lvl'}
            )
            # 删除 id 列（如果存在），让数据库自动生成
            if 'id' in df_shared_profit_all.columns:
                df_shared_profit_all = df_shared_profit_all.drop(
                    ['id'], axis=1)
            add_data('fact_bus_profit_bd', df_shared_profit_all)
            print(f"处理非预提费用公摊利润完成，共 {len(df_shared_profit_all)} 条记录")

        # 计算预提费用的，业务线为无，适用综合比例
        df_shared = df_all[
            (df_all['bus_line'] == '无') &
            (df_all['source_no'].isin(df_profit_bus_pro_list_z))
        ]

        if len(df_shared) > 0:
            df_shared_profit = df_shared[df_index].groupby(
                df_groupby).sum().reset_index()
            df_shared_profit = df_shared_profit.merge(
                df_shared_rate, how='left', on='date')

            df_shared_profit['mo_amt'] = df_shared_profit['mo_amt'].astype(
                float)
            df_shared_profit['mo_amt'] = df_shared_profit['mo_amt'] * \
                df_shared_profit['rate']
            df_shared_profit['fin_con'] = '业报调整'
            df_shared_profit['fin_ind'] = '预提损益分摊'
            df_shared_profit['category'] = '预提费用'
            df_shared_profit = df_shared_profit[
                (df_shared_profit['mo_amt'] != 0) & (
                    df_shared_profit['mo_amt'].notnull())
            ]

            df_shared_profit_adj = df_shared_profit.copy()
            df_shared_profit_adj['mo_amt'] = df_shared_profit_adj['mo_amt'] * -1
            df_shared_profit_adj['bus_line'] = '无'
            df_shared_profit_adj['fin_ind'] = '预提损益冲销'

            df_shared_profit_all = pd.concat(
                [df_shared_profit_adj, df_shared_profit], ignore_index=True)
            df_shared_profit_all = df_shared_profit_all.rename(
                columns={'unique_lvl': 'sec_dist_lvl',
                         'source_lvl': 'unique_lvl'}
            )
            # 删除 id 列（如果存在），让数据库自动生成
            if 'id' in df_shared_profit_all.columns:
                df_shared_profit_all = df_shared_profit_all.drop(
                    ['id'], axis=1)
            add_data('fact_bus_profit_bd', df_shared_profit_all)
            print(f"处理预提费用综合比例公摊利润完成，共 {len(df_shared_profit_all)} 条记录")

        # 计算预提费用的，业务线为无，适用特殊比例
        df_shared_rate_t = df_upload_merge_all[
            df_upload_merge_all['class'] == '预提-特殊比例'
        ][['source_no', 'bus_line', 'rate']]

        df_shared = df_all[
            (df_all['bus_line'] == '无') &
            (df_all['source_no'].isin(df_profit_bus_pro_list_t))
        ]

        if len(df_shared) > 0:
            df_shared_profit = df_shared[df_index].groupby(
                df_groupby).sum().reset_index()
            df_shared_profit = df_shared_profit.merge(
                df_shared_rate_t, how='left', on='source_no')

            df_shared_profit['mo_amt'] = df_shared_profit['mo_amt'].astype(
                float)
            df_shared_profit['mo_amt'] = df_shared_profit['mo_amt'] * \
                df_shared_profit['rate']
            df_shared_profit['fin_con'] = '业报调整'
            df_shared_profit['fin_ind'] = '预提损益分摊'
            df_shared_profit['source_lvl'] = df_shared_profit.apply(
                lambda x: '新国都本部-公共部门-公共部门' if x['source_lvl'] == '无归属-其他调整-待摊事项' else x['source_lvl'],
                axis=1
            )
            df_shared_profit['category'] = '预提费用'
            df_shared_profit = df_shared_profit[
                (df_shared_profit['mo_amt'] != 0) & (
                    df_shared_profit['mo_amt'].notnull())
            ]

            df_shared_profit_adj = df_shared_profit.copy()
            df_shared_profit_adj['mo_amt'] = df_shared_profit_adj['mo_amt'] * -1
            df_shared_profit_adj['bus_line'] = '无'
            df_shared_profit_adj['fin_ind'] = '预提损益冲销'

            df_shared_profit_all = pd.concat(
                [df_shared_profit_adj, df_shared_profit], ignore_index=True)
            df_shared_profit_all = df_shared_profit_all.rename(
                columns={'unique_lvl': 'sec_dist_lvl',
                         'source_lvl': 'unique_lvl'}
            )
            # 删除 id 列（如果存在），让数据库自动生成
            if 'id' in df_shared_profit_all.columns:
                df_shared_profit_all = df_shared_profit_all.drop(
                    ['id'], axis=1)
            add_data('fact_bus_profit_bd', df_shared_profit_all)
            print(f"处理预提费用特殊比例公摊利润完成，共 {len(df_shared_profit_all)} 条记录")

        cur.close()
        conn.close()
        print("处理公摊利润完成")
    except Exception as e:
        print(f"处理公摊利润时发生错误: {str(e)}")
        raise
