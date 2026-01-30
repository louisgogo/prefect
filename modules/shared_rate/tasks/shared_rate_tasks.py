"""综合比例计算相关 Tasks"""
from mypackage.utilities import connect_to_db, delete_data_add_data_by_DateRange
from prefect import task
import pandas as pd
import sys
import os
# 添加根目录到路径（prefect目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@task(name="load_bus_profit_for_shared_rate", log_prints=True)
def load_bus_profit_for_shared_rate_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从 fact_bus_profit 表获取各业务线的收入、毛利润、净利润

    Args:
        date_range: 日期范围

    Returns:
        业务线利润数据 DataFrame，包含 date, bus_line, prim_subj, amt
    """
    try:
        conn, cur = connect_to_db()

        # 查询业务线利润数据，排除"无"和"抵销数"，通过关联 dim_org_struc 表筛选前台和中台
        cur.execute("""
            SELECT p.acct_period as date, p.bus_line, p.prim_subj, p.amt as amt
            FROM fact_bus_profit p
            LEFT JOIN dim_org_struc d ON d.unique_lvl = p.unique_lvl
            WHERE p.bus_line NOT IN ('无', '抵销数')
            AND d.category IN ('前台', '中台')
            AND p.acct_period >= %s AND p.acct_period <= %s
            AND p.prim_subj IN ('营业收入', '毛利润', '净利润')
        """, (date_range.min(), date_range.max()))

        df = pd.DataFrame(cur.fetchall(), columns=[
                          desc[0] for desc in cur.description])
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].isin(date_range)]

        # 确保 amt 为数值类型
        df['amt'] = pd.to_numeric(df['amt'], errors='coerce')
        df = df[df['amt'].notna() & (df['amt'] != 0)]

        print(f"加载业务线利润数据完成，共 {len(df)} 条记录")
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"加载业务线利润数据时发生错误: {str(e)}")
        raise


@task(name="load_personnel_for_shared_rate", log_prints=True)
def load_personnel_for_shared_rate_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从 fact_personnel 表获取人数数据

    Args:
        date_range: 日期范围

    Returns:
        人数数据 DataFrame，包含 date, unique_lvl, person_count
    """
    try:
        conn, cur = connect_to_db()

        # 查询发薪人数数据，通过关联 dim_org_struc 表筛选前台和中台
        cur.execute("""
            SELECT p.date, p.unique_lvl, 
                   SUM(p.num_people) as person_count
            FROM fact_personnel p
            LEFT JOIN dim_org_struc d ON d.unique_lvl = p.unique_lvl
            WHERE p.class = '发薪人数'
            AND d.category IN ('前台', '中台')
            AND p.date >= %s AND p.date <= %s
            GROUP BY p.date, p.unique_lvl
        """, (date_range.min(), date_range.max()))

        df = pd.DataFrame(cur.fetchall(), columns=[
                          desc[0] for desc in cur.description])

        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].isin(date_range)]

        print(f"加载人数数据完成，共 {len(df)} 条记录")
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"加载人数数据时发生错误: {str(e)}")
        raise


@task(name="load_human_cost_for_shared_rate", log_prints=True)
def load_human_cost_for_shared_rate_task(date_range: pd.DatetimeIndex) -> pd.DataFrame:
    """
    从 fact_bus_expense 表获取人力费用，并计算按组织的业务线比例

    Args:
        date_range: 日期范围

    Returns:
        人力费用比例数据 DataFrame，包含 date, unique_lvl, bus_line, expense_ratio
    """
    try:
        conn, cur = connect_to_db()

        # 关联 dim_exp_item 表筛选人力费用，关联 dim_org_struc 表筛选前台和中台
        cur.execute("""
            SELECT 
                e.acct_period as date,
                e.unique_lvl,
                e.bus_line,
                SUM(e.exp_amt) as total_expense
            FROM fact_bus_expense e
            LEFT JOIN dim_exp_item d ON d.encoding = e.exp_item_code
            LEFT JOIN dim_org_struc o ON o.unique_lvl = e.unique_lvl
            WHERE d.exp_nature = '人力费用'
            AND o.category IN ('前台', '中台')
            AND e.acct_period >= %s AND e.acct_period <= %s
            GROUP BY e.acct_period, e.unique_lvl, e.bus_line
        """, (date_range.min(), date_range.max()))

        df = pd.DataFrame(cur.fetchall(), columns=[
                          desc[0] for desc in cur.description])

        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].isin(date_range)]

        # 计算每个组织在各业务线的费用比例
        # 按日期和组织汇总总费用
        df_total = df.groupby(['date', 'unique_lvl'])[
            'total_expense'].sum().reset_index()
        df_total = df_total.rename(
            columns={'total_expense': 'total_org_expense'})

        # 合并并计算比例
        df = df.merge(df_total, on=['date', 'unique_lvl'], how='left')
        df['expense_ratio'] = df['total_expense'] / df['total_org_expense']
        df = df[['date', 'unique_lvl', 'bus_line', 'expense_ratio']]

        # 处理除零和空值
        df = df[df['expense_ratio'].notna() & (df['expense_ratio'] != 0)]

        print(f"加载人力费用比例数据完成，共 {len(df)} 条记录")
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"加载人力费用比例数据时发生错误: {str(e)}")
        raise


@task(name="calculate_personnel_allocation", log_prints=True)
def calculate_personnel_allocation_task(
    df_personnel: pd.DataFrame,
    df_human_cost: pd.DataFrame
) -> pd.DataFrame:
    """
    使用人力费用比例乘以人数，获得人数的业务线分配

    Args:
        df_personnel: 人数数据（Task 2 输出）
        df_human_cost: 人力费用比例数据（Task 3 输出）

    Returns:
        人数分配数据 DataFrame，包含 date, bus_line, personnel_count
    """
    try:
        # 合并人数数据和费用比例数据（按 unique_lvl 和 date）
        df_merged = df_personnel.merge(
            df_human_cost,
            on=['date', 'unique_lvl'],
            how='left'
        )

        # 计算分配到各业务线的人数
        df_merged['personnel_count'] = df_merged['person_count'] * \
            df_merged['expense_ratio']
        df_merged = df_merged[df_merged['personnel_count'].notna() & (
            df_merged['personnel_count'] != 0)]

        # 按 date 和 bus_line 汇总
        df_result = df_merged.groupby(['date', 'bus_line'])[
            'personnel_count'].sum().reset_index()

        print(f"计算人数业务线分配完成，共 {len(df_result)} 条记录")
        return df_result
    except Exception as e:
        print(f"计算人数业务线分配时发生错误: {str(e)}")
        raise


@task(name="calculate_comprehensive_rate", log_prints=True)
def calculate_comprehensive_rate_task(
    df_profit: pd.DataFrame,
    df_personnel_allocation: pd.DataFrame
) -> pd.DataFrame:
    """
    计算四个指标的加权平均（等权重25%）

    Args:
        df_profit: 业务线利润数据（Task 1 输出）
        df_personnel_allocation: 人数分配数据（Task 4 输出）

    Returns:
        综合比例数据 DataFrame，包含 date, bus_line, rate
    """
    try:
        # 1. 从利润数据中提取收入、毛利润、净利润
        df_revenue = df_profit[df_profit['prim_subj'] == '营业收入'].copy()
        df_gross_profit = df_profit[df_profit['prim_subj'] == '毛利润'].copy()
        df_net_profit = df_profit[df_profit['prim_subj'] == '净利润'].copy()

        # 2. 按 date 和 bus_line 汇总各指标
        df_revenue_sum = df_revenue.groupby(['date', 'bus_line'])[
            'amt'].sum().reset_index()
        df_revenue_sum = df_revenue_sum.rename(columns={'amt': 'revenue'})

        df_gross_profit_sum = df_gross_profit.groupby(['date', 'bus_line'])[
            'amt'].sum().reset_index()
        df_gross_profit_sum = df_gross_profit_sum.rename(
            columns={'amt': 'gross_profit'})

        df_net_profit_sum = df_net_profit.groupby(['date', 'bus_line'])[
            'amt'].sum().reset_index()
        df_net_profit_sum = df_net_profit_sum.rename(
            columns={'amt': 'net_profit'})

        # 合并所有指标
        df_all = df_revenue_sum.merge(
            df_gross_profit_sum, on=['date', 'bus_line'], how='outer'
        ).merge(
            df_net_profit_sum, on=['date', 'bus_line'], how='outer'
        ).merge(
            df_personnel_allocation, on=['date', 'bus_line'], how='outer'
        )

        # 填充空值为0
        df_all = df_all.fillna(0)

        # 确保所有数值列为 float 类型（避免 Decimal 和 float 类型不一致的问题）
        numeric_cols = ['revenue', 'gross_profit',
                        'net_profit', 'personnel_count']
        for col in numeric_cols:
            if col in df_all.columns:
                df_all[col] = pd.to_numeric(
                    df_all[col], errors='coerce').astype(float)

        # 对所有金额指标应用绝对值，确保都是正数
        amount_cols = ['revenue', 'gross_profit',
                       'net_profit', 'personnel_count']
        for col in amount_cols:
            if col in df_all.columns:
                df_all[col] = df_all[col].abs()

        # 3. 计算各业务线的比例
        # 按日期计算总和（在取绝对值后重新计算）
        df_total = df_all.groupby('date').agg({
            'revenue': 'sum',
            'gross_profit': 'sum',
            'net_profit': 'sum',
            'personnel_count': 'sum'
        }).reset_index()
        df_total = df_total.rename(columns={
            'revenue': 'total_revenue',
            'gross_profit': 'total_gross_profit',
            'net_profit': 'total_net_profit',
            'personnel_count': 'total_personnel'
        })

        # 合并并计算比例
        df_all = df_all.merge(df_total, on='date', how='left')

        # 确保总和列也是 float 类型
        total_cols = ['total_revenue', 'total_gross_profit',
                      'total_net_profit', 'total_personnel']
        for col in total_cols:
            if col in df_all.columns:
                df_all[col] = pd.to_numeric(
                    df_all[col], errors='coerce').astype(float)

        # 计算各指标的比例（避免除零）
        df_all['revenue_ratio'] = df_all.apply(
            lambda x: x['revenue'] /
            x['total_revenue'] if x['total_revenue'] != 0 else 0.0,
            axis=1
        )
        df_all['gross_profit_ratio'] = df_all.apply(
            lambda x: x['gross_profit'] /
            x['total_gross_profit'] if x['total_gross_profit'] != 0 else 0.0,
            axis=1
        )
        df_all['net_profit_ratio'] = df_all.apply(
            lambda x: x['net_profit'] /
            x['total_net_profit'] if x['total_net_profit'] != 0 else 0.0,
            axis=1
        )
        df_all['personnel_ratio'] = df_all.apply(
            lambda x: x['personnel_count'] /
            x['total_personnel'] if x['total_personnel'] != 0 else 0.0,
            axis=1
        )

        # 确保比例列也是 float 类型
        ratio_cols = ['revenue_ratio', 'gross_profit_ratio',
                      'net_profit_ratio', 'personnel_ratio']
        for col in ratio_cols:
            df_all[col] = pd.to_numeric(
                df_all[col], errors='coerce').astype(float)

        # 4. 计算综合比例 = (收入比例 + 毛利润比例 + 净利润比例 + 人数比例) / 4
        df_all['rate'] = (
            df_all['revenue_ratio'] +
            df_all['gross_profit_ratio'] +
            df_all['net_profit_ratio'] +
            df_all['personnel_ratio']
        ) / 4.0

        # 输出详细的计算结果（方便在 Prefect UI 中查看）
        print("\n" + "="*80)
        print("综合比例计算详细结果")
        print("="*80)

        # 按日期分组输出
        for date in sorted(df_all['date'].unique()):
            df_date = df_all[df_all['date'] == date].copy()
            print(f"\n日期: {date.strftime('%Y-%m-%d')}")
            print("-"*80)
            print(
                f"{'业务线':<20} {'收入比例':<12} {'毛利润比例':<12} {'净利润比例':<12} {'人数比例':<12} {'综合比例':<12}")
            print("-"*80)

            # 按综合比例降序排列
            df_date = df_date.sort_values('rate', ascending=False)

            for _, row in df_date.iterrows():
                if pd.notna(row['rate']) and row['rate'] != 0:
                    print(f"{str(row['bus_line']):<20} "
                          f"{row['revenue_ratio']:>11.4%} "
                          f"{row['gross_profit_ratio']:>11.4%} "
                          f"{row['net_profit_ratio']:>11.4%} "
                          f"{row['personnel_ratio']:>11.4%} "
                          f"{row['rate']:>11.4%}")

            # 输出汇总信息
            print("-"*80)
            print(f"总计 - 收入: {df_date['revenue'].sum():,.2f}, "
                  f"毛利润: {df_date['gross_profit'].sum():,.2f}, "
                  f"净利润: {df_date['net_profit'].sum():,.2f}, "
                  f"人数: {df_date['personnel_count'].sum():,.0f}")
            print(f"综合比例总和: {df_date['rate'].sum():.4%}")

        print("\n" + "="*80)
        print("综合比例计算完成")
        print("="*80 + "\n")

        # 只保留需要的列
        df_result = df_all[['date', 'bus_line', 'rate']].copy()
        df_result = df_result[df_result['rate'].notna() &
                              (df_result['rate'] != 0)]

        print(f"计算综合比例完成，共 {len(df_result)} 条记录")
        return df_result
    except Exception as e:
        print(f"计算综合比例时发生错误: {str(e)}")
        raise


@task(name="save_shared_rate", log_prints=True)
def save_shared_rate_task(
    df_shared_rate: pd.DataFrame,
    date_range: pd.DatetimeIndex
) -> None:
    """
    保存综合比例到 fact_bus_shared_rate 表

    Args:
        df_shared_rate: 综合比例数据
        date_range: 日期范围（用于删除指定月份的数据）
    """
    try:
        # 确保日期格式正确
        df_shared_rate['date'] = pd.to_datetime(df_shared_rate['date'])

        # 只保留日期范围内的数据
        df_shared_rate = df_shared_rate[df_shared_rate['date'].isin(
            date_range)]

        # 删除 id 列（如果存在），让数据库自动生成
        if 'id' in df_shared_rate.columns:
            df_shared_rate = df_shared_rate.drop(['id'], axis=1)

        # 使用 delete_data_add_data_by_DateRange 保存，只删除计算月份的数据
        delete_data_add_data_by_DateRange(
            table_name='fact_bus_shared_rate',
            date_column='date',
            df=df_shared_rate,
            df_date_column='date',
            date_range=date_range
        )

        print(f"保存综合比例到数据库完成，共 {len(df_shared_rate)} 条记录")
    except Exception as e:
        print(f"保存综合比例到数据库时发生错误: {str(e)}")
        raise
