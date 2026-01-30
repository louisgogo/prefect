"""数据导入相关 Tasks"""
from mypackage.mapping import combined_table_mapping, combined_column_mapping
from mypackage.utilities import (
    read_and_map_excel,
    update_full_table,
    update_between_dates,
    add_data,
    url_to_db,
)
from prefect import task
from typing import Dict, Optional, Tuple
import pandas as pd
import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, text

# 添加根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))


def _check_data_exists(
    table_name: str,
    table_date_column: str,
    start_date: str,
    end_date: str
) -> bool:
    """
    检查指定日期范围内是否存在数据（辅助函数）

    Args:
        table_name: 表名
        table_date_column: 日期列名
        start_date: 开始日期 (格式: 'YYYY-MM-DD')
        end_date: 结束日期 (格式: 'YYYY-MM-DD')

    Returns:
        如果存在数据返回 True，否则返回 False
    """
    try:
        db_url = url_to_db()
        engine = create_engine(db_url)

        with engine.connect() as connection:
            query = text(
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {table_date_column} >= :start_date "
                f"AND {table_date_column} <= :end_date"
            )
            result = connection.execute(
                query,
                {"start_date": start_date, "end_date": end_date}
            )
            count = result.fetchone()[0]

            exists = count > 0
            if exists:
                print(
                    f"{table_name}: 在 {start_date} 到 {end_date} 范围内已存在 {count} 条数据")
            else:
                print(f"{table_name}: 在 {start_date} 到 {end_date} 范围内不存在数据")

            return exists
    except Exception as e:
        print(f"检查数据是否存在时发生错误: {str(e)}")
        # 如果表不存在，返回 False
        return False


@task(name="read_excel_data", log_prints=True)
def read_excel_data_task(root_directory: str) -> Dict[str, pd.DataFrame]:
    """
    读取 Excel 文件并映射数据

    Args:
        root_directory: Excel 文件根目录路径

    Returns:
        包含所有映射后数据的字典
    """
    try:
        print(f"开始读取 Excel 数据，目录: {root_directory}")
        dfs = read_and_map_excel(
            root_directory, combined_table_mapping, combined_column_mapping)
        print(f"Excel 数据读取完成，共 {len(dfs)} 个表")
        print(f"表名列表: {list(dfs.keys())}")
        return dfs
    except Exception as e:
        print(f"读取 Excel 数据时发生错误: {str(e)}")
        raise


@task(name="update_data_by_date_range", log_prints=True)
def update_data_by_date_range_task(
    table_name: str,
    table_date_column: str,
    df: pd.DataFrame,
    df_date_column: str,
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    按日期范围更新数据

    Args:
        table_name: 表名
        table_date_column: 表中日期列名
        df: 数据 DataFrame
        df_date_column: DataFrame 中日期列名
        start_date: 开始日期 (格式: 'YYYY-MM-DD')
        end_date: 结束日期 (格式: 'YYYY-MM-DD')
        replace_existing: 是否替换已存在的数据，默认 True
    """
    try:
        # 先检查 DataFrame 中是否有数据
        if df.empty:
            print(f"警告: {table_name} 的 DataFrame 为空，跳过更新操作（避免删除历史数据）")
            return

        # 检查日期列是否存在
        if df_date_column not in df.columns:
            print(
                f"警告: {table_name} 的 DataFrame 中不存在日期列 '{df_date_column}'，跳过更新操作")
            return

        # 过滤出指定日期范围内的数据
        df[df_date_column] = pd.to_datetime(df[df_date_column])
        df[df_date_column] = df[df_date_column].dt.date
        filtered_df = df[
            (df[df_date_column] >= pd.to_datetime(start_date).date()) &
            (df[df_date_column] <= pd.to_datetime(end_date).date())
        ]

        # 如果过滤后的数据为空，不执行删除操作，避免删除历史数据
        if filtered_df.empty:
            print(
                f"警告: {table_name} 在 {start_date} 到 {end_date} 范围内没有数据，跳过更新操作（避免删除历史数据）")
            return

        if replace_existing:
            print(
                f"✓ 更新 {table_name} 数据（替换模式）: {start_date} 到 {end_date}，共 {len(filtered_df)} 条数据")
            update_between_dates(table_name, table_date_column,
                                 df, df_date_column, start_date, end_date)
        else:
            # 检查是否已存在数据
            exists = _check_data_exists(
                table_name, table_date_column, start_date, end_date)
            if exists:
                print(f"⊘ 跳过 {table_name} 数据更新（已存在数据，replace_existing=False）")
            else:
                print(
                    f"✓ 更新 {table_name} 数据（追加模式）: {start_date} 到 {end_date}，共 {len(filtered_df)} 条数据")
                update_between_dates(table_name, table_date_column,
                                     df, df_date_column, start_date, end_date)
    except Exception as e:
        error_msg = f"更新 {table_name} 数据时发生错误: {str(e)}"
        print(f"❌ {error_msg}")
        # 打印完整的错误堆栈信息，便于调试
        import traceback
        print(f"错误详情:\n{traceback.format_exc()}")
        # 重新抛出异常，让 Prefect UI 能够捕获并显示
        raise Exception(error_msg) from e


@task(name="update_production_data", log_prints=True)
def update_production_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    更新生产数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
    """
    print("=" * 60)
    print("开始更新生产数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    # 1. 更新完工入库表
    table_name = 'excel_finished_goods_in'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'term', start_date, end_date)
                if exists:
                    print(f"跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'term', df, 'term', start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'term', df, 'term', start_date, end_date, replace_existing
                )
                updated_count += 1

    # 2. 更新物料调整表（全表更新）
    table_name = 'excel_finished_goods_adj'
    if table_name in dfs:
        df = dfs[table_name].copy()
        df['work_order_material_number'] = (
            df['bus_date'].dt.year.astype(str) +
            df['bus_date'].dt.month.astype(str) + '-' +
            df['work_order_number'] + '-' +
            df['product_code']
        )
        df = df.groupby(['work_order_material_number']).agg({
            'amt': 'sum',
            'quantity': 'sum'
        }).reset_index()
        df['weighted_unit_price'] = df['amt'] / df['quantity']
        df = df[['work_order_material_number', 'weighted_unit_price']]

        # 检查数据是否为空，避免清空整个表
        if df.empty:
            print(f"警告: {table_name} 的 DataFrame 为空，跳过全表更新操作（避免清空历史数据）")
            skipped_count += 1
        else:
            print(f"更新 {table_name}（全表更新），共 {len(df)} 条数据")
            update_full_table(table_name, df)
            updated_count += 1

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"生产数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"生产数据更新完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"生产数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("生产数据检查完成：无数据需要更新")


@task(name="update_rd_data", log_prints=True)
def update_rd_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    更新研发数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
    """
    print("=" * 60)
    print("开始更新研发数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    # 1. 工时统计表（全表更新）
    table_name = 'excel_labor_hours'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 执行逆透视操作
        df = pd.melt(
            df,
            id_vars=['proj_name', 'product_sub_category', 'year'],
            value_vars=['1月', '2月', '3月', '4月', '5月', '6月',
                        '7月', '8月', '9月', '10月', '11月', '12月'],
            var_name='month',
            value_name='hours_worked'
        )
        df = df[df['hours_worked'].notna()]
        df = df[df['hours_worked'] > 0]
        df['year'] = df['year'].astype(float).astype(int).astype(str)
        df['month'] = df['month'].str.extract('(\d+)').astype(int)
        df['date'] = pd.to_datetime(
            df['year'] + '-' + df['month'].astype(str) + '-01')
        df = df[['proj_name', 'product_sub_category', 'hours_worked', 'date']]

        # 检查数据是否为空，避免清空整个表
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            print(f"✓ 更新 {table_name}（全表更新），共 {len(df)} 条数据")
            update_full_table(table_name, df)
            updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 2. 打样模具、技术维护、研发项目、领料费用
    tables_config = [
        ('excel_sample_molds', 'bus_date', 'bus_date'),
        ('excel_tech_maintenance', 'date', 'date'),
        ('excel_dev_projects', 'date', 'date'),
        ('excel_material_usage_costs', 'date', 'date'),
    ]

    for table_name, table_date_column, df_date_column in tables_config:
        if table_name in dfs:
            df = dfs[table_name].copy()
            # 先检查 DataFrame 是否为空
            if df.empty:
                print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
                skipped_count += 1
                continue

            if not replace_existing:
                exists = _check_data_exists(
                    table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name, table_date_column, df, df_date_column,
                start_date, end_date, replace_existing
            )
            updated_count += 1
        else:
            print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
            skipped_count += 1

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"研发数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"研发数据检查完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"研发数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("研发数据检查完成：无数据需要更新")


@task(name="update_purchase_data", log_prints=True)
def update_purchase_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    更新采购数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
    """
    print("=" * 60)
    print("开始更新采购数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    table_name = 'excel_purchase_cost_red'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'date', df, 'date', start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'date', df, 'date', start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"采购数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"采购数据检查完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"采购数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("采购数据检查完成：无数据需要更新")


@task(name="update_inventory_data", log_prints=True)
def update_inventory_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    更新存货数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
    """
    print("=" * 60)
    print("开始更新存货数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    table_name = 'excel_inventory_turn'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'date', df, 'date', start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'date', df, 'date', start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"存货数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"存货数据检查完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"存货数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("存货数据检查完成：无数据需要更新")


@task(name="update_cost_control_data", log_prints=True)
def update_cost_control_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    更新费控数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
    """
    print("=" * 60)
    print("开始更新费控数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    table_name = 'excel_cost_control'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            # 处理数据
            df['submission_date'] = pd.to_datetime(df['submission_date'])
            df['year'] = df['submission_date'].dt.year
            df = df[['submission_date', 'description', 'budget_department_code',
                     'budget_department_name', 'document_number', 'submitter_code',
                     'submitter_name', 'year']]

            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'submission_date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'submission_date', df, 'submission_date',
                        start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'submission_date', df, 'submission_date',
                    start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"费控数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"费控数据检查完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"费控数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("费控数据检查完成：无数据需要更新")


@task(name="update_business_data", log_prints=True)
def update_business_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = False,
    root_directory: Optional[str] = None
) -> None:
    """
    更新业务数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
        root_directory: Excel 文件根目录（用于读取跨境数据）
    """
    print("=" * 60)
    print("开始更新业务数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    # 更新常规业务数据表
    tables_config = [
        ('excel_price_inc_profits', 'date', 'date'),
        ('excel_esign_shipments', 'month', 'month'),
        ('excel_sales_stats', 'date', 'date'),
        ('excel_powerbank_fin', 'month', 'month'),
        ('excel_powerbank_ops', 'month', 'month'),
    ]

    for table_name, table_date_column, df_date_column in tables_config:
        if table_name in dfs:
            df = dfs[table_name].copy()
            # 先检查 DataFrame 是否为空
            if df.empty:
                print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
                skipped_count += 1
                continue

            if not replace_existing:
                exists = _check_data_exists(
                    table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name, table_date_column, df, df_date_column,
                start_date, end_date, replace_existing
            )
            updated_count += 1
        else:
            print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
            skipped_count += 1

    # 更新跨境业务数据（需要特殊处理）
    if root_directory:
        try:
            import os
            file_path = os.path.join(
                root_directory, '7.业务数据', '跨境', '周报取数模板.xlsx')
            if os.path.exists(file_path):
                df_user = pd.read_excel(file_path, sheet_name='用户数').drop(
                    ['周'], axis=1).groupby(['年', '月']).sum().reset_index()
                df_amt = pd.read_excel(file_path, sheet_name='入账提现金额').loc[:, [
                    '年', '月', '入账笔数', '入账成功总金额', '电商入账笔数', '电商入账成功总金额']].groupby(['年', '月']).sum().reset_index()
                df_curr = pd.read_excel(file_path, sheet_name='入账金额分币种').drop(
                    ['周', '入账成功总金额'], axis=1).groupby(['年', '月']).sum().reset_index()
                df_qr = pd.read_excel(file_path, sheet_name='码付').drop(
                    ['周', '单笔均额-码付', '费率-码付'], axis=1).groupby(['年', '月']).sum().reset_index()

                df = pd.merge(df_user, df_amt, on=['年', '月'], how='outer')
                df = pd.merge(df, df_curr, on=['年', '月'], how='outer')
                df = pd.merge(df, df_qr, on=['年', '月'], how='outer')

                if df['月'].isnull().any():
                    print("合并后的年月存在空值")

                df['日期'] = df['年'].astype(
                    str) + '-' + df['月'].astype(str) + '-01'
                df = df.drop(['年', '月'], axis=1)
                df.columns = [combined_column_mapping.get(
                    col, col) for col in df.columns]

                table_name = 'excel_cross_border'
                if not replace_existing:
                    exists = _check_data_exists(
                        table_name, 'date', start_date, end_date)
                    if exists:
                        print(f"⊘ 跳过 {table_name}（已存在数据）")
                        skipped_count += 1
                    else:
                        update_data_by_date_range_task(
                            table_name, 'date', df, 'date',
                            start_date, end_date, replace_existing
                        )
                        updated_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'date', df, 'date',
                        start_date, end_date, replace_existing
                    )
                    updated_count += 1
        except Exception as e:
            print(f"更新跨境业务数据时发生错误: {str(e)}")

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"业务数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"业务数据检查完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"业务数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("业务数据检查完成：无数据需要更新")


@task(name="update_personnel_data", log_prints=True)
def update_personnel_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    更新人力费用数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
    """
    print("=" * 60)
    print("开始更新人力费用数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    table_name = 'fact_personnel'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'date', df, 'date', start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'date', df, 'date', start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"人力费用数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"人力费用数据检查完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"人力费用数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("人力费用数据检查完成：无数据需要更新")


@task(name="update_manual_refresh_data", log_prints=True)
def update_manual_refresh_data_task(
    dfs: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    replace_existing: bool = True
) -> None:
    """
    更新手工刷新数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
    """
    print("=" * 60)
    print("开始更新手工刷新数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    # 更新汇率表
    table_name = 'excel_exchange_rates'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'effective_date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'effective_date', df, 'effective_date',
                        start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'effective_date', df, 'effective_date',
                    start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新利润表
    table_name = 'fact_profit_stmt'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'date', df, 'date', start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'date', df, 'date', start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新其他明细表
    tables_config = [
        ('fact_receipt', 'date', 'date'),
        ('fact_profit_bd', 'date', 'date'),
        ('fact_expense', 'acct_period', 'acct_period'),
        ('fact_inventory', 'acct_period', 'acct_period'),
        ('fact_receivable', 'acct_period', 'acct_period'),
        ('fact_revenue', 'acct_period', 'acct_period'),
        ('fact_inventory_on_way', 'acct_period', 'acct_period'),
    ]

    for table_name, table_date_column, df_date_column in tables_config:
        if table_name in dfs:
            df = dfs[table_name].copy()
            # 先检查 DataFrame 是否为空
            if df.empty:
                print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
                skipped_count += 1
                continue

            if not replace_existing:
                exists = _check_data_exists(
                    table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name, table_date_column, df, df_date_column,
                start_date, end_date, replace_existing
            )
            updated_count += 1
        else:
            print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
            skipped_count += 1

    # 更新抵消表
    table_name = 'fact_offset'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'date', df, 'date', start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'date', df, 'date', start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新业务线工资比重
    table_name = 'fact_bus_wage_rate'
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(
                    table_name, 'date', start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, 'date', df, 'date', start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, 'date', df, 'date', start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新现金流量表
    tables_config = [
        ('fact_cashflow', 'date', 'date'),
        ('excel_cashflow_intl', 'date', 'date'),
    ]

    for table_name, table_date_column, df_date_column in tables_config:
        if table_name in dfs:
            df = dfs[table_name].copy()
            # 先检查 DataFrame 是否为空
            if df.empty:
                print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
                skipped_count += 1
                continue

            if not replace_existing:
                exists = _check_data_exists(
                    table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name, table_date_column, df, df_date_column,
                start_date, end_date, replace_existing
            )
            updated_count += 1
        else:
            print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
            skipped_count += 1

    # 输出总结
    if updated_count > 0 and skipped_count > 0:
        print(f"手工刷新数据检查完成：已更新 {updated_count} 个表，跳过 {skipped_count} 个表")
    elif updated_count > 0:
        print(f"手工刷新数据检查完成：已更新 {updated_count} 个表")
    elif skipped_count > 0:
        print(f"手工刷新数据检查完成：跳过 {skipped_count} 个表（已存在数据或无数据需要更新）")
    else:
        print("手工刷新数据检查完成：无数据需要更新")
