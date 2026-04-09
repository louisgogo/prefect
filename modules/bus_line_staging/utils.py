import pandas as pd
import numpy as np
from mypackage.utilities import engine_to_db
from mypackage.mapping import combined_column_mapping

def insert_to_staging_table(df, df_org, groups, date_range, date_column, table_name, bus_lines, is_split_others=True, is_by_df=True):
    """
    将按业务线拆分好的数据直接插入PostgreSQL中间表，而不导出为Excel。
    采用了打竖（EAV）模式写入DB。
    """
    engine = engine_to_db()
    unique_lvls_used = []

    # 获取所有的分组
    if groups:
        for group in groups:
            try:
                unique_lvls = df_org[df_org.short_name == group].unique_lvl.to_list()
                unique_lvls_used.extend(unique_lvls)

                # 筛选出属于此分组的层级且在指定日期范围内的数据
                filtered_df = df[df['唯一层级'].isin(unique_lvls)].copy()
                if not filtered_df.empty:
                    filtered_df = filtered_df[filtered_df[date_column].isin(date_range)]

                if not filtered_df.empty:
                    filtered_df['audit_status'] = 'PENDING'

                    # 运用 EAV 模型，将所有业务线打竖（unpivot）
                    id_vars = [col for col in filtered_df.columns if col not in bus_lines]
                    value_vars = [col for col in filtered_df.columns if col in bus_lines]

                    melted_df = filtered_df.melt(
                        id_vars=id_vars,
                        value_vars=value_vars,
                        var_name='bus_line_name',
                        value_name='allocated_amount'
                    )

                    # 去除由于填充而产生的空值或全0行
                    melted_df = melted_df.dropna(subset=['allocated_amount'])
                    melted_df = melted_df[melted_df['allocated_amount'] != 0]

                    # 重命名列名以适配英文数据库表
                    mapped_columns = {col: combined_column_mapping.get(col, col) for col in melted_df.columns}
                    melted_df.rename(columns=mapped_columns, inplace=True)

                    # 追加到数据库表
                    melted_df.to_sql(table_name, con=engine, if_exists='append', index=False)
                    print(f"Data for group {group} inserted into {table_name}.")
            except Exception as e:
                print(f"Error inserting group {group} into DB: {e}")

    # 是否需要将剩余的层级塞入到中间表中
    if is_split_others:
        if is_by_df:
            all_lvls = df['唯一层级'].unique().tolist()
            filtered_list = [item for item in all_lvls if item not in unique_lvls_used]
        else:
            filtered_list = [item for item in df_org['unique_lvl'].to_list() if item not in unique_lvls_used]

        if filtered_list:
            filtered_df = df[df['唯一层级'].isin(filtered_list)].copy()
            if not filtered_df.empty:
                filtered_df = filtered_df[filtered_df[date_column].isin(date_range)]

            if not filtered_df.empty:
                filtered_df['audit_status'] = 'PENDING'

                id_vars = [col for col in filtered_df.columns if col not in bus_lines]
                value_vars = [col for col in filtered_df.columns if col in bus_lines]

                melted_df = filtered_df.melt(
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name='bus_line_name',
                    value_name='allocated_amount'
                )

                melted_df = melted_df.dropna(subset=['allocated_amount'])
                melted_df = melted_df[melted_df['allocated_amount'] != 0]

                mapped_columns = {col: combined_column_mapping.get(col, col) for col in melted_df.columns}
                melted_df.rename(columns=mapped_columns, inplace=True)

                melted_df.to_sql(table_name, con=engine, if_exists='append', index=False)
                print(f"Remaining data inserted into {table_name}.")
