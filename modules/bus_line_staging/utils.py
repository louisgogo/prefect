from io import StringIO

import numpy as np
import pandas as pd
from mypackage.mapping import combined_column_mapping
from mypackage.utilities import connect_to_db, engine_to_db


def insert_to_staging_table(
    df,
    df_org,
    groups,
    date_range,
    date_column,
    table_name,
    bus_lines,
    is_split_others=True,
    is_by_df=True,
):
    """
    将按业务线拆分好的数据直接插入PostgreSQL中间表，使用COPY命令提高性能。
    采用了打竖（EAV）模式写入DB。
    """
    conn, cur = connect_to_db()
    unique_lvls_used = []
    all_data = []  # 收集所有数据，批量插入

    # 获取所有的分组
    if groups:
        for group in groups:
            try:
                unique_lvls = df_org[df_org.short_name == group].unique_lvl.to_list()
                unique_lvls_used.extend(unique_lvls)

                # 筛选出属于此分组的层级且在指定日期范围内的数据
                filtered_df = df[df["唯一层级"].isin(unique_lvls)].copy()
                if not filtered_df.empty:
                    filtered_df = filtered_df[filtered_df[date_column].isin(date_range)]

                if not filtered_df.empty:
                    filtered_df["audit_status"] = "PENDING"

                    # 运用 EAV 模型，将所有业务线打竖（unpivot）
                    id_vars = [col for col in filtered_df.columns if col not in bus_lines]
                    value_vars = [col for col in filtered_df.columns if col in bus_lines]

                    melted_df = filtered_df.melt(
                        id_vars=id_vars,
                        value_vars=value_vars,
                        var_name="bus_line",
                        value_name="rate",
                    )

                    # 去除由于填充而产生的空值或全0行
                    melted_df = melted_df.dropna(subset=["rate"])
                    melted_df = melted_df[melted_df["rate"] != 0]

                    if not melted_df.empty:
                        all_data.append(melted_df)
                        print(f"Data for group {group} prepared for {table_name}.")
            except Exception as e:
                print(f"Error preparing group {group} for DB: {e}")

    # 是否需要将剩余的层级塞入到中间表中
    if is_split_others:
        if is_by_df:
            all_lvls = df["唯一层级"].unique().tolist()
            filtered_list = [item for item in all_lvls if item not in unique_lvls_used]
        else:
            filtered_list = [
                item for item in df_org["unique_lvl"].to_list() if item not in unique_lvls_used
            ]

        if filtered_list:
            filtered_df = df[df["唯一层级"].isin(filtered_list)].copy()
            if not filtered_df.empty:
                filtered_df = filtered_df[filtered_df[date_column].isin(date_range)]

            if not filtered_df.empty:
                filtered_df["audit_status"] = "PENDING"

                id_vars = [col for col in filtered_df.columns if col not in bus_lines]
                value_vars = [col for col in filtered_df.columns if col in bus_lines]

                melted_df = filtered_df.melt(
                    id_vars=id_vars, value_vars=value_vars, var_name="bus_line", value_name="rate"
                )

                melted_df = melted_df.dropna(subset=["rate"])
                melted_df = melted_df[melted_df["rate"] != 0]

                if not melted_df.empty:
                    all_data.append(melted_df)
                    print(f"Remaining data prepared for {table_name}.")

    # 使用 COPY 命令批量插入所有数据
    if all_data:
        try:
            final_df = pd.concat(all_data, ignore_index=True)
            # 重命名列名以适配英文数据库表
            mapped_columns = {
                col: combined_column_mapping.get(col, col) for col in final_df.columns
            }
            final_df.rename(columns=mapped_columns, inplace=True)

            # 获取数据库列名（排除 id 和 created_at，让数据库自动生成）
            cur.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
            )
            db_columns = [row[0] for row in cur.fetchall() if row[0] not in ["id", "created_at"]]

            # 只保留数据库中存在的列，并确保列顺序与数据库一致
            existing_columns = [col for col in db_columns if col in final_df.columns]
            final_df = final_df[existing_columns]

            # 使用 COPY 命令导入数据
            copy_data_to_postgres(final_df, table_name, existing_columns, conn, cur)

            print(f"Total {len(final_df)} records inserted into {table_name} via COPY.")
        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
            import traceback

            traceback.print_exc()
    else:
        print(f"No data to insert into {table_name}.")

    cur.close()
    conn.close()


def copy_data_to_postgres(df, table_name, columns, conn, cur):
    """
    使用 PostgreSQL COPY 命令高效导入数据
    比 to_sql 快 10-100 倍
    """
    import csv
    import time

    start_time = time.time()

    # 处理数据类型，确保日期格式正确
    df_copy = df.copy()
    for col in df_copy.columns:
        # 转换日期列为字符串格式
        if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
            df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d")
        # 处理 date 类型的列（object 类型但包含日期）
        elif col in ["acct_period", "date"] and df_copy[col].dtype == "object":
            try:
                df_copy[col] = pd.to_datetime(df_copy[col]).dt.strftime("%Y-%m-%d")
            except:
                pass
        # 将所有列转为字符串，处理 None 和 nan
        df_copy[col] = df_copy[col].astype(str).replace("nan", "\\N").replace("None", "\\N")

    # 将 DataFrame 转换为 CSV 格式的字符串
    buffer = StringIO()
    df_copy.to_csv(
        buffer,
        index=False,
        header=False,
        sep="\t",
        na_rep="\\N",
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )
    buffer.seek(0)

    # 使用 COPY 命令导入 (FORMAT text 而不是 csv)
    columns_str = ", ".join(columns)
    copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL '\\N')"

    try:
        cur.copy_expert(copy_sql, buffer)
        conn.commit()
        elapsed = time.time() - start_time
        print(f"COPY completed in {elapsed:.2f} seconds for {len(df)} records")
    except Exception as e:
        conn.rollback()
        print(f"COPY failed: {e}")
        # 降级到批量 INSERT
        print("Falling back to to_sql...")
        from sqlalchemy import create_engine

        engine = engine_to_db()
        df.to_sql(
            table_name, con=engine, if_exists="append", index=False, method="multi", chunksize=1000
        )
