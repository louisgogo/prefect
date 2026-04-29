from io import StringIO

import numpy as np
import pandas as pd
from mypackage.utilities import connect_to_db, engine_to_db

# 表结构定义（中文列名）
TABLE_SCHEMAS = {
    "staging_bus_expense": {
        "费用": [
            "来源编号",
            "单据编号",
            "报销人",
            "摘要",
            "会计期间",
            "费用性质",
            "费用大类",
            "核算项目-费控",
            "研发项目",
            "项目编码",
            "费用金额",
            "年份",
            "数据来源",
            "分摊业务线",
        ],
        "组织": ["唯一层级", "一级组织", "二级组织", "三级组织"],
    },
    "staging_bus_revenue": {
        "收入": [
            "来源编号",
            "会计期间",
            "收入大类",
            "产品大类",
            "物料名称",
            "不含税金额本位币",
            "成本金额",
            "运费成本",
            "关税成本",
            "软件成本",
            "年份",
            "数据来源",
        ],
        "组织": ["唯一层级", "一级组织", "二级组织", "三级组织"],
    },
    "staging_bus_profit_bd": {
        "其他": ["来源编号", "财报合并", "日期", "一级科目", "本月金额", "年份", "数据来源"],
        "组织": ["唯一层级", "一级组织", "二级组织", "三级组织"],
    },
    "staging_bus_inventory": {
        "存货": [
            "来源编号",
            "财报合并",
            "财报单体",
            "物料编码",
            "物料名称",
            "存货类别",
            "客户类别",
            "客户编码",
            "客户名称",
            "仓库",
            "是否为备货物料",
            "数量(库存)",
            "参考价(基本)",
            "参考金额",
            "6个月以内数量",
            "6个月以内金额",
            "6-9个月数量",
            "6-9个月金额",
            "9个月-1年数量",
            "9个月-1年金额",
            "1-2年数量",
            "1-2年金额",
            "2-3年数量",
            "2-3年金额",
            "3年以上数量",
            "3年以上金额",
            "会计期间",
            "年份",
            "数据来源",
        ],
        "组织": ["唯一层级", "一级组织", "二级组织", "三级组织"],
    },
    "staging_bus_receivable": {
        "应收": [
            "来源编号",
            "财报合并",
            "财报单体",
            "一级科目",
            "客户编码",
            "往来单位",
            "往来性质",
            "客户类型",
            "销售部门",
            "销售区域",
            "赊销未核金额",
            "预收未核金额",
            "分期未核金额",
            "应收账款余额",
            "逾期金额",
            "未到期金额",
            "逾期30天以内金额",
            "逾期30天到90天金额",
            "逾期90天到180天金额",
            "逾期180天到360天金额",
            "逾期360天以上金额",
            "账龄3个月以内",
            "账龄3-6个月",
            "账龄6-9个月",
            "账龄9-12个月",
            "账龄1-2年",
            "账龄2-3年",
            "账龄3年以上",
            "本年借方发生额",
            "本年贷方发生额",
            "销售模块",
            "上个月逾期金额",
            "逾期变动",
            "本年回款金额",
            "会计期间",
            "业务大类",
            "业务小类",
            "应收状态",
            "备注",
            "年份",
            "数据来源",
        ],
        "组织": ["唯一层级", "一级组织", "二级组织", "三级组织"],
    },
    "staging_bus_in_transit_inventory": {
        "在途": [
            "来源编号",
            "财报合并",
            "财报单体",
            "订单号",
            "订单日期",
            "供应商编码",
            "供应商名称",
            "存货类别",
            "物料编码",
            "物料名称",
            "订单金额",
            "在途订单金额",
            "累计付款金额",
            "订单数量",
            "累计入库数量",
            "未入库数量",
            "交货日期",
            "会计期间",
            "年份",
            "数据来源",
        ],
        "组织": ["唯一层级", "一级组织", "二级组织", "三级组织"],
    },
}


def get_table_columns(table_name, bus_lines):
    """获取表的完整列名列表（含业务线）"""
    schema = TABLE_SCHEMAS.get(table_name, {})
    columns = []
    # 组织列放前面
    if "组织" in schema:
        columns.extend(schema["组织"])
    # 数据列
    for key in ["费用", "收入", "存货", "应收", "在途", "其他"]:
        if key in schema:
            columns.extend(schema[key])
    # 业务线列
    columns.extend(bus_lines)
    return columns


def create_staging_table(cur, table_name, bus_lines):
    """创建staging表（中文列名）。表不存在则创建；已存在则自动补全缺失的业务线列。"""
    schema = TABLE_SCHEMAS.get(table_name, {})

    # 检查表是否已存在
    cur.execute(f"SELECT to_regclass('{table_name}')")
    if cur.fetchone()[0] is not None:
        # 表已存在：检查并补全缺失的业务线列
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,),
        )
        existing_cols = {row[0] for row in cur.fetchall()}
        missing = [col for col in bus_lines if col not in existing_cols]
        if missing:
            for col in missing:
                cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" DECIMAL(10,4)')
                print(f'Table {table_name}: added missing column "{col}".')
        else:
            print(f"Table {table_name} already exists, columns aligned.")
        return

    # 表不存在：构建列定义并创建
    columns_def = ["id SERIAL PRIMARY KEY"]

    # 组织列
    if "组织" in schema:
        for col in schema["组织"]:
            columns_def.append(f'"{col}" VARCHAR(500)')

    # 数据列
    for key in ["费用", "收入", "存货", "应收", "在途", "其他"]:
        if key in schema:
            for col in schema[key]:
                if col in [
                    "费用金额",
                    "不含税金额本位币",
                    "成本金额",
                    "运费成本",
                    "关税成本",
                    "软件金额",
                    "本月金额",
                    "参考金额",
                    "在途订单金额",
                    "累计付款金额",
                    "订单金额",
                    "数量(库存)",
                    "参考价(基本)",
                    "6个月以内金额",
                    "6-9个月金额",
                    "9个月-1年金额",
                    "1-2年金额",
                    "2-3年金额",
                    "3年以上金额",
                    "赊销未核金额",
                    "预收未核金额",
                    "分期未核金额",
                    "应收账款余额",
                    "逾期金额",
                    "未到期金额",
                    "逾期30天以内金额",
                    "逾期30天到90天金额",
                    "逾期90天到180天金额",
                    "逾期180天到360天金额",
                    "逾期360天以上金额",
                    "账龄3个月以内",
                    "账龄3-6个月",
                    "账龄6-9个月",
                    "账龄9-12个月",
                    "账龄1-2年",
                    "账龄2-3年",
                    "账龄3年以上",
                    "本年借方发生额",
                    "本年贷方发生额",
                    "上个月逾期金额",
                    "逾期变动",
                    "本年回款金额",
                    "6个月以内数量",
                    "6-9个月数量",
                    "9个月-1年数量",
                    "1-2年数量",
                    "2-3年数量",
                    "3年以上数量",
                    "订单数量",
                    "累计入库数量",
                    "未入库数量",
                ]:
                    columns_def.append(f'"{col}" DECIMAL(20,4)')
                elif col in ["会计期间", "日期", "订单日期", "交货日期"]:
                    columns_def.append(f'"{col}" DATE')
                elif col == "年份":
                    columns_def.append(f'"{col}" INTEGER')
                else:
                    columns_def.append(f'"{col}" VARCHAR(500)')

    # 业务线列 - 存储拆分比例
    for col in bus_lines:
        columns_def.append(f'"{col}" DECIMAL(10,4)')

    # 添加 audit_status 和 created_at
    columns_def.append("\"审核状态\" VARCHAR(50) DEFAULT 'PENDING'")
    columns_def.append('"创建时间" TIMESTAMP DEFAULT CURRENT_TIMESTAMP')

    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns_def)})"
    cur.execute(create_sql)

    # 创建索引（根据表的不同使用不同的时间列）
    date_col = (
        "日期" if table_name in ["staging_bus_profit_bd", "staging_profit_unassigned"] else "会计期间"
    )
    cur.execute(f'CREATE INDEX idx_{table_name}_date ON {table_name}("{date_col}")')
    cur.execute(f'CREATE INDEX idx_{table_name}_lvl ON {table_name}("唯一层级")')

    print(f"Table {table_name} created with {len(columns_def)} columns")


def cleanup_staging_month(cur, table_name, date_column, date_range):
    """删除指定表在指定日期范围内的旧数据"""
    if len(date_range) > 0:
        placeholders = ",".join(["%s"] * len(date_range))
        delete_sql = f'DELETE FROM {table_name} WHERE "{date_column}" IN ({placeholders})'
        cur.execute(delete_sql, tuple(date_range))
        print(
            f"Cleaned up {cur.rowcount} old records from {table_name} for {len(date_range)} dates."
        )


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
    将数据插入PostgreSQL中间表（宽表格式，业务线直接作为列）。
    表不存在则创建，存在则直接追加数据。
    调用方应确保在 flow 级别已清理当月旧数据，避免重复。
    """
    conn, cur = connect_to_db()

    try:
        # 表不存在则创建
        create_staging_table(cur, table_name, bus_lines)

        if df.empty:
            print(f"No data to insert into {table_name}.")
            return

        # 筛选日期范围
        df_filtered = df[df[date_column].isin(date_range)].copy()
        if df_filtered.empty:
            print(f"No data in date range for {table_name}.")
            return

        # 获取所有数据
        all_data = []
        unique_lvls_used = []

        # 处理指定groups
        if groups:
            for group in groups:
                try:
                    unique_lvls = df_org[df_org.short_name == group].unique_lvl.to_list()
                    unique_lvls_used.extend(unique_lvls)

                    group_df = df_filtered[df_filtered["唯一层级"].isin(unique_lvls)].copy()
                    if not group_df.empty:
                        all_data.append(group_df)
                        print(f"Data for group {group} prepared for {table_name}.")
                except Exception as e:
                    print(f"Error preparing group {group}: {e}")

        # 处理其他数据
        if is_split_others:
            if is_by_df:
                all_lvls = df_filtered["唯一层级"].unique().tolist()
                remaining_lvls = [item for item in all_lvls if item not in unique_lvls_used]
            else:
                remaining_lvls = [
                    item for item in df_org["unique_lvl"].to_list() if item not in unique_lvls_used
                ]

            if remaining_lvls:
                others_df = df_filtered[df_filtered["唯一层级"].isin(remaining_lvls)].copy()
                if not others_df.empty:
                    all_data.append(others_df)
                    print(f"Remaining data prepared for {table_name}.")

        # 合并所有数据
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)

            # 获取表的所有列
            table_columns = get_table_columns(table_name, bus_lines)

            # 确保所有业务线列存在（保留已有值，缺失的设为NULL）
            for col in bus_lines:
                if col not in final_df.columns:
                    final_df[col] = None
                # 注意：如果col已存在，保留原值（不覆盖）

            # 添加审核状态
            final_df["审核状态"] = "PENDING"

            # 只保留表定义的列
            existing_cols = [col for col in table_columns if col in final_df.columns]
            final_df = final_df[existing_cols]

            # 使用COPY导入数据
            copy_data_to_postgres(final_df, table_name, existing_cols, conn, cur)
            print(f"Total {len(final_df)} records inserted into {table_name} via COPY.")
        else:
            print(f"No data to insert into {table_name}.")

    finally:
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
        elif col in ["会计期间", "日期", "订单日期", "交货日期"] and df_copy[col].dtype == "object":
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
    # 使用双引号包裹中文列名
    columns_str = ", ".join([f'"{col}"' for col in columns])
    copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL '\\\\N')"

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
