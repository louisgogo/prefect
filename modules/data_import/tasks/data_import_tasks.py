"""数据导入相关 Tasks"""

import os
import re
import sys
from datetime import datetime
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
from mypackage.mapping import combined_column_mapping, combined_table_mapping
from mypackage.utilities import (
    add_data,
    read_and_map_excel,
    update_between_dates,
    update_full_table,
    url_to_db,
)
from prefect import task
from sqlalchemy import create_engine, text

UNSTORED_COLUMNS_BY_TABLE = {
    "fact_inventory_on_way": {
        "order_amount",
        "total_payment_amount",
        "total_inventory_received",
    },
}

LINKED_FACT_TABLES = {
    "fact_revenue",
    "fact_expense",
    "fact_receivable",
    "fact_inventory",
    "fact_inventory_on_way",
    "fact_profit_bd",
}
FACT_IDENTITY_COLUMNS = ("id", "source_no", "business_report_staging_id")
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# 添加根目录到路径
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
)


def drop_unstored_columns(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=list(UNSTORED_COLUMNS_BY_TABLE.get(table_name, set())), errors="ignore")


def _normalized_source_keys(df: pd.DataFrame, *, context: str) -> pd.Series:
    if "source_no" not in df.columns:
        raise ValueError(f"{context} 缺少 source_no，无法安全保留 Fact 关联")
    blank = df["source_no"].isna() | df["source_no"].astype(str).str.strip().eq("")
    if blank.any():
        raise ValueError(f"{context} 存在 {int(blank.sum())} 条空 source_no")
    keys = df["source_no"].astype(str).str.strip()
    duplicated = keys.duplicated(keep=False)
    if duplicated.any():
        examples = ", ".join(keys[duplicated].drop_duplicates().head(5).tolist())
        raise ValueError(f"{context} 存在重复 source_no：{examples}")
    return keys


def merge_fact_identity_columns(
    incoming: pd.DataFrame,
    existing: pd.DataFrame,
    allocated_ids: Iterable[int],
) -> pd.DataFrame:
    """Preserve Fact identity/link columns while preparing a date-range replacement."""
    result = incoming.copy()
    incoming_keys = _normalized_source_keys(result, context="待导入 Fact 数据")
    result["source_no"] = incoming_keys
    existing_keys = _normalized_source_keys(existing, context="现有 Fact 数据")
    existing_identity = existing.copy()
    existing_identity["__source_key"] = existing_keys
    identity_by_source = existing_identity.set_index("__source_key")

    result["id"] = incoming_keys.map(identity_by_source["id"])
    result["business_report_staging_id"] = incoming_keys.map(
        identity_by_source["business_report_staging_id"]
    )

    new_mask = result["id"].isna()
    new_ids = list(allocated_ids)
    if len(new_ids) != int(new_mask.sum()):
        raise ValueError(f"新 Fact ID 数量不匹配：需要 {int(new_mask.sum())} 个，实际 {len(new_ids)} 个")
    if new_ids:
        result.loc[new_mask, "id"] = new_ids
    result.loc[new_mask, "business_report_staging_id"] = None
    result["id"] = result["id"].astype("int64")
    return result


def verify_fact_identity_rows(expected: pd.DataFrame, actual: pd.DataFrame) -> None:
    """Ensure replacement kept Fact IDs and staging UUID links exactly as prepared."""
    expected_keys = _normalized_source_keys(expected, context="预期 Fact 数据")
    actual_keys = _normalized_source_keys(actual, context="写入后 Fact 数据")
    expected_rows = expected.copy()
    actual_rows = actual.copy()
    expected_rows["__source_key"] = expected_keys
    actual_rows["__source_key"] = actual_keys
    expected_rows = expected_rows.set_index("__source_key")
    actual_rows = actual_rows.set_index("__source_key")
    if set(expected_rows.index) != set(actual_rows.index):
        missing = sorted(set(expected_rows.index) - set(actual_rows.index))[:5]
        extra = sorted(set(actual_rows.index) - set(expected_rows.index))[:5]
        raise RuntimeError(f"Fact 替换后 source_no 集合不一致：缺失={missing}，新增={extra}")

    for source_key in expected_rows.index:
        expected_id = int(expected_rows.at[source_key, "id"])
        actual_id = int(actual_rows.at[source_key, "id"])
        expected_link = expected_rows.at[source_key, "business_report_staging_id"]
        actual_link = actual_rows.at[source_key, "business_report_staging_id"]
        normalized_expected_link = None if pd.isna(expected_link) else str(expected_link)
        normalized_actual_link = None if pd.isna(actual_link) else str(actual_link)
        if expected_id != actual_id or normalized_expected_link != normalized_actual_link:
            raise RuntimeError(
                f"Fact 身份关联校验失败：source_no={source_key}，"
                f"id={actual_id}/{expected_id}，"
                f"business_report_staging_id={normalized_actual_link}/{normalized_expected_link}"
            )


def _validate_identifier(value: str) -> str:
    if not _SAFE_IDENTIFIER.fullmatch(str(value or "")):
        raise ValueError(f"不安全的数据库标识符：{value}")
    return value


def _read_fact_identity_rows(
    connection,
    table_name: str,
    table_date_column: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    safe_table = _validate_identifier(table_name)
    safe_date_column = _validate_identifier(table_date_column)
    return pd.read_sql_query(
        text(
            f"SELECT id, source_no, business_report_staging_id FROM {safe_table} "
            f"WHERE {safe_date_column} >= :start_date AND {safe_date_column} <= :end_date"
        ),
        connection,
        params={"start_date": start_date, "end_date": end_date},
    )


def replace_linked_fact_rows(
    table_name: str,
    table_date_column: str,
    filtered_df: pd.DataFrame,
    df_date_column: str,
    start_date: str,
    end_date: str,
) -> None:
    """Replace one Fact date range atomically without losing IDs or staging UUID links."""
    safe_table = _validate_identifier(table_name)
    safe_date_column = _validate_identifier(table_date_column)
    engine = create_engine(url_to_db())
    with engine.begin() as connection:
        existing = _read_fact_identity_rows(
            connection,
            table_name,
            table_date_column,
            start_date,
            end_date,
        )
        incoming_keys = _normalized_source_keys(filtered_df, context="待导入 Fact 数据")
        existing_keys = _normalized_source_keys(existing, context="现有 Fact 数据")
        new_count = int((~incoming_keys.isin(set(existing_keys))).sum())
        allocated_ids = []
        if new_count:
            sequence_name = connection.execute(
                text("SELECT pg_get_serial_sequence(:table_name, 'id')"),
                {"table_name": table_name},
            ).scalar_one_or_none()
            if not sequence_name:
                raise RuntimeError(f"{table_name}.id 未配置序列，无法为新记录分配主键")
            allocated_ids = [
                int(row[0])
                for row in connection.execute(
                    text(
                        "SELECT nextval(CAST(:sequence_name AS regclass)) "
                        "FROM generate_series(1, :new_count)"
                    ),
                    {"sequence_name": sequence_name, "new_count": new_count},
                )
            ]
        replacement = merge_fact_identity_columns(filtered_df, existing, allocated_ids)
        replacement[df_date_column] = pd.to_datetime(replacement[df_date_column]).dt.date
        connection.execute(
            text(
                f"DELETE FROM {safe_table} "
                f"WHERE {safe_date_column} >= :start_date AND {safe_date_column} <= :end_date"
            ),
            {"start_date": start_date, "end_date": end_date},
        )
        replacement.to_sql(table_name, con=connection, if_exists="append", index=False)
        actual = _read_fact_identity_rows(
            connection,
            table_name,
            table_date_column,
            start_date,
            end_date,
        )
        verify_fact_identity_rows(replacement[list(FACT_IDENTITY_COLUMNS)], actual)


def _check_data_exists(
    table_name: str, table_date_column: str, start_date: str, end_date: str
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
            result = connection.execute(query, {"start_date": start_date, "end_date": end_date})
            count = result.fetchone()[0]

            exists = count > 0
            if exists:
                print(f"{table_name}: 在 {start_date} 到 {end_date} 范围内已存在 {count} 条数据")
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
        dfs = read_and_map_excel(root_directory, combined_table_mapping, combined_column_mapping)
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
    replace_existing: bool = True,
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
        df = drop_unstored_columns(table_name, df.copy())
        # 先检查 DataFrame 中是否有数据
        if df.empty:
            print(f"警告: {table_name} 的 DataFrame 为空，跳过更新操作（避免删除历史数据）")
            return

        # 检查日期列是否存在
        if df_date_column not in df.columns:
            print(f"警告: {table_name} 的 DataFrame 中不存在日期列 '{df_date_column}'，跳过更新操作")
            return

        # 过滤出指定日期范围内的数据
        df[df_date_column] = pd.to_datetime(df[df_date_column])
        df[df_date_column] = df[df_date_column].dt.date
        filtered_df = df[
            (df[df_date_column] >= pd.to_datetime(start_date).date())
            & (df[df_date_column] <= pd.to_datetime(end_date).date())
        ]

        # 如果过滤后的数据为空，不执行删除操作，避免删除历史数据
        if filtered_df.empty:
            print(f"警告: {table_name} 在 {start_date} 到 {end_date} 范围内没有数据，跳过更新操作（避免删除历史数据）")
            return

        if replace_existing:
            print(f"✓ 更新 {table_name} 数据（替换模式）: {start_date} 到 {end_date}，共 {len(filtered_df)} 条数据")
            if table_name in LINKED_FACT_TABLES:
                replace_linked_fact_rows(
                    table_name,
                    table_date_column,
                    filtered_df,
                    df_date_column,
                    start_date,
                    end_date,
                )
            else:
                update_between_dates(
                    table_name,
                    table_date_column,
                    filtered_df,
                    df_date_column,
                    start_date,
                    end_date,
                )
        else:
            # 检查是否已存在数据
            exists = _check_data_exists(table_name, table_date_column, start_date, end_date)
            if exists:
                print(f"⊘ 跳过 {table_name} 数据更新（已存在数据，replace_existing=False）")
            else:
                print(
                    f"✓ 更新 {table_name} 数据（追加模式）: {start_date} 到 {end_date}，共 {len(filtered_df)} 条数据"
                )
                update_between_dates(
                    table_name, table_date_column, filtered_df, df_date_column, start_date, end_date
                )
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
    dfs: Dict[str, pd.DataFrame], start_date: str, end_date: str, replace_existing: bool = True
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
    table_name = "excel_finished_goods_in"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(table_name, "term", start_date, end_date)
                if exists:
                    print(f"跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "term", df, "term", start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, "term", df, "term", start_date, end_date, replace_existing
                )
                updated_count += 1

    # 2. 更新物料调整表（全表更新）
    table_name = "excel_finished_goods_adj"
    if table_name in dfs:
        df = dfs[table_name].copy()
        df["work_order_material_number"] = (
            df["bus_date"].dt.year.astype(str)
            + df["bus_date"].dt.month.astype(str)
            + "-"
            + df["work_order_number"]
            + "-"
            + df["product_code"]
        )
        df = (
            df.groupby(["work_order_material_number"])
            .agg({"amt": "sum", "quantity": "sum"})
            .reset_index()
        )
        df["weighted_unit_price"] = df["amt"] / df["quantity"]
        df = df[["work_order_material_number", "weighted_unit_price"]]

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
    replace_existing: bool = True,
    root_directory: Optional[str] = None,
) -> None:
    """
    更新研发数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
        root_directory: Excel文件根目录路径（用于读取产品细类）
    """
    print("=" * 60)
    print("开始更新研发数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    table_name = "excel_sub_prod"
    df_sub_prod = None

    if "excel_sub_prod" in dfs:
        df_sub_prod = dfs["excel_sub_prod"].copy()
        print(f"✓ 从dfs中找到 'excel_sub_prod' 数据")
    elif root_directory:
        file_path = os.path.join(root_directory, "2.研发数据", "产品细类.xlsx")
        if os.path.exists(file_path):
            print(f"✓ 直接从Excel文件读取: {file_path}")
            df_sub_prod = pd.read_excel(file_path, sheet_name="产品细类")
        else:
            print(f"⚠️  文件不存在: {file_path}")

    if df_sub_prod is not None and not df_sub_prod.empty:
        required_columns = ["encoding", "name", "product_sub_category"]
        if all(col in df_sub_prod.columns for col in required_columns):
            df_sub_prod = df_sub_prod[required_columns]
            df_sub_prod = df_sub_prod.dropna(subset=["encoding", "product_sub_category"])

            for col in df_sub_prod.columns:
                if df_sub_prod[col].dtype == "object":
                    df_sub_prod[col] = df_sub_prod[col].astype(str).str.strip()

            print(f"✓ 准备更新 {table_name}（全表更新），共 {len(df_sub_prod)} 条数据")
            db_url = url_to_db()
            engine = create_engine(db_url)

            with engine.connect() as connection:
                try:
                    connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                    connection.commit()
                    print("✓ 表数据已清空")

                    df_sub_prod.to_sql(table_name, connection, if_exists="append", index=False)
                    connection.commit()
                    print(f"✓ 新数据已导入: {len(df_sub_prod)} 条")
                    updated_count += 1
                except Exception as e:
                    connection.rollback()
                    print(f"❌ 更新 {table_name} 失败: {e}")
                    skipped_count += 1
        else:
            missing = [col for col in required_columns if col not in df_sub_prod.columns]
            print(f"⚠️  跳过 {table_name}（缺少列: {missing}）")
            skipped_count += 1
    else:
        print(f"⊘ 跳过 {table_name}（无数据）")
        skipped_count += 1

    # 1. 工时统计表（全表更新）
    table_name = "excel_labor_hours"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 执行逆透视操作
        df = pd.melt(
            df,
            id_vars=["proj_name", "product_sub_category", "year"],
            value_vars=[
                "1月",
                "2月",
                "3月",
                "4月",
                "5月",
                "6月",
                "7月",
                "8月",
                "9月",
                "10月",
                "11月",
                "12月",
            ],
            var_name="month",
            value_name="hours_worked",
        )
        df = df[df["hours_worked"].notna()]
        df = df[df["hours_worked"] > 0]
        df["year"] = df["year"].astype(float).astype(int).astype(str)
        df["month"] = df["month"].str.extract("(\d+)").astype(int)
        df["date"] = pd.to_datetime(df["year"] + "-" + df["month"].astype(str) + "-01")
        df = df[["proj_name", "product_sub_category", "hours_worked", "date"]]

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
        ("excel_sample_molds", "bus_date", "bus_date"),
        ("excel_tech_maintenance", "date", "date"),
        ("excel_dev_projects", "date", "date"),
        ("excel_material_usage_costs", "date", "date"),
    ]

    for table_name, table_date_column, df_date_column in tables_config:
        if table_name in dfs:
            df = drop_unstored_columns(table_name, dfs[table_name].copy())
            # 先检查 DataFrame 是否为空
            if df.empty:
                print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
                skipped_count += 1
                continue

            if not replace_existing:
                exists = _check_data_exists(table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name,
                table_date_column,
                df,
                df_date_column,
                start_date,
                end_date,
                replace_existing,
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
    dfs: Dict[str, pd.DataFrame], start_date: str, end_date: str, replace_existing: bool = True
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

    table_name = "excel_purchase_cost_red"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(table_name, "date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "date", df, "date", start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, "date", df, "date", start_date, end_date, replace_existing
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
    dfs: Dict[str, pd.DataFrame], start_date: str, end_date: str, replace_existing: bool = True
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

    table_name = "excel_inventory_turn"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(table_name, "date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "date", df, "date", start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, "date", df, "date", start_date, end_date, replace_existing
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
    dfs: Dict[str, pd.DataFrame], start_date: str, end_date: str, replace_existing: bool = True
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

    table_name = "excel_cost_control"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            # 处理数据
            df["submission_date"] = pd.to_datetime(df["submission_date"])
            df["year"] = df["submission_date"].dt.year
            df = df[
                [
                    "submission_date",
                    "description",
                    "budget_department_code",
                    "budget_department_name",
                    "document_number",
                    "submitter_code",
                    "submitter_name",
                    "year",
                ]
            ]

            if not replace_existing:
                exists = _check_data_exists(table_name, "submission_date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name,
                        "submission_date",
                        df,
                        "submission_date",
                        start_date,
                        end_date,
                        replace_existing,
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name,
                    "submission_date",
                    df,
                    "submission_date",
                    start_date,
                    end_date,
                    replace_existing,
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
    root_directory: Optional[str] = None,
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
        ("excel_price_inc_profits", "date", "date"),
        ("excel_esign_shipments", "month", "month"),
        ("excel_sales_stats", "date", "date"),
        ("excel_powerbank_fin", "month", "month"),
        ("excel_powerbank_ops", "month", "month"),
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
                exists = _check_data_exists(table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name,
                table_date_column,
                df,
                df_date_column,
                start_date,
                end_date,
                replace_existing,
            )
            updated_count += 1
        else:
            print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
            skipped_count += 1

    # 更新跨境业务数据（需要特殊处理）
    if root_directory:
        try:
            import os

            file_path = os.path.join(root_directory, "7.业务数据", "跨境", "周报取数模板.xlsx")
            if os.path.exists(file_path):
                df_user = (
                    pd.read_excel(file_path, sheet_name="用户数")
                    .drop(["周"], axis=1)
                    .groupby(["年", "月"])
                    .sum()
                    .reset_index()
                )
                df_amt = (
                    pd.read_excel(file_path, sheet_name="入账提现金额")
                    .loc[
                        :,
                        [
                            "年",
                            "月",
                            "入账笔数",
                            "入账成功总金额",
                            "电商入账笔数",
                            "电商入账成功总金额",
                        ],
                    ]
                    .groupby(["年", "月"])
                    .sum()
                    .reset_index()
                )
                df_curr = (
                    pd.read_excel(file_path, sheet_name="入账金额分币种")
                    .drop(["周", "入账成功总金额"], axis=1)
                    .groupby(["年", "月"])
                    .sum()
                    .reset_index()
                )
                df_qr = (
                    pd.read_excel(file_path, sheet_name="码付")
                    .drop(["周", "单笔均额-码付", "费率-码付"], axis=1)
                    .groupby(["年", "月"])
                    .sum()
                    .reset_index()
                )

                df = pd.merge(df_user, df_amt, on=["年", "月"], how="outer")
                df = pd.merge(df, df_curr, on=["年", "月"], how="outer")
                df = pd.merge(df, df_qr, on=["年", "月"], how="outer")

                if df["月"].isnull().any():
                    print("合并后的年月存在空值")

                df["日期"] = df["年"].astype(str) + "-" + df["月"].astype(str) + "-01"
                df = df.drop(["年", "月"], axis=1)
                df.columns = [combined_column_mapping.get(col, col) for col in df.columns]

                table_name = "excel_cross_border"
                if not replace_existing:
                    exists = _check_data_exists(table_name, "date", start_date, end_date)
                    if exists:
                        print(f"⊘ 跳过 {table_name}（已存在数据）")
                        skipped_count += 1
                    else:
                        update_data_by_date_range_task(
                            table_name, "date", df, "date", start_date, end_date, replace_existing
                        )
                        updated_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "date", df, "date", start_date, end_date, replace_existing
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
    dfs: Dict[str, pd.DataFrame], start_date: str, end_date: str, replace_existing: bool = True
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

    table_name = "fact_personnel"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(table_name, "date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "date", df, "date", start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, "date", df, "date", start_date, end_date, replace_existing
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
    replace_existing: bool = True,
    replace_exchange_rates: Optional[bool] = None,
    update_exchange_rates: bool = True,
) -> None:
    """
    更新手工刷新数据

    Args:
        dfs: 包含所有数据的字典
        start_date: 开始日期
        end_date: 结束日期
        replace_existing: 是否替换已存在的数据
        replace_exchange_rates: 汇率表是否替换已存数据。不指定时跟随 replace_existing
        update_exchange_rates: 是否允许从 Excel 更新汇率；日常流程默认关闭
    """
    print("=" * 60)
    print("开始更新手工刷新数据")
    print("=" * 60)

    updated_count = 0
    skipped_count = 0

    if replace_exchange_rates is None:
        replace_exchange_rates = replace_existing

    # 更新汇率表
    table_name = "excel_exchange_rates"
    if not update_exchange_rates:
        print(f"⊘ 跳过 {table_name}（汇率已改由金蝶基础数据流程按月更新）")
        skipped_count += 1
    elif table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_exchange_rates:
                exists = _check_data_exists(table_name, "effective_date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name,
                        "effective_date",
                        df,
                        "effective_date",
                        start_date,
                        end_date,
                        replace_exchange_rates,
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name,
                    "effective_date",
                    df,
                    "effective_date",
                    start_date,
                    end_date,
                    replace_exchange_rates,
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新利润表
    table_name = "fact_profit_stmt"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(table_name, "date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "date", df, "date", start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, "date", df, "date", start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新其他明细表
    #
    # 业报收集的六张原始 Fact 已停止通过 Prefect Excel 导入直接写库，
    # 后续由平台 CRUD/批量导入接口统一维护单据、填报人和变更记录：
    #   fact_revenue, fact_expense, fact_profit_bd,
    #   fact_receivable, fact_inventory, fact_inventory_on_way
    # 如需恢复，不应只取消下方注释，而应改为调用平台受控接口。
    print("⊘ 已停用业报收集 Fact 的 Prefect 直接导入：" "收入、费用、其他、应收、存货、在途将由平台统一维护")
    tables_config = [
        ("fact_receipt", "date", "date"),
        # ("fact_profit_bd", "date", "date"),
        # ("fact_expense", "acct_period", "acct_period"),
        # ("fact_inventory", "acct_period", "acct_period"),
        # ("fact_receivable", "acct_period", "acct_period"),
        # ("fact_revenue", "acct_period", "acct_period"),
        # ("fact_inventory_on_way", "acct_period", "acct_period"),
    ]

    for table_name, table_date_column, df_date_column in tables_config:
        if table_name in dfs:
            df = dfs[table_name].copy()
            # 对应收表的 sales_region 进行替换：俄联邦区 → ELB
            if table_name == "fact_receivable" and "sales_region" in df.columns:
                df.loc[df["sales_region"] == "俄联邦区", "sales_region"] = "ELB"
            # 先检查 DataFrame 是否为空
            if df.empty:
                print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
                skipped_count += 1
                continue

            if not replace_existing:
                exists = _check_data_exists(table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name,
                table_date_column,
                df,
                df_date_column,
                start_date,
                end_date,
                replace_existing,
            )
            updated_count += 1
        else:
            print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
            skipped_count += 1

    # 更新抵消表
    table_name = "fact_offset"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(table_name, "date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "date", df, "date", start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, "date", df, "date", start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新业务线工资比重
    table_name = "fact_bus_wage_rate"
    if table_name in dfs:
        df = dfs[table_name].copy()
        # 先检查 DataFrame 是否为空
        if df.empty:
            print(f"⊘ 跳过 {table_name}（DataFrame 为空，无数据需要更新）")
            skipped_count += 1
        else:
            if not replace_existing:
                exists = _check_data_exists(table_name, "date", start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                else:
                    update_data_by_date_range_task(
                        table_name, "date", df, "date", start_date, end_date, replace_existing
                    )
                    updated_count += 1
            else:
                update_data_by_date_range_task(
                    table_name, "date", df, "date", start_date, end_date, replace_existing
                )
                updated_count += 1
    else:
        print(f"⚠️  警告: {table_name} 不在数据字典中，跳过处理")
        skipped_count += 1

    # 更新现金流量表
    tables_config = [
        ("fact_cashflow", "date", "date"),
        ("excel_cashflow_intl", "date", "date"),
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
                exists = _check_data_exists(table_name, table_date_column, start_date, end_date)
                if exists:
                    print(f"⊘ 跳过 {table_name}（已存在数据）")
                    skipped_count += 1
                    continue

            update_data_by_date_range_task(
                table_name,
                table_date_column,
                df,
                df_date_column,
                start_date,
                end_date,
                replace_existing,
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
