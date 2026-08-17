"""Safe persistence helpers for business-line calculation results."""

import re
from typing import Optional

import pandas as pd
from mypackage.utilities import url_to_db
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

SOURCE_METADATA_COLUMNS = frozenset({"business_line_ratios"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def strip_source_metadata_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove source-only columns that do not belong in ``fact_bus_*`` tables."""
    columns_to_drop = SOURCE_METADATA_COLUMNS.intersection(df.columns)
    if not columns_to_drop:
        return df
    return df.drop(columns=sorted(columns_to_drop))


def _validate_identifier(value: str, label: str) -> None:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}")


def replace_date_range_data(
    table_name: str,
    date_column: str,
    df: pd.DataFrame,
    df_date_column: str,
    date_range: pd.DatetimeIndex,
    *,
    engine: Optional[Engine] = None,
) -> int:
    """Atomically replace one date range in a business-line result table.

    Source-only metadata is removed before validating the DataFrame against the
    existing target schema. The delete and insert share one SQLAlchemy
    transaction, so an insert failure restores the previous rows.
    """
    _validate_identifier(table_name, "table name")
    _validate_identifier(date_column, "date column")
    _validate_identifier(df_date_column, "DataFrame date column")

    if len(date_range) == 0:
        raise ValueError("date_range must not be empty")

    prepared = strip_source_metadata_columns(df).drop(columns=["id"], errors="ignore").copy()
    if df_date_column not in prepared.columns:
        raise ValueError(f"DataFrame is missing date column {df_date_column!r}")

    prepared[df_date_column] = pd.to_datetime(prepared[df_date_column])
    allowed_dates = {timestamp.date() for timestamp in pd.DatetimeIndex(date_range)}
    prepared = prepared[prepared[df_date_column].dt.date.isin(allowed_dates)]

    owns_engine = engine is None
    db_engine = engine or create_engine(url_to_db())
    min_date = pd.Timestamp(date_range.min()).to_pydatetime()
    max_date = pd.Timestamp(date_range.max()).to_pydatetime()

    try:
        with db_engine.begin() as connection:
            target_columns = {
                column["name"] for column in inspect(connection).get_columns(table_name)
            }
            unexpected_columns = sorted(set(prepared.columns).difference(target_columns))
            if unexpected_columns:
                raise ValueError(
                    f"{table_name} does not contain DataFrame columns: "
                    f"{', '.join(unexpected_columns)}"
                )

            connection.execute(
                text(
                    f"DELETE FROM {table_name} "
                    f"WHERE {date_column} >= :min_date AND {date_column} <= :max_date"
                ),
                {"min_date": min_date, "max_date": max_date},
            )
            prepared.to_sql(
                table_name,
                con=connection,
                if_exists="append",
                index=False,
                chunksize=10000,
            )
    finally:
        if owns_engine:
            db_engine.dispose()

    return len(prepared)
