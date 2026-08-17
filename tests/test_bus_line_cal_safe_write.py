import pandas as pd
import pytest
from pandas.errors import DatabaseError
from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
)

from modules.bus_line_cal.tasks.write_utils import (
    replace_date_range_data,
    strip_source_metadata_columns,
)


def _create_result_table(engine):
    metadata = MetaData()
    table = Table(
        "fact_bus_revenue",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("acct_period", DateTime, nullable=False),
        Column("source_no", String, nullable=False),
        Column("amount", Float, nullable=False),
        CheckConstraint("amount >= 0", name="ck_non_negative_amount"),
    )
    metadata.create_all(engine)
    return table


def test_strip_source_metadata_columns_removes_json_dict():
    source = pd.DataFrame(
        [
            {
                "source_no": "R1",
                "business_line_ratios": {"国际业务": 1},
                "business_report_staging_id": "staging-row-1",
            }
        ]
    )

    result = strip_source_metadata_columns(source)

    assert list(result.columns) == ["source_no"]
    assert "business_line_ratios" in source.columns
    assert "business_report_staging_id" in source.columns


def test_replace_date_range_data_drops_source_metadata_and_preserves_other_months(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'safe-write.db'}")
    table = _create_result_table(engine)
    with engine.begin() as connection:
        connection.execute(
            table.insert(),
            [
                {
                    "acct_period": pd.Timestamp("2026-06-01"),
                    "source_no": "OLD-JUNE",
                    "amount": 1,
                },
                {
                    "acct_period": pd.Timestamp("2026-07-01"),
                    "source_no": "KEEP-JULY",
                    "amount": 2,
                },
            ],
        )

    inserted = replace_date_range_data(
        "fact_bus_revenue",
        "acct_period",
        pd.DataFrame(
            [
                {
                    "id": 999,
                    "acct_period": pd.Timestamp("2026-06-30"),
                    "source_no": "NEW-JUNE",
                    "amount": 3,
                    "business_line_ratios": {},
                    "business_report_staging_id": "staging-row-1",
                }
            ]
        ),
        "acct_period",
        pd.date_range("2026-06-01", "2026-06-30"),
        engine=engine,
    )

    with engine.connect() as connection:
        rows = connection.execute(
            select(table.c.source_no, table.c.amount).order_by(table.c.source_no)
        ).all()

    assert inserted == 1
    assert rows == [("KEEP-JULY", 2.0), ("NEW-JUNE", 3.0)]


def test_replace_date_range_data_rolls_back_delete_when_insert_fails(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'rollback.db'}")
    table = _create_result_table(engine)
    with engine.begin() as connection:
        connection.execute(
            table.insert(),
            {
                "acct_period": pd.Timestamp("2026-06-01"),
                "source_no": "OLD-JUNE",
                "amount": 1,
            },
        )

    with pytest.raises(DatabaseError):
        replace_date_range_data(
            "fact_bus_revenue",
            "acct_period",
            pd.DataFrame(
                [
                    {
                        "acct_period": pd.Timestamp("2026-06-30"),
                        "source_no": "INVALID-JUNE",
                        "amount": -1,
                    }
                ]
            ),
            "acct_period",
            pd.date_range("2026-06-01", "2026-06-30"),
            engine=engine,
        )

    with engine.connect() as connection:
        rows = connection.execute(select(table.c.source_no, table.c.amount)).all()

    assert rows == [("OLD-JUNE", 1.0)]
