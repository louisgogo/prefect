import pandas as pd
import pytest

from modules.bus_line_staging.fact_assignments import (
    INTERNAL_ASSIGNMENT_COLUMN,
    RESTORE_SPECS,
    FactBusinessLineError,
    apply_fact_assignments,
    build_restore_insert_sql,
    legacy_fact_filter,
    resolve_fact_assignment,
    split_fact_assigned,
)

ACTIVE_LINES = {"国际业务", "国内硬件", "小POS"}


def test_json_fact_assignment_is_authoritative():
    assert resolve_fact_assignment(
        {
            "source_no": "R100",
            "business_line_ratios": {"国际业务": 0.4, "国内硬件": 0.6},
        },
        table_name="fact_revenue",
        active_bus_lines=ACTIVE_LINES,
    ) == {"国际业务": 0.4, "国内硬件": 0.6}


def test_legacy_single_line_becomes_full_assignment():
    assert resolve_fact_assignment(
        {
            "source_no": "E100",
            "business_line_ratios": {},
            "dist_bus_line": "小POS",
        },
        table_name="fact_expense",
        active_bus_lines=ACTIVE_LINES,
    ) == {"小POS": 1.0}


def test_equivalent_json_and_single_line_are_allowed():
    assert resolve_fact_assignment(
        {
            "source_no": "E101",
            "business_line_ratios": {"国际业务": 1},
            "dist_bus_line": "国际业务",
        },
        table_name="fact_expense",
        active_bus_lines=ACTIVE_LINES,
    ) == {"国际业务": 1.0}


def test_conflicting_json_and_single_line_raise_actionable_error():
    with pytest.raises(FactBusinessLineError) as exc_info:
        resolve_fact_assignment(
            {
                "source_no": "E-CONFLICT",
                "business_line_ratios": {"国内硬件": 1},
                "dist_bus_line": "国际业务",
            },
            table_name="fact_expense",
            active_bus_lines=ACTIVE_LINES,
        )

    message = str(exc_info.value)
    assert "fact_expense" in message
    assert "E-CONFLICT" in message
    assert "国际业务" in message
    assert "国内硬件" in message
    assert "冲突" in message


@pytest.mark.parametrize(
    "ratios, expected",
    [
        ({"已停用业务线": 1}, "不存在、已停用或不可填报"),
        ({"国际业务": 0.8}, "合计为 0.800000"),
        ({"国际业务": 0}, "必须大于0且不超过1"),
    ],
)
def test_invalid_fact_json_assignments_fail(ratios, expected):
    with pytest.raises(FactBusinessLineError, match=expected):
        resolve_fact_assignment(
            {"source_no": "BAD-1", "business_line_ratios": ratios},
            table_name="fact_revenue",
            active_bus_lines=ACTIVE_LINES,
        )


def test_apply_and_split_fact_assignments_preserve_legacy_rows():
    source = pd.DataFrame(
        [
            {
                "来源编号": "R1",
                "业务线分摊比例": {"国际业务": 1},
            },
            {
                "来源编号": "R2",
                "业务线分摊比例": {},
            },
        ]
    )

    result = apply_fact_assignments(
        source,
        table_name="fact_revenue",
        bus_lines=["国际业务", "国内硬件"],
        active_bus_lines=ACTIVE_LINES,
    )
    assigned, legacy = split_fact_assigned(result)

    assert assigned["来源编号"].tolist() == ["R1"]
    assert assigned["国际业务"].tolist() == [1.0]
    assert legacy["来源编号"].tolist() == ["R2"]
    assert not bool(legacy.iloc[0][INTERNAL_ASSIGNMENT_COLUMN])


def test_legacy_sql_filter_targets_empty_json_only():
    assert legacy_fact_filter("fact") == (
        "COALESCE(fact.business_line_ratios, '{}'::jsonb) = '{}'::jsonb"
    )


def test_restore_sql_qualifies_derived_year_and_in_transit_amount():
    revenue_sql = build_restore_insert_sql("fact_revenue", RESTORE_SPECS["fact_revenue"])
    transit_sql = build_restore_insert_sql(
        "fact_inventory_on_way", RESTORE_SPECS["fact_inventory_on_way"]
    )

    assert "EXTRACT(YEAR FROM fact.acct_period)::integer" in revenue_sql
    assert (
        "fact.unreceived_inventory * fact.unit_price * COALESCE(fact.exchange_rate, 1)"
        in transit_sql
    )
    assert "COALESCE(fact.business_line_ratios, '{}'::jsonb) <> '{}'::jsonb" in transit_sql
