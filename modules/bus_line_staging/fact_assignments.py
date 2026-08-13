"""Authoritative business-line assignments stored on source fact rows."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd
from mypackage.utilities import connect_to_db
from prefect import task

RATIO_COLUMN_CANDIDATES = ("business_line_ratios", "业务线分摊比例")
DIRECT_COLUMN_CANDIDATES = (
    "bus_line",
    "业务线",
    "dist_bus_line",
    "分摊业务线",
    "business_line",
)
SOURCE_COLUMN_CANDIDATES = ("source_no", "来源编号")
INTERNAL_ASSIGNMENT_COLUMN = "__fact_assignment"
RATIO_TOLERANCE = 1e-6

FACT_TABLE_PERIOD_COLUMNS = {
    "fact_revenue": "acct_period",
    "fact_expense": "acct_period",
    "fact_profit_bd": "date",
    "fact_receivable": "acct_period",
    "fact_inventory": "acct_period",
    "fact_inventory_on_way": "acct_period",
}

RESTORE_SPECS = {
    "fact_revenue": {
        "staging_table": "staging_bus_revenue",
        "class_name": "收入",
        "period_column": "acct_period",
        "staging_period_column": "会计期间",
        "direct_column": None,
        "columns": {
            "来源编号": "source_no",
            "唯一层级": "unique_lvl",
            "一级组织": "prim_org",
            "二级组织": "sec_org",
            "三级组织": "third_org",
            "会计期间": "acct_period",
            "收入大类": "inc_major_cat",
            "产品大类": "prod_major_cat",
            "物料名称": "mat_name",
            "不含税金额本位币": "amt_tax_exc_loc",
            "成本金额": "cost_amt",
            "运费成本": "freight_cost",
            "关税成本": "tariff_cost",
            "软件成本": "soft_cost",
            "年份": "EXTRACT(YEAR FROM acct_period)::integer",
            "数据来源": "unique_lvl",
        },
    },
    "fact_expense": {
        "staging_table": "staging_bus_expense",
        "class_name": "费用",
        "period_column": "acct_period",
        "staging_period_column": "会计期间",
        "direct_column": "dist_bus_line",
        "columns": {
            "来源编号": "source_no",
            "唯一层级": "unique_lvl",
            "一级组织": "prim_org",
            "二级组织": "sec_org",
            "三级组织": "third_org",
            "单据编号": "doc_no",
            "报销人": "claimant",
            "摘要": "summary",
            "会计期间": "acct_period",
            "费用性质": "exp_nature",
            "费用大类": "exp_major_cat",
            "核算项目-费控": "acc_proj_cost",
            "研发项目": "rd_proj",
            "项目编码": "proj_code",
            "费用金额": "exp_amt",
            "年份": "EXTRACT(YEAR FROM acct_period)::integer",
            "来源层级": "unique_lvl",
            "数据来源": "'费用'",
            "分摊业务线": "dist_bus_line",
        },
    },
    "fact_profit_bd": {
        "staging_table": "staging_bus_profit_bd",
        "class_name": "其他",
        "period_column": "date",
        "staging_period_column": "日期",
        "direct_column": None,
        "columns": {
            "来源编号": "source_no",
            "唯一层级": "unique_lvl",
            "一级组织": "prim_org",
            "二级组织": "sec_org",
            "三级组织": "third_org",
            "财报合并": "fin_con",
            "日期": "date",
            "一级科目": "prim_subj",
            "本月金额": "mo_amt",
            "年份": "EXTRACT(YEAR FROM date)::integer",
            "数据来源": "unique_lvl",
        },
    },
    "fact_inventory": {
        "staging_table": "staging_bus_inventory",
        "class_name": "存货",
        "period_column": "acct_period",
        "staging_period_column": "会计期间",
        "direct_column": None,
        "columns": {
            "来源编号": "source_no",
            "唯一层级": "unique_lvl",
            "一级组织": "prim_org",
            "二级组织": "sec_org",
            "三级组织": "third_org",
            "财报合并": "fin_con",
            "财报单体": "fin_ind",
            "物料编码": "mat_code",
            "物料名称": "mat_name",
            "存货类别": "inv_cat",
            "客户类别": "cust_cat",
            "客户编码": "cust_code",
            "客户名称": "cust_name",
            "仓库": "warehouse",
            "是否为备货物料": "is_stock_mat",
            "数量(库存)": "qty_inv",
            "参考价(基本)": "ref_price_base",
            "参考金额": "ref_amt",
            "6个月以内数量": "qty_6m_less",
            "6个月以内金额": "amt_6m_less",
            "6-9个月数量": "qty_6_9m",
            "6-9个月金额": "amt_6_9m",
            "9个月-1年数量": "qty_9m_1y",
            "9个月-1年金额": "amt_9m_1y",
            "1-2年数量": "qty_1_2y",
            "1-2年金额": "amt_1_2y",
            "2-3年数量": "qty_2_3y",
            "2-3年金额": "amt_2_3y",
            "3年以上数量": "qty_3y_plus",
            "3年以上金额": "amt_3y_plus",
            "会计期间": "acct_period",
            "年份": "EXTRACT(YEAR FROM acct_period)::integer",
            "数据来源": "unique_lvl",
        },
    },
    "fact_receivable": {
        "staging_table": "staging_bus_receivable",
        "class_name": "应收",
        "period_column": "acct_period",
        "staging_period_column": "会计期间",
        "direct_column": None,
        "columns": {
            "来源编号": "source_no",
            "唯一层级": "unique_lvl",
            "一级组织": "prim_org",
            "二级组织": "sec_org",
            "三级组织": "third_org",
            "财报合并": "fin_con",
            "财报单体": "fin_ind",
            "一级科目": "prim_subj",
            "客户编码": "cust_code",
            "往来单位": "txn_unit",
            "往来性质": "txn_nature",
            "客户类型": "cust_type",
            "销售部门": "sales_dept",
            "销售区域": "sales_region",
            "赊销未核金额": "unaudited_sales_amt",
            "预收未核金额": "unaudited_prepay_amt",
            "分期未核金额": "unaudited_inst_amt",
            "应收账款余额": "ar_balance",
            "逾期金额": "ovd_amt",
            "未到期金额": "undue_amt",
            "逾期30天以内金额": "ovd_30d_less_amt",
            "逾期30天到90天金额": "ovd_30_90d_amt",
            "逾期90天到180天金额": "ovd_90_180d_amt",
            "逾期180天到360天金额": "ovd_180_360d_amt",
            "逾期360天以上金额": "ovd_360d_plus_amt",
            "账龄3个月以内": "acct_age_3m_less",
            "账龄3-6个月": "acct_age_3_6m",
            "账龄6-9个月": "acct_age_6_9m",
            "账龄9-12个月": "acct_age_9_12m",
            "账龄1-2年": "acct_age_1_2y",
            "账龄2-3年": "acct_age_2_3y",
            "账龄3年以上": "acct_age_3y_plus",
            "本年借方发生额": "yr_debit_occ",
            "本年贷方发生额": "yr_credit_occ",
            "销售模块": "sales_module",
            "上个月逾期金额": "last_mo_ovd_amt",
            "逾期变动": "ovd_change",
            "本年回款金额": "yr_repay_amt",
            "会计期间": "acct_period",
            "业务大类": "bus_major_cat",
            "业务小类": "bus_sub_cat",
            "应收状态": "ar_status",
            "备注": "remarks",
            "年份": "EXTRACT(YEAR FROM acct_period)::integer",
            "数据来源": "unique_lvl",
        },
    },
    "fact_inventory_on_way": {
        "staging_table": "staging_bus_in_transit_inventory",
        "class_name": "在途存货",
        "period_column": "acct_period",
        "staging_period_column": "会计期间",
        "direct_column": None,
        "columns": {
            "来源编号": "source_no",
            "唯一层级": "unique_lvl",
            "一级组织": "prim_org",
            "二级组织": "sec_org",
            "三级组织": "third_org",
            "财报合并": "fin_con",
            "财报单体": "fin_ind",
            "订单号": "order_number",
            "订单日期": "order_date",
            "供应商编码": "supplier_code",
            "供应商名称": "supplier_name",
            "存货类别": "inv_cat",
            "物料编码": "mat_code",
            "物料名称": "mat_name",
            "在途订单金额": "unreceived_inventory * unit_price * COALESCE(exchange_rate, 1)",
            "订单数量": "order_count",
            "未入库数量": "unreceived_inventory",
            "交货日期": "delivery_date",
            "会计期间": "acct_period",
            "年份": "EXTRACT(YEAR FROM acct_period)::integer",
            "数据来源": "unique_lvl",
        },
    },
}


class FactBusinessLineError(ValueError):
    """Raised when a fact row contains an unsafe business-line declaration."""


def _clean_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value).strip()


def _source_number(row: Mapping[str, Any]) -> str:
    for column in SOURCE_COLUMN_CANDIDATES:
        value = _clean_text(row.get(column))
        if value:
            return value
    return "<空来源编号>"


def _parse_ratio_object(raw: Any, *, table_name: str, source_no: str) -> dict[str, float]:
    if raw is None or raw == "":
        return {}
    if isinstance(raw, float) and math.isnan(raw):
        return {}
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as exc:
            raise FactBusinessLineError(
                f"{table_name} 来源编号 {source_no} 的 business_line_ratios 不是有效JSON对象：{exc.msg}"
            ) from exc
    if not isinstance(raw, Mapping):
        raise FactBusinessLineError(
            f"{table_name} 来源编号 {source_no} 的 business_line_ratios 必须是JSON对象"
        )

    ratios: dict[str, float] = {}
    for raw_line, raw_rate in raw.items():
        bus_line = _clean_text(raw_line)
        if not bus_line:
            raise FactBusinessLineError(
                f"{table_name} 来源编号 {source_no} 的 business_line_ratios 包含空业务线"
            )
        try:
            rate = float(raw_rate)
        except (TypeError, ValueError) as exc:
            raise FactBusinessLineError(
                f"{table_name} 来源编号 {source_no} 的业务线“{bus_line}”比例不是数字：{raw_rate!r}"
            ) from exc
        if not math.isfinite(rate) or rate <= 0 or rate > 1:
            raise FactBusinessLineError(
                f"{table_name} 来源编号 {source_no} 的业务线“{bus_line}”比例必须大于0且不超过1，当前为 {raw_rate!r}"
            )
        ratios[bus_line] = rate

    if ratios and not math.isclose(sum(ratios.values()), 1.0, abs_tol=RATIO_TOLERANCE):
        raise FactBusinessLineError(
            f"{table_name} 来源编号 {source_no} 的 business_line_ratios 合计为 "
            f"{sum(ratios.values()):.6f}，必须等于1"
        )
    return ratios


def _direct_business_line(row: Mapping[str, Any], *, table_name: str, source_no: str) -> str:
    values = {
        _clean_text(row.get(column))
        for column in DIRECT_COLUMN_CANDIDATES
        if _clean_text(row.get(column))
    }
    if len(values) > 1:
        raise FactBusinessLineError(
            f"{table_name} 来源编号 {source_no} 的单值业务线字段互相冲突：{', '.join(sorted(values))}"
        )
    return next(iter(values), "")


def resolve_fact_assignment(
    row: Mapping[str, Any],
    *,
    table_name: str,
    active_bus_lines: Sequence[str] | set[str],
) -> dict[str, float]:
    """Resolve one fact row using JSON first and a legacy single line second."""
    source_no = _source_number(row)
    raw_ratios = next(
        (row.get(column) for column in RATIO_COLUMN_CANDIDATES if column in row),
        None,
    )
    ratios = _parse_ratio_object(raw_ratios, table_name=table_name, source_no=source_no)
    direct_line = _direct_business_line(row, table_name=table_name, source_no=source_no)
    active_lines = {str(value).strip() for value in active_bus_lines}

    declared_lines = set(ratios)
    if direct_line:
        declared_lines.add(direct_line)
    invalid_lines = sorted(line for line in declared_lines if line not in active_lines)
    if invalid_lines:
        raise FactBusinessLineError(
            f"{table_name} 来源编号 {source_no} 使用了不存在、已停用或不可填报的业务线：" f"{', '.join(invalid_lines)}"
        )

    if ratios and direct_line:
        if len(ratios) != 1 or not math.isclose(
            ratios.get(direct_line, 0.0), 1.0, abs_tol=RATIO_TOLERANCE
        ):
            ratio_text = json.dumps(ratios, ensure_ascii=False, sort_keys=True)
            raise FactBusinessLineError(
                f"{table_name} 来源编号 {source_no} 的单值业务线“{direct_line}”与 "
                f"business_line_ratios={ratio_text} 冲突，请统一后重新抽取"
            )
        return ratios
    if ratios:
        return ratios
    if direct_line:
        return {direct_line: 1.0}
    return {}


def apply_fact_assignments(
    frame: pd.DataFrame,
    *,
    table_name: str,
    bus_lines: Sequence[str],
    active_bus_lines: Sequence[str] | set[str],
) -> pd.DataFrame:
    """Populate temporary wide ratio columns used by the existing Staging writer."""
    result = frame.copy()
    for line in bus_lines:
        if line not in result.columns:
            result[line] = pd.NA
    result[INTERNAL_ASSIGNMENT_COLUMN] = False
    for index, row in result.iterrows():
        ratios = resolve_fact_assignment(
            row.to_dict(), table_name=table_name, active_bus_lines=active_bus_lines
        )
        if not ratios:
            continue
        result.at[index, INTERNAL_ASSIGNMENT_COLUMN] = True
        for line, rate in ratios.items():
            result.at[index, line] = rate
    return result


def split_fact_assigned(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return authoritative and legacy rows without losing columns."""
    if frame.empty or INTERNAL_ASSIGNMENT_COLUMN not in frame.columns:
        return frame.iloc[0:0].copy(), frame.copy()
    mask = frame[INTERNAL_ASSIGNMENT_COLUMN].fillna(False).astype(bool)
    return frame.loc[mask].copy(), frame.loc[~mask].copy()


def get_active_fact_bus_lines(cur) -> list[str]:
    cur.execute(
        """
        SELECT bus_line
        FROM dim_bus_line
        WHERE status = '正常'
          AND bus_line IS NOT NULL
          AND BTRIM(bus_line) NOT IN ('', '集团', '无', '抵销数')
          AND COALESCE(bus_line_no, 0) <> 99
        ORDER BY bus_line_no NULLS LAST, bus_line
        """
    )
    return [str(row[0]).strip() for row in cur.fetchall()]


def legacy_fact_filter(table_alias: str = "") -> str:
    """SQL predicate selecting rows without an authoritative fact assignment."""
    prefix = f"{table_alias}." if table_alias else ""
    return f"COALESCE({prefix}business_line_ratios, '{{}}'::jsonb) = '{{}}'::jsonb"


def _fact_assignment_columns(cur, table_name: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    available = {row[0] for row in cur.fetchall()}
    if "business_line_ratios" not in available:
        raise FactBusinessLineError(f"{table_name} 缺少 business_line_ratios 字段，请先部署已批准的fact业务线比例迁移")
    return [
        column
        for column in (
            "source_no",
            "unique_lvl",
            "business_line_ratios",
            "bus_line",
            "dist_bus_line",
            "business_line",
        )
        if column in available
    ]


@task(name="校验fact业务线直接归属", log_prints=True)
def validate_fact_assignments_task(date_range) -> dict[str, int]:
    """Fail before Staging writes when any source fact assignment is unsafe."""
    conn, cur = connect_to_db()
    counts: dict[str, int] = {}
    try:
        active_lines = get_active_fact_bus_lines(cur)
        cur.execute("SELECT unique_lvl FROM dim_org_struc WHERE unique_lvl IS NOT NULL")
        known_org_levels = {str(row[0]).strip() for row in cur.fetchall()}
        date_values = list(date_range)
        for table_name, period_column in FACT_TABLE_PERIOD_COLUMNS.items():
            selected_columns = _fact_assignment_columns(cur, table_name)
            cur.execute(
                f"SELECT {', '.join(selected_columns)} FROM {table_name} "
                f"WHERE {period_column} = ANY(%s::date[])",
                (date_values,),
            )
            names = [description[0] for description in cur.description]
            assigned = 0
            for values in cur.fetchall():
                row = dict(zip(names, values))
                ratios = resolve_fact_assignment(
                    row,
                    table_name=table_name,
                    active_bus_lines=active_lines,
                )
                if ratios:
                    source_no = _source_number(row)
                    unique_lvl = _clean_text(row.get("unique_lvl"))
                    if not unique_lvl or unique_lvl not in known_org_levels or "无归属" in unique_lvl:
                        raise FactBusinessLineError(
                            f"{table_name} 来源编号 {source_no} 已注明业务线，但唯一层级“"
                            f"{unique_lvl or '<空>'}”无法进入业务线Staging；请先修正组织归属"
                        )
                assigned += bool(ratios)
            counts[table_name] = assigned
            print(f"{table_name}: 校验通过，发现 {assigned} 条fact直接归属记录。")
        conn.rollback()
        return counts
    finally:
        cur.close()
        conn.close()


def _explicit_assignment_filter(spec: Mapping[str, Any], fact_alias: str = "fact") -> str:
    json_filter = f"COALESCE({fact_alias}.business_line_ratios, '{{}}'::jsonb) <> '{{}}'::jsonb"
    direct_column = spec.get("direct_column")
    if direct_column:
        return f"({json_filter} OR NULLIF(BTRIM({fact_alias}.{direct_column}), '') IS NOT NULL)"
    return json_filter


def _qualified_restore_expression(expression: str) -> str:
    if not (
        expression.startswith("'") or "(" in expression or "::" in expression or "*" in expression
    ):
        return f"fact.{expression}"
    qualified = expression
    replacements = {
        "unreceived_inventory": "fact.unreceived_inventory",
        "unit_price": "fact.unit_price",
        "exchange_rate": "fact.exchange_rate",
        "acct_period": "fact.acct_period",
        "EXTRACT(YEAR FROM date)": "EXTRACT(YEAR FROM fact.date)",
    }
    for original, replacement in replacements.items():
        qualified = qualified.replace(original, replacement)
    return qualified


def build_restore_insert_sql(fact_table: str, spec: Mapping[str, Any]) -> str:
    staging_table = spec["staging_table"]
    period_column = spec["period_column"]
    staging_columns = ["batch_id", "record_id", *spec["columns"].keys(), "审核状态"]
    source_expressions = [
        "%s",
        "gen_random_uuid()",
        *[_qualified_restore_expression(value) for value in spec["columns"].values()],
        "'PENDING'",
    ]
    quoted_columns = ", ".join(f'"{column}"' for column in staging_columns)
    return f"""
        INSERT INTO {staging_table} ({quoted_columns})
        SELECT {', '.join(source_expressions)}
        FROM {fact_table} AS fact
        WHERE fact.{period_column} = ANY(%s::date[])
          AND {_explicit_assignment_filter(spec)}
          AND fact.unique_lvl IS NOT NULL
          AND fact.unique_lvl NOT LIKE '%%无归属%%'
          AND NOT EXISTS (
              SELECT 1 FROM {staging_table} AS staging
              WHERE staging.batch_id = %s
                AND staging."来源编号" = fact.source_no
                AND staging."唯一层级" = fact.unique_lvl
                AND staging."{spec['staging_period_column']}" = fact.{period_column}
          )
    """


@task(name="还原fact业务线到Staging", log_prints=True)
def restore_fact_assignments_task(date_range, batch_id) -> dict[str, int]:
    """Insert authoritative fact rows and their normalized ratios into one Staging batch."""
    conn, cur = connect_to_db()
    restored: dict[str, int] = {}
    try:
        date_values = list(date_range)
        for fact_table, spec in RESTORE_SPECS.items():
            staging_table = spec["staging_table"]
            period_column = spec["period_column"]
            cur.execute(
                build_restore_insert_sql(fact_table, spec),
                (batch_id, date_values, batch_id),
            )
            restored[fact_table] = cur.rowcount

            direct_column = spec.get("direct_column")
            fallback_expression = (
                f"jsonb_build_object(fact.{direct_column}, 1)" if direct_column else "'{}'::jsonb"
            )
            cur.execute(
                f"""
                INSERT INTO staging_bus_line_ratio(class, record_id, bus_line, rate)
                SELECT %s, staging.record_id, ratio.key, ratio.value::numeric
                FROM {staging_table} AS staging
                JOIN {fact_table} AS fact
                  ON fact.source_no = staging."来源编号"
                 AND fact.unique_lvl = staging."唯一层级"
                 AND fact.{period_column} = staging."{spec['staging_period_column']}"
                CROSS JOIN LATERAL jsonb_each_text(
                    CASE
                        WHEN COALESCE(fact.business_line_ratios, '{{}}'::jsonb) <> '{{}}'::jsonb
                            THEN fact.business_line_ratios
                        ELSE {fallback_expression}
                    END
                ) AS ratio
                WHERE staging.batch_id = %s
                  AND {_explicit_assignment_filter(spec)}
                ON CONFLICT (class, record_id, bus_line) DO UPDATE
                SET rate = EXCLUDED.rate, updated_at = CURRENT_TIMESTAMP
                """,
                (spec["class_name"], batch_id),
            )
            print(f"{fact_table}: 还原 {restored[fact_table]} 条Staging基础记录。")
        conn.commit()
        return restored
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
