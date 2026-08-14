"""季度存货跌价计算及业报数据校验 Tasks。"""

import hashlib
import os
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests
from mypackage.utilities import connect_to_db
from prefect import task

AGING_AMOUNT_COLUMNS = [
    "amt_6m_less",
    "amt_6_9m",
    "amt_9m_1y",
    "amt_1_2y",
    "amt_2_3y",
    "amt_3y_plus",
]
AGING_QUANTITY_COLUMNS = [
    "qty_6m_less",
    "qty_6_9m",
    "qty_9m_1y",
    "qty_1_2y",
    "qty_2_3y",
    "qty_3y_plus",
]
SPECIAL_WAREHOUSES = {"不良品仓", "待处理仓", "待加工仓库", "待退仓", "客退品仓"}

INVENTORY_COLUMNS = [
    "fin_con",
    "fin_ind",
    "inv_cat",
    "cust_cat",
    "warehouse",
    "ref_amt",
    *AGING_QUANTITY_COLUMNS,
    *AGING_AMOUNT_COLUMNS,
    "acct_period",
]

IN_TRANSIT_COLUMNS = [
    "fin_con",
    "fin_ind",
    "inv_cat",
    "order_date",
    "delivery_date",
    "order_count",
    "unit_price",
    "exchange_rate",
    "unreceived_inventory",
    "acct_period",
]

CALCULATION_KEY_COLUMNS = ["fin_ind", "unique_lvl", "prim_org", "sec_org", "third_org"]
FACT_PROFIT_BD_COLUMNS = [
    "fin_con",
    "fin_ind",
    "prim_org",
    "sec_org",
    "third_org",
    "prim_subj",
    "mo_amt",
    "date",
    "unique_lvl",
    "year",
    "remarks",
    "source_no",
]
PLATFORM_BASE_URL_ENV = "AIHUB_PLATFORM_BASE_URL"
PLATFORM_TOKEN_ENV = "AIHUB_PLATFORM_API_TOKEN"
PLATFORM_TOKEN_FALLBACK_ENV = "XGD_TOKEN"
DEFAULT_PLATFORM_BASE_URL = "http://127.0.0.1:8001/api/v1"
PLATFORM_SYNC_PATH = "/data-collect/business-report/inventory-impairment-sync"
PLATFORM_SYNC_TIMEOUT_SECONDS = 120


def get_default_inventory_impairment_period(
    reference_date: Optional[date] = None,
) -> Tuple[int, int]:
    """返回参考日期之前最近一个已结束季度。"""
    reference = pd.Timestamp(reference_date or date.today()).normalize()
    current_quarter_start_month = ((reference.month - 1) // 3) * 3 + 1
    current_quarter_start = pd.Timestamp(
        year=reference.year, month=current_quarter_start_month, day=1
    )
    previous_quarter_period = current_quarter_start - pd.DateOffset(months=1)
    previous_quarter = (previous_quarter_period.month - 1) // 3 + 1
    return previous_quarter_period.year, previous_quarter


def resolve_inventory_impairment_period(
    year: Optional[int],
    quarter: Optional[int],
    reference_date: Optional[date] = None,
) -> Tuple[int, int]:
    """解析显式期间；两者都省略时使用最近已结束季度。"""
    if year is None and quarter is None:
        return get_default_inventory_impairment_period(reference_date)
    if year is None or quarter is None:
        raise ValueError("year 和 quarter 必须同时提供，或同时留空使用最近已结束季度")
    get_quarter_periods(year, quarter)
    return year, quarter


def get_quarter_periods(year: int, quarter: int) -> Tuple[List[pd.Timestamp], pd.Timestamp]:
    """返回上季度末及目标季度三个月的月初日期，以及季度标记日期。"""
    if isinstance(year, bool) or not isinstance(year, int) or year < 2000:
        raise ValueError("year 必须是大于等于 2000 的整数")
    if isinstance(quarter, bool) or not isinstance(quarter, int) or quarter not in range(1, 5):
        raise ValueError("quarter 必须是 1 到 4 的整数")

    first_month = (quarter - 1) * 3 + 1
    first_period = pd.Timestamp(year=year, month=first_month, day=1)
    periods = [first_period - pd.DateOffset(months=1)]
    periods.extend(first_period + pd.DateOffset(months=offset) for offset in range(3))
    quarter_period = pd.Timestamp(year=year, month=quarter * 3, day=1)
    return periods, quarter_period


def _require_columns(df: pd.DataFrame, required: Sequence[str], source_name: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"{source_name} 缺少必要字段: {', '.join(missing)}")


def _round_half_up(value: float) -> float:
    if pd.isna(value):
        return np.nan
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _split_unique_level(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    split_levels = result["unique_lvl"].str.split("-", n=2, expand=True)
    result["prim_org"] = split_levels[0]
    result["sec_org"] = split_levels[1]
    result["third_org"] = split_levels[2]
    return result


def _classify_warehouse(df: pd.DataFrame) -> pd.Series:
    warehouse = df["warehouse"].fillna("").astype(str)
    fin_con = df["fin_con"].fillna("").astype(str)
    fin_ind = df["fin_ind"].fillna("").astype(str)
    is_jialian = fin_con.eq("嘉联")

    return pd.Series(
        np.select(
            [
                warehouse.eq("在途仓库"),
                is_jialian & warehouse.str.contains("澳门", regex=False),
                is_jialian & fin_ind.str.contains("分公司", regex=False),
                is_jialian & df["warehouse"].isna(),
                is_jialian & warehouse.str.contains("分公司", regex=False),
            ],
            ["在途仓库", "集团仓库", "分公司仓库", "集团仓库", "分公司仓库"],
            default="集团仓库",
        ),
        index=df.index,
        dtype="object",
    )


def calculate_impairment_detail(df: pd.DataFrame) -> pd.DataFrame:
    """按 Power BI 规则计算每条存货记录的跌价准备余额。"""
    required = ["fin_con", "fin_ind", "inv_cat", "cust_cat", "warehouse", *AGING_AMOUNT_COLUMNS]
    _require_columns(df, required, "存货跌价明细")

    result = df.copy()
    result["fin_con"] = result["fin_con"].fillna("").astype(str)
    result["fin_ind"] = result["fin_ind"].fillna("").astype(str)
    result["inv_cat"] = result["inv_cat"].fillna("").astype(str)
    result["cust_cat"] = result["cust_cat"].fillna("").astype(str)
    for column in AGING_AMOUNT_COLUMNS:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)

    result["warehouse_class"] = _classify_warehouse(result)
    is_zhongzheng = result["fin_con"].eq("中正")
    is_outbound = result["inv_cat"].eq("发出商品")
    is_bank = result["cust_cat"].eq("银行")
    is_special_warehouse = result["warehouse"].isin(SPECIAL_WAREHOUSES)

    within_one_year = np.select(
        [
            is_zhongzheng,
            is_outbound & ~is_bank,
            is_outbound & is_bank,
            is_special_warehouse,
        ],
        [
            0.0,
            result["amt_6_9m"] * 0.5 + result["amt_9m_1y"] * 0.5,
            0.0,
            result["amt_6m_less"] + result["amt_6_9m"] + result["amt_9m_1y"],
        ],
        default=result["amt_6_9m"] * 0.3 + result["amt_9m_1y"] * 0.5,
    )
    over_one_year = np.select(
        [is_zhongzheng, is_outbound & is_bank],
        [
            0.0,
            result["amt_1_2y"] * 0.5 + result["amt_2_3y"] + result["amt_3y_plus"],
        ],
        default=result["amt_1_2y"] + result["amt_2_3y"] + result["amt_3y_plus"],
    )

    result["impairment_within_one_year"] = within_one_year.astype(float)
    result["impairment_over_one_year"] = over_one_year.astype(float)
    result["impairment_balance"] = (
        result["impairment_within_one_year"] + result["impairment_over_one_year"]
    )
    result["inventory_status"] = np.where(result["warehouse_class"].eq("在途仓库"), "在途", "在库")
    return result


def prepare_inventory_detail(df_inventory: pd.DataFrame) -> pd.DataFrame:
    """规范存货表字段并计算跌价准备。"""
    _require_columns(df_inventory, INVENTORY_COLUMNS, "fact_inventory")
    result = df_inventory.copy()
    result["acct_period"] = pd.to_datetime(result["acct_period"], errors="raise").dt.normalize()
    result["source_kind"] = "inventory"
    return calculate_impairment_detail(result)


def prepare_in_transit_detail(df_in_transit: pd.DataFrame) -> pd.DataFrame:
    """将在途存货按交货日期优先的账龄转换成跌价计算明细。"""
    _require_columns(df_in_transit, IN_TRANSIT_COLUMNS, "fact_inventory_on_way")
    result = df_in_transit.copy()
    result["acct_period"] = pd.to_datetime(result["acct_period"], errors="raise").dt.normalize()
    result["order_date"] = pd.to_datetime(result["order_date"], errors="coerce").dt.normalize()
    result["delivery_date"] = pd.to_datetime(
        result["delivery_date"], errors="coerce"
    ).dt.normalize()
    result["aging_date"] = result["delivery_date"].fillna(result["order_date"])
    result["aging_date_source"] = np.select(
        [result["delivery_date"].notna(), result["order_date"].notna()],
        ["delivery_date", "order_date"],
        default="missing",
    )

    for column in ["order_count", "unit_price", "unreceived_inventory"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result["exchange_rate"] = pd.to_numeric(result["exchange_rate"], errors="coerce").fillna(1.0)
    result["transit_amount"] = (
        result["unreceived_inventory"].fillna(0.0)
        * result["unit_price"].fillna(0.0)
        * result["exchange_rate"]
    )

    for column in AGING_AMOUNT_COLUMNS + AGING_QUANTITY_COLUMNS:
        result[column] = 0.0

    # Power BI 规则优先使用交货日期，交货日期为空时回退订单日期；账龄按
    # 30 天/月、360 天/年划分，而不是按自然月偏移。
    age_days = (result["acct_period"] - result["aging_date"]).dt.days
    has_aging_date = result["aging_date"].notna()

    bucket_conditions = [
        has_aging_date & age_days.lt(180),
        has_aging_date & age_days.ge(180) & age_days.lt(270),
        has_aging_date & age_days.ge(270) & age_days.lt(360),
        has_aging_date & age_days.ge(360) & age_days.lt(720),
        has_aging_date & age_days.ge(720) & age_days.lt(1080),
        has_aging_date & age_days.ge(1080),
    ]
    for condition, amount_column, quantity_column in zip(
        bucket_conditions, AGING_AMOUNT_COLUMNS, AGING_QUANTITY_COLUMNS
    ):
        result.loc[condition, amount_column] = result.loc[condition, "transit_amount"]
        result.loc[condition, quantity_column] = result.loc[
            condition, "unreceived_inventory"
        ].fillna(0.0)

    result["cust_cat"] = ""
    result["warehouse"] = "在途仓库"
    result["ref_amt"] = result["transit_amount"]
    result["source_kind"] = "in_transit"
    return calculate_impairment_detail(result)


def calculate_monthly_and_quarterly_impairment(
    df_inventory: pd.DataFrame,
    df_in_transit: pd.DataFrame,
    periods: Sequence[pd.Timestamp],
    quarter_period: pd.Timestamp,
) -> Dict[str, pd.DataFrame]:
    """计算月度余额变动和季度主体跌价金额。"""
    normalized_periods = [pd.Timestamp(period).normalize() for period in periods]
    if len(normalized_periods) != 4:
        raise ValueError("季度计算必须包含上季度末和本季度三个月，共 4 个期间")

    inventory_detail = prepare_inventory_detail(df_inventory)
    in_transit_detail = prepare_in_transit_detail(df_in_transit)
    detail = pd.concat([inventory_detail, in_transit_detail], ignore_index=True, sort=False)
    unexpected_periods = sorted(set(detail["acct_period"]) - set(normalized_periods))
    if unexpected_periods:
        formatted = ", ".join(
            pd.Timestamp(period).strftime("%Y-%m-%d") for period in unexpected_periods
        )
        raise ValueError(f"源数据包含计划外会计期间: {formatted}")

    detail["unique_lvl"] = np.select(
        [
            detail["warehouse_class"].eq("分公司仓库"),
            detail["fin_con"].isin(["嘉联", "嘉嘉电"]),
        ],
        [
            "国内渠道事业群-国内分公司-分公司运营支持部",
            "智造事业群-收单供应中心-公共部门",
        ],
        default="智造事业群-智造管理中心-公共部门",
    )

    balances_by_source = (
        detail.groupby(["acct_period", "fin_con", "unique_lvl", "source_kind"], dropna=False)[
            "impairment_balance"
        ]
        .sum()
        .unstack("source_kind", fill_value=0.0)
        .reset_index()
    )
    for source_kind in ["inventory", "in_transit"]:
        if source_kind not in balances_by_source.columns:
            balances_by_source[source_kind] = 0.0
    balances_by_source = balances_by_source.rename(
        columns={
            "fin_con": "fin_ind",
            "inventory": "inventory_balance",
            "in_transit": "in_transit_balance",
        }
    )

    entity_keys = balances_by_source[["fin_ind", "unique_lvl"]].drop_duplicates()
    period_frame = pd.DataFrame({"acct_period": normalized_periods})
    entity_keys["_join_key"] = 1
    period_frame["_join_key"] = 1
    balance_grid = entity_keys.merge(period_frame, on="_join_key").drop(columns="_join_key")
    balance_grid = balance_grid.merge(
        balances_by_source,
        on=["acct_period", "fin_ind", "unique_lvl"],
        how="left",
    )
    for column in ["inventory_balance", "in_transit_balance"]:
        balance_grid[column] = balance_grid[column].fillna(0.0).astype(float)
    balance_grid["current_balance"] = (
        balance_grid["inventory_balance"] + balance_grid["in_transit_balance"]
    )
    balance_grid = balance_grid.sort_values(CALCULATION_KEY_COLUMNS[:2] + ["acct_period"])

    for column in ["inventory_balance", "in_transit_balance", "current_balance"]:
        balance_grid[f"previous_{column}"] = balance_grid.groupby(
            ["fin_ind", "unique_lvl"], dropna=False
        )[column].shift(1)

    monthly = balance_grid[balance_grid["acct_period"].isin(normalized_periods[1:])].copy()
    monthly["inventory_movement"] = (
        monthly["previous_inventory_balance"] - monthly["inventory_balance"]
    )
    monthly["in_transit_movement"] = (
        monthly["previous_in_transit_balance"] - monthly["in_transit_balance"]
    )
    monthly["mo_amt_raw"] = monthly["previous_current_balance"] - monthly["current_balance"]
    monthly["mo_amt"] = monthly["mo_amt_raw"].map(_round_half_up)
    monthly["quarter_period"] = pd.Timestamp(quarter_period).normalize()
    monthly = _split_unique_level(monthly)

    quarter = (
        monthly.groupby(CALCULATION_KEY_COLUMNS, dropna=False)
        .agg(
            quarter_impairment_amount=("mo_amt", "sum"),
            inventory_movement=("inventory_movement", "sum"),
            in_transit_movement=("in_transit_movement", "sum"),
            nonzero_month_count=("mo_amt", lambda values: int(values.ne(0).sum())),
        )
        .reset_index()
    )
    quarter = quarter[quarter["nonzero_month_count"] > 0].copy()
    endpoint_balances = balance_grid[
        balance_grid["acct_period"].isin(normalized_periods[::3])
    ].copy()
    endpoint_balances["balance_label"] = np.where(
        endpoint_balances["acct_period"].eq(normalized_periods[0]), "prior", "current"
    )
    endpoint_balances = endpoint_balances.pivot(
        index=["fin_ind", "unique_lvl"], columns="balance_label", values="current_balance"
    ).reset_index()
    endpoint_balances = endpoint_balances.rename(
        columns={"prior": "prior_quarter_balance", "current": "quarter_end_balance"}
    )
    quarter = quarter.merge(endpoint_balances, on=["fin_ind", "unique_lvl"], how="left")
    quarter["quarter_period"] = pd.Timestamp(quarter_period).normalize()
    quarter["fin_con"] = "业报调整"
    quarter["prim_subj"] = "资产减值损失"
    quarter["year"] = str(pd.Timestamp(quarter_period).year)
    quarter["quarter_impairment_amount"] = quarter["quarter_impairment_amount"].map(_round_half_up)

    return {
        "detail": detail,
        "monthly": monthly.reset_index(drop=True),
        "quarterly": quarter.reset_index(drop=True),
    }


def reconcile_quarterly_impairment(
    calculated: pd.DataFrame, recorded: pd.DataFrame, tolerance: float = 0.01
) -> pd.DataFrame:
    """按主体和唯一层级核对计算结果与 fact_profit_bd。"""
    _require_columns(
        calculated,
        ["fin_ind", "unique_lvl", "quarter_impairment_amount"],
        "季度跌价计算结果",
    )
    _require_columns(recorded, ["fin_ind", "unique_lvl", "mo_amt"], "fact_profit_bd 校验数据")

    actual = recorded.copy()
    actual["mo_amt"] = pd.to_numeric(actual["mo_amt"], errors="coerce").fillna(0.0)
    actual = (
        actual.groupby(["fin_ind", "unique_lvl"], dropna=False)
        .agg(recorded_amount=("mo_amt", "sum"), recorded_rows=("mo_amt", "size"))
        .reset_index()
    )
    actual["recorded_amount"] = actual["recorded_amount"].map(_round_half_up)

    comparison = calculated[["fin_ind", "unique_lvl", "quarter_impairment_amount"]].merge(
        actual, on=["fin_ind", "unique_lvl"], how="outer", indicator=True
    )
    comparison["difference"] = (
        comparison["quarter_impairment_amount"] - comparison["recorded_amount"]
    )
    comparison["difference"] = comparison["difference"].map(_round_half_up)
    comparison["status"] = np.select(
        [
            comparison["_merge"].eq("left_only"),
            comparison["_merge"].eq("right_only"),
            comparison["difference"].abs().le(tolerance),
        ],
        ["missing_recorded", "missing_calculated", "matched"],
        default="different",
    )
    return (
        comparison.drop(columns="_merge")
        .sort_values(["fin_ind", "unique_lvl"])
        .reset_index(drop=True)
    )


def prepare_fact_profit_bd_rows(
    quarterly: pd.DataFrame, quarter_period: pd.Timestamp
) -> pd.DataFrame:
    """将季度计算结果转换为 fact_profit_bd 待写入行。"""
    required = [*CALCULATION_KEY_COLUMNS, "quarter_impairment_amount"]
    _require_columns(quarterly, required, "季度跌价计算结果")
    if quarterly.empty:
        raise ValueError("季度跌价计算结果为空，拒绝替换 fact_profit_bd")

    result = quarterly[required].copy()
    duplicate_keys = result.duplicated(["fin_ind", "unique_lvl"], keep=False)
    if duplicate_keys.any():
        duplicates = result.loc[duplicate_keys, ["fin_ind", "unique_lvl"]].drop_duplicates()
        raise ValueError(f"季度跌价结果存在重复主体层级: {duplicates.to_dict('records')}")

    result["mo_amt"] = pd.to_numeric(result.pop("quarter_impairment_amount"), errors="raise").map(
        _round_half_up
    )
    if result["mo_amt"].isna().any() or np.isinf(result["mo_amt"]).any():
        raise ValueError("季度跌价金额包含空值或无穷值")

    normalized_quarter_period = pd.Timestamp(quarter_period).normalize()
    quarter = (normalized_quarter_period.month - 1) // 3 + 1
    remarks = f"存货跌价自动计算-{normalized_quarter_period.year}Q{quarter}"
    result["fin_con"] = "业报调整"
    result["prim_subj"] = "资产减值损失"
    result["date"] = normalized_quarter_period.date()
    result["year"] = str(normalized_quarter_period.year)
    result["remarks"] = remarks

    def build_source_no(row: pd.Series) -> str:
        identity = f"{normalized_quarter_period:%Y-%m-%d}|{row['fin_ind']}|{row['unique_lvl']}"
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
        return f"INVIMP-{normalized_quarter_period:%Y%m}-{digest}"

    result["source_no"] = result.apply(build_source_no, axis=1)
    if result["source_no"].duplicated().any():
        raise ValueError("生成的 fact_profit_bd source_no 存在重复")

    return (
        result[FACT_PROFIT_BD_COLUMNS].sort_values(["fin_ind", "unique_lvl"]).reset_index(drop=True)
    )


def _platform_json_value(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    return value


def sync_inventory_impairment_via_platform(
    rows: pd.DataFrame,
    quarter_period: pd.Timestamp,
) -> Dict[str, object]:
    """Submit calculated impairment rows to the platform-owned linked sync API."""
    _require_columns(rows, FACT_PROFIT_BD_COLUMNS, "fact_profit_bd 待写入数据")
    if rows.empty:
        raise ValueError("待同步的资产减值数据为空")

    target_date = pd.Timestamp(quarter_period).date()
    base_url = os.getenv(PLATFORM_BASE_URL_ENV, DEFAULT_PLATFORM_BASE_URL).strip().rstrip("/")
    token = (
        os.getenv(PLATFORM_TOKEN_ENV, "").strip()
        or os.getenv(PLATFORM_TOKEN_FALLBACK_ENV, "").strip()
    )
    if not token:
        raise RuntimeError(
            f"必须配置 {PLATFORM_TOKEN_ENV} 或 {PLATFORM_TOKEN_FALLBACK_ENV}，" "资产减值结果才能通过平台同步"
        )
    url = f"{base_url}{PLATFORM_SYNC_PATH}"
    payload = {
        "quarter_period": target_date.isoformat(),
        "rows": [
            {column: _platform_json_value(row[column]) for column in FACT_PROFIT_BD_COLUMNS}
            for _, row in rows[FACT_PROFIT_BD_COLUMNS].iterrows()
        ],
    }
    try:
        response = requests.post(
            url,
            json=payload,
            headers={"X-Internal-Token": token},
            timeout=PLATFORM_SYNC_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        body = response.json()
    except requests.RequestException as exc:
        detail = ""
        if getattr(exc, "response", None) is not None:
            try:
                detail = str(exc.response.json().get("detail") or "")
            except (AttributeError, ValueError):
                detail = exc.response.text[:500]
        suffix = f"：{detail}" if detail else ""
        raise RuntimeError(f"资产减值平台同步请求失败{suffix}") from exc
    except ValueError as exc:
        raise RuntimeError("资产减值平台同步返回了无效 JSON") from exc

    if body.get("code") != 200 or not isinstance(body.get("data"), dict):
        raise RuntimeError(f"资产减值平台同步返回异常：{body}")
    return body["data"]


def _fetch_dataframe(cur, query: str, params: Iterable[object]) -> pd.DataFrame:
    cur.execute(query, tuple(params))
    return pd.DataFrame(cur.fetchall(), columns=[description[0] for description in cur.description])


def _validate_source_periods(
    df: pd.DataFrame, expected_periods: Sequence[date], source_name: str
) -> None:
    observed_periods = set(pd.to_datetime(df["acct_period"], errors="coerce").dt.date.dropna())
    missing_periods = sorted(set(expected_periods) - observed_periods)
    if missing_periods:
        formatted = ", ".join(period.isoformat() for period in missing_periods)
        raise ValueError(f"{source_name} 缺少会计期间: {formatted}")


@task(name="load_inventory_impairment_sources", log_prints=True)
def load_inventory_impairment_sources_task(
    periods: Sequence[pd.Timestamp],
) -> Dict[str, pd.DataFrame]:
    """只读加载指定期间的存货和在途存货数据。"""
    period_dates = [pd.Timestamp(period).date() for period in periods]
    conn, cur = connect_to_db()
    if conn is None or cur is None:
        raise ConnectionError("无法连接 PostgreSQL")

    try:
        inventory = _fetch_dataframe(
            cur,
            f"SELECT {', '.join(INVENTORY_COLUMNS)} FROM fact_inventory WHERE acct_period = ANY(%s)",
            (period_dates,),
        )
        in_transit = _fetch_dataframe(
            cur,
            f"SELECT {', '.join(IN_TRANSIT_COLUMNS)} FROM fact_inventory_on_way WHERE acct_period = ANY(%s)",
            (period_dates,),
        )
    finally:
        cur.close()
        conn.close()

    _validate_source_periods(inventory, period_dates, "fact_inventory")
    _validate_source_periods(in_transit, period_dates, "fact_inventory_on_way")
    print(f"存货跌价源数据加载完成：期间数={len(period_dates)}，" f"存货={len(inventory)} 行，在途存货={len(in_transit)} 行")
    return {"inventory": inventory, "in_transit": in_transit}


@task(name="replace_inventory_impairment_in_fact_profit_bd", log_prints=True)
def replace_inventory_impairment_in_fact_profit_bd_task(
    rows: pd.DataFrame, quarter_period: pd.Timestamp
) -> Dict[str, object]:
    """通过平台原子同步指定季度的存货跌价 Fact/staging 记录。"""
    metrics = sync_inventory_impairment_via_platform(rows, quarter_period)

    print(
        f"资产减值平台同步完成：日期={metrics['date']}，"
        f"新增={metrics['inserted']}，更新={metrics['updated']}，"
        f"补链={metrics['repaired_links']}，删除={metrics['deleted']}"
    )
    return metrics


@task(name="calculate_quarterly_inventory_impairment", log_prints=True)
def calculate_quarterly_inventory_impairment_task(
    df_inventory: pd.DataFrame,
    df_in_transit: pd.DataFrame,
    periods: Sequence[pd.Timestamp],
    quarter_period: pd.Timestamp,
) -> Dict[str, pd.DataFrame]:
    """计算季度存货跌价金额。"""
    result = calculate_monthly_and_quarterly_impairment(
        df_inventory=df_inventory,
        df_in_transit=df_in_transit,
        periods=periods,
        quarter_period=quarter_period,
    )
    quarterly = result["quarterly"]
    print(
        f"季度存货跌价计算完成：季度={pd.Timestamp(quarter_period).date()}，"
        f"主体层级数={len(quarterly)}，合计={quarterly['quarter_impairment_amount'].sum():.2f}"
    )
    return result


@task(name="load_recorded_inventory_impairment", log_prints=True)
def load_recorded_inventory_impairment_task(quarter_period: pd.Timestamp) -> pd.DataFrame:
    """只读加载 fact_profit_bd 中指定季度的业报资产减值损失。"""
    conn, cur = connect_to_db()
    if conn is None or cur is None:
        raise ConnectionError("无法连接 PostgreSQL")
    try:
        recorded = _fetch_dataframe(
            cur,
            """
            SELECT fin_ind, unique_lvl, prim_org, sec_org, third_org, mo_amt, source_no
            FROM fact_profit_bd
            WHERE fin_con = %s AND prim_subj = %s AND date = %s
            """,
            ("业报调整", "资产减值损失", pd.Timestamp(quarter_period).date()),
        )
    finally:
        cur.close()
        conn.close()

    print(f"业报校验数据加载完成：季度={pd.Timestamp(quarter_period).date()}，记录数={len(recorded)}")
    return recorded


@task(name="reconcile_quarterly_inventory_impairment", log_prints=True)
def reconcile_quarterly_inventory_impairment_task(
    calculated: pd.DataFrame, recorded: pd.DataFrame, tolerance: float = 0.01
) -> pd.DataFrame:
    """核对季度存货跌价计算结果。"""
    comparison = reconcile_quarterly_impairment(calculated, recorded, tolerance=tolerance)
    status_counts = comparison["status"].value_counts().to_dict()
    calculated_total = comparison["quarter_impairment_amount"].fillna(0.0).sum()
    recorded_total = comparison["recorded_amount"].fillna(0.0).sum()
    print(
        f"季度存货跌价核对完成：状态={status_counts}，"
        f"计算合计={calculated_total:.2f}，业报合计={recorded_total:.2f}，"
        f"差异={calculated_total - recorded_total:.2f}"
    )
    return comparison
