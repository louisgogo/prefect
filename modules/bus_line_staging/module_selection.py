"""Allowlisted module selection for the business-line Staging flow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, TypeAlias

StagingModuleOption: TypeAlias = Literal[
    "费用",
    "收入",
    "其他损益",
    "存货",
    "应收",
    "在途存货",
]


@dataclass(frozen=True)
class StagingModule:
    code: str
    label: str
    staging_table: str
    fact_table: str


STAGING_MODULES = (
    StagingModule("expense", "费用", "staging_bus_expense", "fact_expense"),
    StagingModule("revenue", "收入", "staging_bus_revenue", "fact_revenue"),
    StagingModule("profit_other", "其他损益", "staging_bus_profit_bd", "fact_profit_bd"),
    StagingModule("inventory", "存货", "staging_bus_inventory", "fact_inventory"),
    StagingModule("receivable", "应收", "staging_bus_receivable", "fact_receivable"),
    StagingModule(
        "in_transit_inventory",
        "在途存货",
        "staging_bus_in_transit_inventory",
        "fact_inventory_on_way",
    ),
)

MODULE_BY_CODE = {module.code: module for module in STAGING_MODULES}
ALL_MODULE_CODES = tuple(module.code for module in STAGING_MODULES)
OPTION_TO_CODE = {module.label: module.code for module in STAGING_MODULES}
ALL_MODULE_OPTIONS: tuple[StagingModuleOption, ...] = tuple(
    module.label for module in STAGING_MODULES
)  # type: ignore[assignment]


def normalize_modules(modules: Iterable[str] | None) -> tuple[str, ...]:
    """Return valid module codes in canonical execution order; empty means all."""
    if modules is None:
        return ALL_MODULE_CODES

    requested = set()
    for value in modules:
        normalized = str(value).strip()
        if not normalized:
            continue
        requested.add(OPTION_TO_CODE.get(normalized, normalized))
    if not requested:
        return ALL_MODULE_CODES

    unknown = sorted(requested.difference(MODULE_BY_CODE))
    if unknown:
        allowed = ", ".join(ALL_MODULE_CODES)
        raise ValueError(f"不支持的Staging模块: {', '.join(unknown)}；可选值: {allowed}")
    return tuple(code for code in ALL_MODULE_CODES if code in requested)


def module_labels(module_codes: Iterable[str]) -> list[str]:
    return [MODULE_BY_CODE[code].label for code in module_codes]


def module_fact_tables(module_codes: Iterable[str]) -> tuple[str, ...]:
    return tuple(MODULE_BY_CODE[code].fact_table for code in module_codes)


def module_staging_tables(module_codes: Iterable[str]) -> tuple[str, ...]:
    return tuple(MODULE_BY_CODE[code].staging_table for code in module_codes)


def is_full_refresh(module_codes: Iterable[str]) -> bool:
    return tuple(module_codes) == ALL_MODULE_CODES
