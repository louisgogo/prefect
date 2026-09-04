"""往来对账 - 数据采集 & 写库 Tasks

阶段0：从 2-往来对账填报表 同步 4月文件到 9-数据源
阶段1：从 MySQL + 共享盘 Excel 采集原始数据，先删除目标月旧数据，再写入 PostgreSQL。
移植自 FastAPI 项目 recon_tool.py，改为同步版本，依赖 mypackage。
"""
import json
import os
import platform
import re
import shutil
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests
from prefect import task

# 添加 prefect 根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# ──────────────────────────────────────────────
# 辅助：计算目标月份参数
# ──────────────────────────────────────────────


def _get_data_source_paths() -> Tuple[str, str]:
    """获取数据源和填报表路径"""
    if platform.system() == "Windows":
        source_dir = r"Z:\10-内部往来对账\2-往来对账填报表"
        target_dir = r"Z:\10-内部往来对账\9-数据源"
    else:
        source_dir = r"/mnt/xgd_share/10-内部往来对账/2-往来对账填报表"
        target_dir = r"/mnt/xgd_share/10-内部往来对账/9-数据源"
    return source_dir, target_dir


# ──────────────────────────────────────────────
# Task 0：同步数据源（同步目标月份到当前月份之间修改的文件）
# ──────────────────────────────────────────────


def _calc_month_start_end_ts(year: int, month: int) -> Tuple[float, float]:
    """计算某年月的起始和结束时间戳."""
    start = datetime(year, month, 1).timestamp()
    if month == 12:
        end = datetime(year + 1, 1, 1).timestamp()
    else:
        end = datetime(year, month + 1, 1).timestamp()
    return start, end


@task(name="sync_data_source", log_prints=True)
def sync_data_source_task(target_date: Optional[str] = None) -> Dict[str, Any]:
    """
    从 2-往来对账填报表 同步文件到 9-数据源。
    同步目标月份到当前自然月份之间所有修改的文件，防止遗漏。

    Args:
        target_date: 目标月份，格式 YYYY-MM-DD（如 "2026-02-01"）。
                     不传则自动使用上个自然月（相对于运行日期）。

    Returns:
        {'success': bool, 'copied': int, 'skipped': int, 'message': str, 'months': list}
    """
    source_dir, target_dir = _get_data_source_paths()

    # 计算目标月份和当前月份
    if target_date:
        try:
            target = pd.to_datetime(target_date).date()
        except Exception:
            target = date.today()
    else:
        today = date.today()
        target = today.replace(day=1) - timedelta(days=1)

    target_month = date(target.year, target.month, 1)
    current_month = date.today().replace(day=1)

    # 计算需要同步的月份范围（目标月份到当前月份，包含两端）
    months_to_sync = []
    current = target_month
    while current <= current_month:
        months_to_sync.append((current.year, current.month))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    # 构建时间戳范围
    sync_ranges = []
    for y, m in months_to_sync:
        start_ts, end_ts = _calc_month_start_end_ts(y, m)
        sync_ranges.append((start_ts, end_ts, f"{y}年{m}月"))

    print(f"--> 开始同步数据源")
    print(f"    源目录: {source_dir}")
    print(f"    目标目录: {target_dir}")
    print(f"    目标月份: {target_month}")
    print(f"    当前月份: {current_month}")
    print(f"    同步月份: {[r[2] for r in sync_ranges]}")

    # 检查源目录
    if not os.path.exists(source_dir):
        return {
            "success": False,
            "copied": 0,
            "skipped": 0,
            "message": f"源目录不存在: {source_dir}",
            "months": [],
        }

    # 1. 清空目标目录中的所有文件
    deleted_count = 0
    if os.path.exists(target_dir):
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                filepath = os.path.join(root, file)
                try:
                    os.remove(filepath)
                    deleted_count += 1
                except Exception as e:
                    print(f"    [WARN] 删除失败 {filepath}: {e}")
        print(f"--> 已清理目标目录，删除 {deleted_count} 个旧文件")

    # 2. 找出源目录中在目标月份到当前月份之间修改的文件并复制
    copied_count = 0
    skipped_count = 0
    copied_months = set()

    for root, dirs, files in os.walk(source_dir):
        for file in files:
            # 跳过临时文件和房租相关文件
            if "~" in file or "$" in file or "房租" in file:
                skipped_count += 1
                continue

            source_path = os.path.join(root, file)

            # 检查修改时间是否在同步范围内
            try:
                mtime = os.path.getmtime(source_path)
                in_range = False
                for start_ts, end_ts, month_str in sync_ranges:
                    if start_ts <= mtime < end_ts:
                        in_range = True
                        copied_months.add(month_str)
                        break
                if not in_range:
                    skipped_count += 1
                    continue
            except Exception as e:
                print(f"    [WARN] 无法获取文件时间 {source_path}: {e}")
                skipped_count += 1
                continue

            # 计算目标路径，保持目录结构
            rel_path = os.path.relpath(source_path, source_dir)
            target_path = os.path.join(target_dir, rel_path)

            # 创建目标目录
            try:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
            except Exception as e:
                print(f"    [WARN] 创建目录失败 {os.path.dirname(target_path)}: {e}")
                continue

            # 复制文件（保留元数据）
            try:
                shutil.copy2(source_path, target_path)
                copied_count += 1
                print(f"    复制: {rel_path}")
            except Exception as e:
                print(f"    [WARN] 复制失败 {rel_path}: {e}")

    print(f"--> 同步完成: 复制 {copied_count} 个文件，跳过 {skipped_count} 个文件")
    return {
        "success": True,
        "copied": copied_count,
        "skipped": skipped_count,
        "message": f"同步完成，复制 {copied_count} 个文件（月份: {list(copied_months)}）",
        "months": list(copied_months),
    }


# ──────────────────────────────────────────────
# 辅助：计算目标月份参数
# ──────────────────────────────────────────────


def _calc_target_month(target_date: Optional[str] = None) -> Tuple[date, str, str]:
    """
    根据 target_date 计算目标月的 date 对象、字符串，以及扫描路径。

    Args:
        target_date: 格式 YYYY-MM-DD，None 时取上个自然月。

    Returns:
        (lastmonth: date, lastmonth_str: str, scan_path: str)
    """
    if target_date:
        try:
            target = pd.to_datetime(target_date).date()
        except Exception:
            print(f"[WARN] 无效的日期格式: {target_date}，自动使用上个月")
            today = date.today()
            target = today.replace(day=1) - timedelta(days=1)
    else:
        today = date.today()
        target = today.replace(day=1) - timedelta(days=1)

    lastmonth = date(target.year, target.month, 1)
    lastmonth_str = lastmonth.strftime("%Y-%m-%d")

    if platform.system() == "Windows":
        scan_path = r"Z:\10-内部往来对账\9-数据源"
    else:
        scan_path = r"/mnt/xgd_share/10-内部往来对账/9-数据源"

    return lastmonth, lastmonth_str, scan_path


# ──────────────────────────────────────────────
# Task 1：从金蝶正表现金流补充 staging_recon
# ──────────────────────────────────────────────


def _cf_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _cf_norm(value: Any) -> str:
    return re.sub(r"\s+", "", _cf_text(value)).replace("（", "(").replace("）", ")").upper()


def _cf_code_key(value: Any) -> str:
    """规范化金蝶核算维度编码，但保留编码中的层级点号。"""
    text = _cf_text(value)
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]
    return text


def _cf_amount(value: Any) -> Optional[Decimal]:
    try:
        text = _cf_text(value).replace(",", "")
        return Decimal(text) if text else None
    except (InvalidOperation, ValueError):
        return None


def _cf_period_row(row: Dict[str, Any], period: str) -> bool:
    year, month = period.split("-")
    value = _cf_text(row.get("FDate") or row.get("date")).replace("/", "-")
    match = re.match(r"^(\d{4})-(\d{1,2})", value)
    if match:
        return match.group(1) == year and int(match.group(2)) == int(month)
    return _cf_text(row.get("FYear")) == year and _cf_text(row.get("FPeriod")).lstrip(
        "0"
    ) == month.lstrip("0")


def _cf_row_value(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    # 金蝶不同网关版本偶尔改变字段大小写；不改变业务字段名口径，
    # 仅在读取时做不区分大小写的兼容。
    lowered = {str(key).lower(): value for key, value in row.items()}
    for name in names:
        value = lowered.get(name.lower())
        if value not in (None, ""):
            return value
    return None


# 金蝶接口返回负数时，不能简单丢弃；负数表示方向记反时，应落到对应的
# 正反向现金流项目。这里按现金流项目名称维护，避免依赖不同账套的项目编码。
_CF_REVERSE_ITEMS = {
    "销售商品、提供劳务收到的现金": "购买商品、接受劳务支付的现金",
    "收到其他与经营活动有关的现金": "支付其他与经营活动有关的现金",
    "购买商品、接受劳务支付的现金": "销售商品、提供劳务收到的现金",
    "支付其他与经营活动有关的现金": "收到其他与经营活动有关的现金",
    "支付给职工以及为职工支付的现金": "收到其他与经营活动有关的现金",
    "收回投资收到的现金": "投资支付的现金",
    "投资支付的现金": "收回投资收到的现金",
    "处置固定资产、无形资产和其他长期资产收回的现金净额": "购建固定资产、无形资产和其他长期资产支付的现金",
    "购建固定资产、无形资产和其他长期资产支付的现金": "处置固定资产、无形资产和其他长期资产收回的现金净额",
    "支付其他与筹资活动有关的现金": "收到其他与筹资活动有关的现金",
    "收到其他与筹资活动有关的现金": "支付其他与筹资活动有关的现金",
    "分配股利、利润或偿付利息支付的现金": "收到其他与筹资活动有关的现金",
    "吸收投资收到的现金": "支付其他与筹资活动有关的现金",
    "现金流出": "现金流入",
    "现金流入": "现金流出",
}


def _cf_actual_item(item: str, amount: Decimal) -> Tuple[str, Decimal]:
    """按技能规则返回实际项目和正数金额。"""
    reverse_item = _CF_REVERSE_ITEMS.get(item)
    if reverse_item is None:
        normalized = _cf_norm(item)
        reverse_item = next(
            (
                target
                for source, target in _CF_REVERSE_ITEMS.items()
                if _cf_norm(source) == normalized
            ),
            None,
        )
    if amount < 0 and reverse_item:
        return reverse_item, abs(amount)
    return item, amount


def _cf_txn_date(value: Any, period: str) -> str:
    """把金蝶日期统一成 YYYY-MM-DD，无法解析时落到目标月月初。"""
    text = _cf_text(value)
    if text:
        parsed = pd.to_datetime(text.replace("/", "-"), errors="coerce")
        if not pd.isna(parsed):
            return parsed.strftime("%Y-%m-%d")
    return f"{period}-01"


def _fetch_kingdee_cashflow_rows(
    period: str,
    books: list[str],
    entity_codes: list[str],
    entity_names: list[str],
) -> list[dict[str, Any]]:
    token = _cf_text(os.environ.get("XGD_TOKEN")) or _cf_text(
        os.environ.get("AIHUB_FONE_API_TOKEN")
    )
    if not token:
        raise RuntimeError("缺少 XGD_TOKEN 或 AIHUB_FONE_API_TOKEN，无法查询金蝶现金流")
    year, month = period.split("-")
    endpoint = f"{os.environ.get('KINGDEE_VOUCHER_BASE_URL', 'https://aihub.xgd.com').rstrip('/')}/api/proxy/erp/sdk/GetSysReportData"
    fields = [
        "FYear",
        "FPeriod",
        "FDate",
        "FAccountbookname",
        "FVoucherGroup",
        "FVoucherGroupNo",
        "FSEQ",
        "FExplanation",
        "FAcctNo",
        "FAcctName",
        "FDETAILNUMBER",
        "FDETAILNAME",
        "FCashflowAmount",
        "FCFItemNo",
        "FCFItemName",
        "FCFSubItemName",
    ]
    model = {
        "FACCTBOOKID": [{"FNumber": book} for book in books],
        "FByPeriod": "true",
        "FByDate": "false",
        "FSTARTYEAR": year,
        "FSTARTPERIOD": month,
        "FENDYEAR": year,
        "FENDPERIOD": month,
        "FNOTPOSTVOUCHER": "true",
        "FSHOWALLVCHINFO": "true",
    }
    rows: list[dict[str, Any]] = []
    query_filters = []
    for field, values, chunk_size in (
        ("FDETAILNUMBER", entity_codes, 30),
        ("FDETAILNAME", entity_names, 25),
    ):
        for offset in range(0, len(values), chunk_size):
            chunk = values[offset : offset + chunk_size]
            query_filters.append(
                [
                    {
                        "Left": "",
                        "FieldName": field,
                        "Compare": "81",
                        "Value": value,
                        "Right": "",
                        "Logic": 1 if i < len(chunk) - 1 else 0,
                    }
                    for i, value in enumerate(chunk)
                ]
            )
    query_filters.append(
        [
            {
                "Left": "",
                "FieldName": "FAcctName",
                "Compare": "81",
                "Value": "内部关联方往来",
                "Right": "",
                "Logic": 0,
            }
        ]
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for filters in query_filters:
        start = 0
        while True:
            payload = {
                "FormId": "GL_Rpt_CashflowQuery",
                "FieldKeys": ",".join(fields),
                "SchemeId": os.environ.get("KINGDEE_CASHFLOW_SCHEME_ID", ""),
                "StartRow": start,
                "Limit": 2000,
                "IsVerifyBaseDataField": True,
                "FilterString": filters,
                "Model": model,
            }
            response = requests.post(endpoint, json=payload, headers=headers, timeout=180)
            response.raise_for_status()
            body = response.json()
            result = body.get("Result", body) if isinstance(body, dict) else {}
            status = result.get("ResponseStatus") if isinstance(result, dict) else None
            if isinstance(status, dict) and status.get("IsSuccess") is False:
                errors = status.get("Errors") or [{}]
                message = _cf_text(errors[0].get("Message")) if isinstance(errors[0], dict) else ""
                raise RuntimeError(f"金蝶现金流查询失败: {message or '未知错误'}")
            page = result.get("Rows") or result.get("rows") or []
            field_names = result.get("Fields") or fields
            for item in page:
                if isinstance(item, dict):
                    rows.append(item)
                elif isinstance(item, (list, tuple)):
                    rows.append(dict(zip(field_names, item)))
            if len(page) < 2000:
                break
            start += len(page)
            if start > 100000:
                raise RuntimeError("金蝶现金流查询超过10万行，请收紧主体范围")
    unique = {}
    for row in rows:
        # FSHOWALLVCHINFO=true 会返回同一凭证行的现金腿/对方腿等副本。
        # 去重键保留账簿、凭证日期、凭证字、凭证号、行号、科目、核算维度、
        # 现金流项目和金额，避免不同主体或同一分录拆分流量被误合并。
        key = tuple(
            _cf_text(_cf_row_value(row, name))
            for name in (
                "FAccountbookname",
                "FDate",
                "FVoucherGroup",
                "FVoucherGroupNo",
                "FSEQ",
                "FAcctNo",
                "FDETAILNUMBER",
                "FCFItemNo",
                "FCashflowAmount",
            )
        )
        unique[key] = row
    return [row for row in unique.values() if _cf_period_row(row, period)]


@task(name="import_kingdee_cashflow_to_staging", log_prints=True)
def import_kingdee_cashflow_to_staging_task(target_date: Optional[str] = None) -> Dict[str, Any]:
    """按流程目标期间导入金蝶正表现金流；已有主体期间数据时整主体跳过。"""
    from mypackage.utilities import connect_to_db

    target, _, _ = _calc_target_month(target_date)
    period = target.strftime("%Y-%m")
    conn, cur = connect_to_db()
    if conn is None or cur is None:
        raise RuntimeError("无法连接财务数据库")
    try:
        cur.execute(
            """
            SELECT btrim(fin_ind),
                   btrim(COALESCE(cust_full_name, '')),
                   btrim(COALESCE(fone::text, ''))
            FROM dim_fin_ind
            WHERE fin_ind IS NOT NULL AND btrim(fin_ind) <> ''
            """
        )
        entities = cur.fetchall()
        short_by_norm = {_cf_norm(row[0]): _cf_text(row[0]) for row in entities}
        full_by_norm = {_cf_norm(row[1]): _cf_text(row[0]) for row in entities if _cf_text(row[1])}
        full_names_by_company = {}
        for row in entities:
            company_name = _cf_text(row[0])
            full_name = _cf_text(row[1])
            if company_name and full_name:
                full_names_by_company.setdefault(company_name, set()).add(full_name)
        code_to_short = {
            _cf_code_key(row[2]): _cf_text(row[0]) for row in entities if _cf_text(row[2])
        }
        book_name_candidates = []
        for row in entities:
            short_name = _cf_text(row[0])
            full_name = _cf_text(row[1])
            if short_name:
                book_name_candidates.append((_cf_norm(short_name), short_name))
            if full_name:
                book_name_candidates.append((_cf_norm(full_name), short_name))
        companies = sorted(set(short_by_norm.values()))
        books = [
            x.strip()
            for x in os.environ.get(
                "KINGDEE_CASHFLOW_BOOKS",
                os.environ.get("KINGDEE_CASHFLOW_BOOK", ""),
            ).split(",")
            if x.strip()
        ]
        if not books:
            # dim_fin_ind.fone 是当前财务库维护的金蝶账簿编码；显式环境变量
            # 优先，未配置时使用该参数表作为兼容回退。
            books = sorted(code_to_short)
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", (f"staging-recon-cashflow:{period}",)
        )
        # 保护边界按“主体 + 当前期间”判断，而不是只看现金流类别：
        # 只要该主体本期间已经存在任意往来填报数据（手工、历史或其他类别），
        # 就不再自动补入金蝶现金流，避免后续流程把已有填报当成可覆盖数据。
        cur.execute(
            'SELECT DISTINCT btrim("公司简称") FROM staging_recon '
            "WHERE \"期间\" = %s AND btrim(COALESCE(\"公司简称\", '')) <> ''",
            (period,),
        )
        existing = {_cf_text(row[0]) for row in cur.fetchall()}
        existing_norm = {_cf_norm(company) for company in existing}
        eligible = [company for company in companies if _cf_norm(company) not in existing_norm]
        if not eligible:
            return {
                "success": True,
                "period": period,
                "inserted_count": 0,
                "imported_companies": [],
                "skipped_companies": sorted(existing),
                "no_match_companies": [],
                "message": "当前期间所有主体已有 staging_recon 数据，未导入",
            }
        eligible_codes = [code for code, company in code_to_short.items() if company in eligible]
        eligible_names = sorted(
            {name for company in eligible for name in full_names_by_company.get(company, set())}
            | set(eligible)
        )
        rows = _fetch_kingdee_cashflow_rows(period, books, eligible_codes, eligible_names)
        cur.execute(
            "SELECT name FROM dim_ic_subject WHERE is_active = true AND category_code = %s",
            ("现金流量",),
        )
        subject_map = {_cf_norm(row[0]): _cf_text(row[0]) for row in cur.fetchall()}
        inserted = 0
        imported_companies = set()
        matched_companies = set()
        seen = set()
        for row in rows:
            # 只导入金蝶现金流量表正表项目。仅有 FCFSubItemName 的间接法
            # 调节项不进入 staging_recon；若同一行同时有正表和附表项目，
            # 仍以正表项目为准。
            original_item = _cf_text(_cf_row_value(row, "FCFItemName", "item_name"))
            if not original_item or _cf_norm(original_item) not in subject_map:
                continue
            amount = _cf_amount(_cf_row_value(row, "FCashflowAmount", "cash_amount"))
            if amount is None:
                continue
            item, amount = _cf_actual_item(original_item, amount)
            if _cf_norm(item) not in subject_map or amount == 0:
                continue
            book = _cf_text(_cf_row_value(row, "FAccountbookname", "account_book"))
            company = (
                full_by_norm.get(_cf_norm(book))
                or short_by_norm.get(_cf_norm(book))
                or code_to_short.get(_cf_code_key(book))
            )
            if not company:
                # 某些账套返回账簿显示名而非编码；仅在唯一主体候选时使用
                # 包含匹配，避免短名称相互包含造成误归属。
                book_norm = _cf_norm(book)
                candidates = {
                    short_name
                    for name_norm, short_name in book_name_candidates
                    if name_norm and name_norm in book_norm
                }
                if len(candidates) == 1:
                    company = next(iter(candidates))
            if not company or company not in eligible:
                continue
            detail_name = _cf_text(_cf_row_value(row, "FDETAILNAME", "detail_name"))
            counterparty = ""
            # 明细名称可能同时带编码、全称和多个分隔符；只接受维度表中
            # 可精确映射的主体，避免把普通摘要误识别成对方主体。
            detail_parts = re.split(r"[\\/、,，;；|]+", detail_name)
            for part in detail_parts:
                token = _cf_norm(part)
                if token in short_by_norm:
                    counterparty = short_by_norm[token]
                    break
                if token in full_by_norm:
                    counterparty = full_by_norm[token]
                    break
                code = _cf_code_key(part)
                if code in code_to_short:
                    counterparty = code_to_short[code]
                    break
            if not counterparty:
                continue
            matched_companies.add(company)
            txn_date = _cf_txn_date(_cf_row_value(row, "FDate", "date"), period)
            key = (
                company,
                counterparty,
                item,
                txn_date,
                _cf_text(row.get("FVoucherGroupNo")),
                _cf_text(row.get("FSEQ")),
                str(amount),
            )
            if key in seen:
                continue
            seen.add(key)
            content = " ".join(
                v
                for v in (
                    _cf_text(row.get("FExplanation")),
                    f"凭证{_cf_text(row.get('FVoucherGroup'))}{_cf_text(row.get('FVoucherGroupNo'))}",
                )
                if v
            )
            # 系统来源使用简洁、可读的备注；原始科目已保存在“科目名称”中，
            # 不再把接口元数据序列化到用户可见备注。
            remark = "金蝶现金流数据"
            cur.execute(
                """
                INSERT INTO staging_recon (
                    id, "期间", "大类", "公司简称", "科目名称", "类别", "附注分类",
                    "对方简称", "具体内容", "金额", "日期", "备注", "责任人",
                    "验证结果", "上报状态", "创建人", "更新时间", "创建时间"
                ) VALUES (
                    gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, NULL, NOW(), NOW()
                )
                """,
                (
                    period,
                    "现金流量",
                    company,
                    subject_map[_cf_norm(item)],
                    "其他",
                    "无",
                    counterparty,
                    content,
                    amount,
                    txn_date,
                    remark,
                    "金蝶现金流",
                    json.dumps({"passed": True, "source": "kingdee"}, ensure_ascii=False),
                    "已验证通过待上报",
                ),
            )
            inserted += 1
            imported_companies.add(company)
        conn.commit()
        return {
            "success": True,
            "period": period,
            "inserted_count": inserted,
            "imported_companies": sorted(imported_companies),
            "skipped_companies": sorted(existing),
            "no_match_companies": sorted(set(eligible) - matched_companies),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ──────────────────────────────────────────────
# Task 1：从 MySQL 读取当月对账数据
# ──────────────────────────────────────────────


@task(name="fetch_recon_from_mysql", log_prints=True)
def fetch_recon_from_mysql_task(target_date: Optional[str] = None) -> pd.DataFrame:
    """
    从 MySQL Fone2BI_IntCommCheck 表读取指定月份对账数据，并映射列名为英文。

    Args:
        target_date: 格式 YYYY-MM-DD；None 时取上个自然月。

    Returns:
        列名已映射为英文的 DataFrame。
    """
    from mypackage.utilities import engine_to_mysql

    lastmonth, lastmonth_str, _ = _calc_target_month(target_date)

    column_mapping = {
        "公司简称": "co_abbr",
        "科目名称": "prim_subj",
        "类别": "class",
        "对方简称": "cp_abbr",
        "具体内容": "content",
        "金额": "amt",
        "日期": "date",
        "备注": "remarks",
        "责任人": "resp_person",
        "大类": "major_cat",
        "附注分类": "note_cat",
    }

    try:
        engine = engine_to_mysql()
        sql = f"SELECT * FROM Fone2BI_IntCommCheck WHERE 日期 = '{lastmonth_str}'"
        print(f"--> 从 MySQL 查询: {sql}")
        df = pd.read_sql(sql, con=engine)

        # 删除 id 列（如存在）
        if "id" in df.columns:
            df = df.drop(columns=["id"])

        # 列名映射（只映射存在的列）
        rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        # note_cat 置空（MySQL 无此字段）
        df["note_cat"] = None

        print(f"--> 从 MySQL 获取 {len(df)} 条记录")
        if df.empty:
            raise ValueError(f"MySQL 没有 {lastmonth_str} 的数据，流程终止")

        return df

    except Exception as e:
        print(f"[ERROR] 从 MySQL 获取数据失败: {e}")
        raise


# ──────────────────────────────────────────────
# Task 2：从共享盘 Excel 收集数据
# ──────────────────────────────────────────────


@task(name="collect_recon_from_excel", log_prints=True)
def collect_recon_from_excel_task(target_date: Optional[str] = None) -> pd.DataFrame:
    """
    扫描共享盘指定目录，读取"内部往来填报表"子表，映射列名为英文。
    按日期列内容过滤目标月份数据（而非文件名）。

    Args:
        target_date: 格式 YYYY-MM-DD；None 时取上个自然月。

    Returns:
        列名已映射为英文的 DataFrame，若无数据则返回空 DataFrame。
    """
    lastmonth, lastmonth_str, scan_path = _calc_target_month(target_date)

    column_mapping = {
        "公司简称": "co_abbr",
        "科目名称": "prim_subj",
        "类别": "class",
        "对方简称": "cp_abbr",
        "具体内容": "content",
        "金额": "amt",
        "日期": "date",
        "备注": "remarks",
        "责任人": "resp_person",
        "大类": "major_cat",
        "附注分类": "note_cat",
    }
    sheet_name = "内部往来填报表"

    # 目标月份的起止时间（用于按日期列内容过滤）
    month_start = pd.Timestamp(lastmonth)
    if lastmonth.month == 12:
        month_end = pd.Timestamp(date(lastmonth.year + 1, 1, 1))
    else:
        month_end = pd.Timestamp(date(lastmonth.year, lastmonth.month + 1, 1))

    print(f"--> 扫描 Excel 路径: {scan_path}，目标月份: {lastmonth_str}")

    all_dfs = []
    if not os.path.exists(scan_path):
        print(f"[WARN] 共享盘路径不存在: {scan_path}，跳过 Excel 采集")
        return pd.DataFrame()

    filter_date = pd.to_datetime(lastmonth_str).date()  # 目标日期对象

    # 使用 os.walk 递归扫描子目录（原始 recon_tool.py 使用 rglob，等价于此）
    for root, dirs, files in os.walk(scan_path):
        for filename in files:
            if not filename.endswith((".xlsx", ".xlsm")):
                continue
            if "~" in filename or "$" in filename:  # 跳过临时文件
                continue
            filepath = os.path.join(root, filename)
            try:
                xls = pd.ExcelFile(filepath)
                if sheet_name not in xls.sheet_names:
                    continue  # 没有目标子表，静默跳过
                df = pd.read_excel(xls, sheet_name=sheet_name)
                df = df.dropna(how="all")

                # 按日期列内容过滤目标月份（使用 .dt.date 比较，与原始 recon_tool.py 一致）
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                    df = df[df["日期"].notna() & (df["日期"].dt.date == filter_date)].copy()

                if df.empty:
                    continue  # 该文件没有目标月数据，跳过

                rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
                df = df.rename(columns=rename_map)
                # 补齐 note_cat
                if "note_cat" not in df.columns:
                    df["note_cat"] = None
                all_dfs.append(df)
                print(f"--> 读取 {filename} 完成，目标月数据 {len(df)} 条")
            except Exception as e:
                print(f"[WARN] 读取 {filename} 失败: {e}")

    if not all_dfs:
        print("[WARN] 共享盘 Excel 中未找到目标月份数据，Excel 数据为空（不影响 MySQL 数据）")
        return pd.DataFrame()

    df_combined = pd.concat(all_dfs, ignore_index=True)
    print(f"--> 共从 Excel 获取 {len(df_combined)} 条记录")
    return df_combined


# ──────────────────────────────────────────────
# Task 3：从 staging_recon 收集填报数据
# ──────────────────────────────────────────────


@task(name="fetch_recon_from_staging_recon", log_prints=True)
def fetch_recon_from_staging_recon_task(target_date: Optional[str] = None) -> pd.DataFrame:
    """
    从 PostgreSQL staging_recon 表读取指定月份的填报数据，并映射列名为英文。

    该数据源替代共享盘 Excel 填报表；FONE/MySQL 侧数据仍由 fetch_recon_from_mysql_task 获取。
    """
    from mypackage.utilities import engine_to_db
    from sqlalchemy import text

    lastmonth, lastmonth_str, _ = _calc_target_month(target_date)

    column_mapping = {
        "公司简称": "co_abbr",
        "科目名称": "prim_subj",
        "类别": "class",
        "对方简称": "cp_abbr",
        "具体内容": "content",
        "金额": "amt",
        "日期": "date",
        "备注": "remarks",
        "责任人": "resp_person",
        "大类": "major_cat",
        "附注分类": "note_cat",
    }
    source_cols = list(column_mapping.keys())

    try:
        engine = engine_to_db()
        sql = text(
            """
            SELECT "公司简称", "科目名称", "类别", "对方简称", "具体内容", "金额",
                   "日期", "备注", "责任人", "大类", "附注分类"
            FROM public.staging_recon
            WHERE "期间" = :target_period
            """
        )
        print(f"--> 从 PostgreSQL staging_recon 查询目标月份: {lastmonth_str}")
        df = pd.read_sql(
            sql,
            con=engine,
            params={"target_period": lastmonth.strftime("%Y-%m")},
        )

        if df.empty:
            print(f"[WARN] staging_recon 没有 {lastmonth_str} 的数据，填报数据为空")
            return pd.DataFrame()

        for col in source_cols:
            if col not in df.columns:
                df[col] = None

        df = df[source_cols].rename(columns=column_mapping)
        print(f"--> 从 staging_recon 获取 {len(df)} 条记录")
        return df

    except Exception as e:
        print(f"[ERROR] 从 staging_recon 获取数据失败: {e}")
        raise


# ──────────────────────────────────────────────
# Task 4：删除目标月旧数据
# ──────────────────────────────────────────────


@task(name="delete_old_recon_data", log_prints=True)
def delete_old_recon_data_task(target_date: Optional[str] = None) -> Dict[str, Any]:
    """
    删除 PostgreSQL excel_account_recon 表中目标月份的旧数据。

    Args:
        target_date: 格式 YYYY-MM-DD；None 时取上个自然月。

    Returns:
        {'success': bool, 'message': str}
    """
    from mypackage.utilities import engine_to_db
    from sqlalchemy import text

    lastmonth, lastmonth_str, _ = _calc_target_month(target_date)

    try:
        engine = engine_to_db()
        with engine.connect() as conn:
            trans = conn.begin()
            if lastmonth.month == 12:
                next_month = date(lastmonth.year + 1, 1, 1)
            else:
                next_month = date(lastmonth.year, lastmonth.month + 1, 1)
            try:
                count_result = conn.execute(
                    text(
                        "SELECT count(*) FROM excel_account_recon "
                        "WHERE date >= :month_start AND date < :next_month"
                    ),
                    {"month_start": lastmonth, "next_month": next_month},
                )
                count = count_result.scalar()
                print(f"--> 准备删除 {lastmonth_str[:7]} 旧数据，现有 {count} 条")

                conn.execute(
                    text(
                        "DELETE FROM excel_account_recon "
                        "WHERE date >= :month_start AND date < :next_month"
                    ),
                    {"month_start": lastmonth, "next_month": next_month},
                )
                trans.commit()
                print(f"--> 删除 {lastmonth_str[:7]} 旧数据完成，共 {count} 条")
                return {"success": True, "message": f"成功删除 {lastmonth_str[:7]} 旧数据 {count} 条"}
            except Exception:
                trans.rollback()
                raise
    except Exception as e:
        print(f"[ERROR] 删除旧数据失败: {e}")
        return {"success": False, "error": str(e)}


# ──────────────────────────────────────────────
# Task 5：合并 MySQL + Excel/Staging 并写入 PostgreSQL
# ──────────────────────────────────────────────


@task(name="insert_recon_data", log_prints=True)
def insert_recon_data_task(
    df_mysql: pd.DataFrame,
    df_excel: pd.DataFrame,
) -> Dict[str, Any]:
    """
    合并 MySQL 与 Excel 数据，做数据预处理后写入 PostgreSQL excel_account_recon 表。

    Args:
        df_mysql: 从 MySQL 获取的 DataFrame
        df_excel: 从 Excel 获取的 DataFrame（可为空）

    Returns:
        {'success': bool, 'message': str, 'count': int}
    """
    from mypackage.utilities import engine_to_db

    try:
        # 合并
        if df_excel.empty:
            df_combined = df_mysql.copy()
            print(f"--> Excel 为空，仅使用 MySQL 数据 {len(df_combined)} 条")
        else:
            # 统一列集合（取交集避免结构不一致）
            mysql_cols = set(df_mysql.columns)
            excel_cols = set(df_excel.columns)
            if mysql_cols != excel_cols:
                common_cols = list(mysql_cols & excel_cols)
                print(f"[WARN] 列结构不一致，取交集: {common_cols}")
                df_mysql = df_mysql[common_cols]
                df_excel = df_excel[common_cols]
            df_combined = pd.concat([df_mysql, df_excel], ignore_index=True)
            print(f"--> 合并后共 {len(df_combined)} 条记录")

        if df_combined.empty:
            raise ValueError("合并后数据为空，无法写入")

        # 第一步：在做任何字符串转换之前，先用 major_cat 的原始 NaN 状态过滤
        # （此时 NaN 还是真正的 NaN，dropna 最可靠）
        before_count = len(df_combined)
        df_combined = df_combined.dropna(subset=["major_cat"]).copy()
        if len(df_combined) < before_count:
            print(
                f"[INFO] 已过滤 {before_count - len(df_combined)} 条 major_cat 为空的无效行，剩余 {len(df_combined)} 条"
            )

        # 数据预处理
        df = df_combined.copy()
        if "amt" in df.columns:
            df["amt"] = pd.to_numeric(df["amt"], errors="coerce").fillna(0)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").fillna(
                pd.Timestamp("1900-01-01")
            )

        string_cols = [
            "major_cat",
            "co_abbr",
            "prim_subj",
            "class",
            "cp_abbr",
            "content",
            "remarks",
            "resp_person",
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).replace("nan", "").replace("None", "")

        # 剔除 major_cat（大类）为空的脏数据行，这类行是 Excel 末尾空行被读进来的
        before_count = len(df)
        df = df[df["major_cat"].str.strip().astype(bool)].copy()
        if len(df) < before_count:
            print(f"[INFO] 已过滤 {before_count - len(df)} 条 major_cat 为空的无效行，剩余 {len(df)} 条")

        # 剔除 co_abbr/cp_abbr（公司简称/对方简称）为空的记录，避免违反外键约束
        for col, label in [("co_abbr", "公司简称"), ("cp_abbr", "对方简称")]:
            before_count = len(df)
            df = df[df[col].str.strip().astype(bool)].copy()
            if len(df) < before_count:
                print(
                    f"[WARN] 存在 {before_count - len(df)} 条 {col}（{label}）为空白的记录，已进行过滤，剩余 {len(df)} 条"
                )

        # 写入数据库
        engine = engine_to_db()
        df.to_sql("excel_account_recon", con=engine, if_exists="append", index=False)
        print(f"--> 成功写入 excel_account_recon，共 {len(df)} 条记录")
        return {"success": True, "message": f"写入完成", "count": len(df)}

    except Exception as e:
        print(f"[ERROR] 写入数据库失败: {e}")
        return {"success": False, "error": str(e), "count": 0}
