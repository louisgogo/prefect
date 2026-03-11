"""往来对账 - 数据采集 & 写库 Tasks

阶段1：从 MySQL + 共享盘 Excel 采集原始数据，先删除目标月旧数据，再写入 PostgreSQL。
移植自 FastAPI 项目 recon_tool.py，改为同步版本，依赖 mypackage。
"""
import platform
import sys
import os
from datetime import date, timedelta
from typing import Optional, Dict, Any, Tuple

import pandas as pd
from prefect import task

# 添加 prefect 根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


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
# Task 3：删除目标月旧数据
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
            try:
                count_result = conn.execute(
                    text("SELECT count(*) FROM excel_account_recon WHERE date = :d"),
                    {"d": lastmonth}
                )
                count = count_result.scalar()
                print(f"--> 准备删除 {lastmonth_str} 旧数据，现有 {count} 条")

                conn.execute(
                    text("DELETE FROM excel_account_recon WHERE date = :d"),
                    {"d": lastmonth}
                )
                trans.commit()
                print(f"--> 删除 {lastmonth_str} 旧数据完成，共 {count} 条")
                return {"success": True, "message": f"成功删除 {lastmonth_str} 旧数据 {count} 条"}
            except Exception:
                trans.rollback()
                raise
    except Exception as e:
        print(f"[ERROR] 删除旧数据失败: {e}")
        return {"success": False, "error": str(e)}


# ──────────────────────────────────────────────
# Task 4：合并 MySQL + Excel 并写入 PostgreSQL
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
            print(f"[INFO] 已过滤 {before_count - len(df_combined)} 条 major_cat 为空的无效行，剩余 {len(df_combined)} 条")

        # 数据预处理
        df = df_combined.copy()
        if "amt" in df.columns:
            df["amt"] = pd.to_numeric(df["amt"], errors="coerce").fillna(0)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").fillna(pd.Timestamp("1900-01-01"))

        string_cols = ["major_cat", "co_abbr", "prim_subj", "class",
                       "cp_abbr", "content", "remarks", "resp_person"]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).replace("nan", "").replace("None", "")

        # 剔除 major_cat（大类）为空的脏数据行，这类行是 Excel 末尾空行被读进来的
        before_count = len(df)
        df = df[df["major_cat"].str.strip().astype(bool)].copy()
        if len(df) < before_count:
            print(f"[INFO] 已过滤 {before_count - len(df)} 条 major_cat 为空的无效行，剩余 {len(df)} 条")

        # 写入数据库
        engine = engine_to_db()
        df.to_sql("excel_account_recon", con=engine, if_exists="append", index=False)
        print(f"--> 成功写入 excel_account_recon，共 {len(df)} 条记录")
        return {"success": True, "message": f"写入完成", "count": len(df)}

    except Exception as e:
        print(f"[ERROR] 写入数据库失败: {e}")
        return {"success": False, "error": str(e), "count": 0}
