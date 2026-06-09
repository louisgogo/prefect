"""组织架构同步对比 Tasks"""
import os
import sys
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from prefect import task

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _build_fone_path(row: pd.Series) -> str:
    """用 FONE 的各级组织拼接成带 '-' 的路径"""
    parts = []
    for col in [
        "PrimaryOrganization",
        "SecondaryOrganization",
        "TertiaryOrganization",
        "FourthOrganization",
    ]:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() and str(val).strip() != "0":
            parts.append(str(val).strip())
    return "-".join(parts) if parts else ""


def _build_unique_lvl_from_fone(row: pd.Series) -> str:
    """用 FONE 的前三级组织拼接成 unique_lvl 格式（供匹配 dim_org_struc 用）"""
    parts = []
    for col in ["PrimaryOrganization", "SecondaryOrganization", "TertiaryOrganization"]:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() and str(val).strip() != "0":
            parts.append(str(val).strip())
    return "-".join(parts) if parts else ""


@task(name="fetch_fone_org", log_prints=True)
def fetch_fone_org_task() -> pd.DataFrame:
    """从 FONE MySQL 获取 XGD_MRPT_ENTITY 最新组织架构"""
    from mypackage.utilities import engine_to_mysql

    engine = engine_to_mysql()
    sql = """
        SELECT
            ID          AS fone_id,
            Name        AS fone_name,
            ParentID    AS parent_id,
            Attribute,
            PrimaryOrganization,
            SecondaryOrganization,
            TertiaryOrganization,
            FourthOrganization,
            Level,
            FullName,
            BusinessLine,
            LastStage,
            FONE_SYN_Time
        FROM XGD_MRPT_ENTITY
        WHERE FONE_SYN_Time = (
            SELECT MAX(FONE_SYN_Time) FROM XGD_MRPT_ENTITY
        )
        ORDER BY ID
    """
    df = pd.read_sql(text(sql), con=engine)
    print(f"从 FONE 获取到 {len(df)} 条组织架构记录")
    return df


@task(name="fetch_map_org", log_prints=True)
def fetch_map_org_task() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """从 mydb 获取 map_org 和 dim_org_struc"""
    from mypackage.utilities import engine_to_db

    engine = engine_to_db()

    df_map = pd.read_sql(
        text("SELECT * FROM map_org ORDER BY identifier_id"),
        con=engine,
    )
    print(f"从 mydb 获取到 map_org {len(df_map)} 条记录")

    df_dim = pd.read_sql(
        text(
            "SELECT unique_lvl, prim_org, sec_org, third_org, bus_line, category FROM dim_org_struc ORDER BY unique_lvl"
        ),
        con=engine,
    )
    print(f"从 mydb 获取到 dim_org_struc {len(df_dim)} 条记录")

    return df_map, df_dim


@task(name="compare_org", log_prints=True)
def compare_org_task(
    df_fone: pd.DataFrame,
    df_map: pd.DataFrame,
    df_dim: pd.DataFrame,
    only_last_stage: bool = True,
) -> Dict[str, Any]:
    """
    对比 FONE 和 map_org，找出差异

    Returns:
        dict 包含以下 DataFrame:
        - added: FONE 有、map_org 无（新增）
        - modified: ID 相同但层级路径不一致
        - removed: map_org 有、FONE 无（停用/删除）
        - unchanged: 完全一致
        - summary: 汇总统计
    """
    # 预处理 FONE 数据
    df_fone = df_fone.copy()
    df_fone["fone_path"] = df_fone.apply(_build_fone_path, axis=1)
    df_fone["suggested_unique_lvl"] = df_fone.apply(_build_unique_lvl_from_fone, axis=1)

    if only_last_stage:
        df_fone_active = df_fone[df_fone["LastStage"] == "是"].copy()
        print(f"筛选 LastStage='是' 后剩余 {len(df_fone_active)} 条记录")
    else:
        df_fone_active = df_fone.copy()

    # 预处理 map_org 数据
    df_map = df_map.copy()
    df_map["identifier_id"] = df_map["identifier_id"].astype(str).str.strip()
    df_map["db_corr_rel"] = df_map["db_corr_rel"].fillna("").astype(str).str.strip()

    # 构建集合
    fone_ids = set(df_fone_active["fone_id"].astype(str).str.strip())
    map_ids = set(df_map["identifier_id"])

    # ========== 1. 新增 ==========
    added_ids = fone_ids - map_ids
    df_added = df_fone_active[df_fone_active["fone_id"].astype(str).isin(added_ids)].copy()
    # 尝试自动匹配 unique_lvl
    dim_unique_lvls = set(df_dim["unique_lvl"].dropna().astype(str))
    df_added["matched_unique_lvl"] = df_added["suggested_unique_lvl"].apply(
        lambda x: x if x in dim_unique_lvls else ""
    )

    # 如果没精确匹配，尝试前缀模糊匹配
    def _fuzzy_match_unique_lvl(suggested: str) -> str:
        if not suggested:
            return ""
        if suggested in dim_unique_lvls:
            return suggested
        # 尝试用前两級匹配
        parts = suggested.split("-")
        if len(parts) >= 2:
            prefix = "-".join(parts[:2])
            candidates = [u for u in dim_unique_lvls if u.startswith(prefix)]
            if len(candidates) == 1:
                return candidates[0]
        return "【需人工确认】"

    df_added["matched_unique_lvl"] = df_added["suggested_unique_lvl"].apply(_fuzzy_match_unique_lvl)
    print(f"新增组织: {len(df_added)} 个")

    # ========== 2. 停用/删除 ==========
    removed_ids = map_ids - fone_ids
    df_removed = df_map[df_map["identifier_id"].isin(removed_ids)].copy()
    print(f"停用/缺失组织: {len(df_removed)} 个")

    # ========== 3. 变更（ID 相同但路径不同）==========
    common_ids = fone_ids & map_ids
    df_fone_common = df_fone_active[df_fone_active["fone_id"].astype(str).isin(common_ids)].copy()
    df_map_common = df_map[df_map["identifier_id"].isin(common_ids)].copy()

    # merge 对比
    df_fone_common["fone_id"] = df_fone_common["fone_id"].astype(str).str.strip()
    df_compare = df_fone_common.merge(
        df_map_common[["identifier_id", "db_corr_rel", "unique_lvl"]],
        left_on="fone_id",
        right_on="identifier_id",
        how="left",
    )
    # 判断路径是否一致（忽略空格差异）
    df_compare["path_match"] = df_compare.apply(
        lambda r: str(r["fone_path"]).replace(" ", "") == str(r["db_corr_rel"]).replace(" ", ""),
        axis=1,
    )
    df_modified = df_compare[~df_compare["path_match"]].copy()
    print(f"层级变更组织: {len(df_modified)} 个")

    # ========== 4. 完全一致 ==========
    df_unchanged = df_compare[df_compare["path_match"]].copy()
    print(f"完全一致组织: {len(df_unchanged)} 个")

    # ========== 汇总统计 ==========
    summary = pd.DataFrame(
        {
            "差异类型": ["新增", "层级变更", "停用/缺失", "完全一致", "合计(FONE)", "合计(map_org)"],
            "数量": [
                len(df_added),
                len(df_modified),
                len(df_removed),
                len(df_unchanged),
                len(df_fone_active),
                len(df_map),
            ],
        }
    )

    return {
        "added": df_added,
        "modified": df_modified,
        "removed": df_removed,
        "unchanged": df_unchanged,
        "summary": summary,
    }


@task(name="generate_org_diff_report", log_prints=True)
def generate_org_diff_report_task(
    diff_result: Dict[str, Any],
    output_dir: Optional[str] = None,
) -> str:
    """生成 Excel 差异报告"""
    from datetime import datetime

    if output_dir is None:
        output_dir = os.getcwd()
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"org_diff_report_{timestamp}.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # 汇总
        diff_result["summary"].to_excel(writer, sheet_name="汇总", index=False)

        # 新增
        if len(diff_result["added"]) > 0:
            cols = [
                "fone_id",
                "fone_name",
                "fone_path",
                "suggested_unique_lvl",
                "matched_unique_lvl",
                "PrimaryOrganization",
                "SecondaryOrganization",
                "TertiaryOrganization",
                "FourthOrganization",
                "Level",
                "BusinessLine",
                "LastStage",
                "FONE_SYN_Time",
            ]
            available_cols = [c for c in cols if c in diff_result["added"].columns]
            diff_result["added"][available_cols].to_excel(writer, sheet_name="新增组织", index=False)

        # 变更
        if len(diff_result["modified"]) > 0:
            cols = [
                "identifier_id",
                "fone_name",
                "fone_path",
                "db_corr_rel",
                "unique_lvl",
                "PrimaryOrganization",
                "SecondaryOrganization",
                "TertiaryOrganization",
                "FourthOrganization",
                "Level",
                "BusinessLine",
                "LastStage",
                "FONE_SYN_Time",
            ]
            available_cols = [c for c in cols if c in diff_result["modified"].columns]
            diff_result["modified"][available_cols].to_excel(writer, sheet_name="层级变更", index=False)

        # 停用/缺失
        if len(diff_result["removed"]) > 0:
            diff_result["removed"][
                [
                    "identifier_id",
                    "unique_lvl",
                    "prim_org",
                    "sec_org",
                    "third_org",
                    "fourth_org",
                    "db_corr_rel",
                ]
            ].to_excel(writer, sheet_name="停用或缺失", index=False)

    print(f"差异报告已生成: {output_path}")
    return output_path


@task(name="save_org_diff_to_db", log_prints=True)
def save_org_diff_to_db_task(diff_result: Dict[str, Any]) -> None:
    """将差异写入 mydb.org_diff_log（如果不存在则创建）"""
    from mypackage.utilities import engine_to_db
    from sqlalchemy import text

    engine = engine_to_db()

    # 建表（如果不存在）
    create_sql = """
    CREATE TABLE IF NOT EXISTS org_diff_log (
        id SERIAL PRIMARY KEY,
        diff_type VARCHAR(20) NOT NULL,
        identifier_id TEXT,
        fone_name TEXT,
        fone_path TEXT,
        db_corr_rel TEXT,
        unique_lvl TEXT,
        suggested_unique_lvl TEXT,
        matched_unique_lvl TEXT,
        primary_org TEXT,
        secondary_org TEXT,
        tertiary_org TEXT,
        fourth_org TEXT,
        business_line TEXT,
        fone_level TEXT,
        last_stage TEXT,
        fone_syn_time TIMESTAMP,
        checked BOOLEAN DEFAULT FALSE,
        checked_by TEXT,
        checked_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        # 清空当天的旧记录（可选：只保留最新一次运行的结果）
        conn.execute(text("DELETE FROM org_diff_log WHERE created_at::date = CURRENT_DATE"))

    # 组装写入数据
    rows = []
    now = pd.Timestamp.now()

    for _, r in diff_result["added"].iterrows():
        rows.append(
            {
                "diff_type": "新增",
                "identifier_id": str(r.get("fone_id", "")),
                "fone_name": str(r.get("fone_name", "")),
                "fone_path": str(r.get("fone_path", "")),
                "suggested_unique_lvl": str(r.get("suggested_unique_lvl", "")),
                "matched_unique_lvl": str(r.get("matched_unique_lvl", "")),
                "primary_org": str(r.get("PrimaryOrganization", "")),
                "secondary_org": str(r.get("SecondaryOrganization", "")),
                "tertiary_org": str(r.get("TertiaryOrganization", "")),
                "fourth_org": str(r.get("FourthOrganization", "")),
                "business_line": str(r.get("BusinessLine", "")),
                "fone_level": str(r.get("Level", "")),
                "last_stage": str(r.get("LastStage", "")),
                "fone_syn_time": r.get("FONE_SYN_Time"),
                "created_at": now,
            }
        )

    for _, r in diff_result["modified"].iterrows():
        rows.append(
            {
                "diff_type": "层级变更",
                "identifier_id": str(r.get("identifier_id", "")),
                "fone_name": str(r.get("fone_name", "")),
                "fone_path": str(r.get("fone_path", "")),
                "db_corr_rel": str(r.get("db_corr_rel", "")),
                "unique_lvl": str(r.get("unique_lvl", "")),
                "primary_org": str(r.get("PrimaryOrganization", "")),
                "secondary_org": str(r.get("SecondaryOrganization", "")),
                "tertiary_org": str(r.get("TertiaryOrganization", "")),
                "fourth_org": str(r.get("FourthOrganization", "")),
                "business_line": str(r.get("BusinessLine", "")),
                "fone_level": str(r.get("Level", "")),
                "last_stage": str(r.get("LastStage", "")),
                "fone_syn_time": r.get("FONE_SYN_Time"),
                "created_at": now,
            }
        )

    for _, r in diff_result["removed"].iterrows():
        rows.append(
            {
                "diff_type": "停用或缺失",
                "identifier_id": str(r.get("identifier_id", "")),
                "db_corr_rel": str(r.get("db_corr_rel", "")),
                "unique_lvl": str(r.get("unique_lvl", "")),
                "primary_org": str(r.get("prim_org", "")),
                "secondary_org": str(r.get("sec_org", "")),
                "tertiary_org": str(r.get("third_org", "")),
                "fourth_org": str(r.get("fourth_org", "")),
                "created_at": now,
            }
        )

    if rows:
        df_write = pd.DataFrame(rows)
        df_write.to_sql("org_diff_log", con=engine, if_exists="append", index=False)
        print(f"已写入 org_diff_log {len(rows)} 条差异记录")
    else:
        print("今日无差异，未写入数据")


@task(name="build_unique_lvl_mapping", log_prints=True)
def build_unique_lvl_mapping_task(
    df_fone: pd.DataFrame,
    df_dim: pd.DataFrame,
) -> pd.DataFrame:
    """
    生成 FONE 组织 -> unique_lvl 的映射建议表
    用于批量确认和维护 map_org 的 unique_lvl 字段
    """
    df_fone = df_fone.copy()
    df_fone["suggested_unique_lvl"] = df_fone.apply(_build_unique_lvl_from_fone, axis=1)

    dim_unique_lvls = df_dim["unique_lvl"].dropna().astype(str).tolist()

    def _match(suggested: str) -> Tuple[str, str]:
        if not suggested:
            return "", "无建议"
        if suggested in dim_unique_lvls:
            return suggested, "精确匹配"
        parts = suggested.split("-")
        if len(parts) >= 2:
            prefix = "-".join(parts[:2])
            candidates = [u for u in dim_unique_lvls if u.startswith(prefix)]
            if len(candidates) == 1:
                return candidates[0], "前缀模糊匹配"
            elif len(candidates) > 1:
                return candidates[0], f"多候选({len(candidates)}个)"
        return "", "未匹配"

    results = df_fone["suggested_unique_lvl"].apply(_match)
    df_fone["matched_unique_lvl"] = results.apply(lambda x: x[0])
    df_fone["match_status"] = results.apply(lambda x: x[1])

    cols = [
        "fone_id",
        "fone_name",
        "suggested_unique_lvl",
        "matched_unique_lvl",
        "match_status",
        "PrimaryOrganization",
        "SecondaryOrganization",
        "TertiaryOrganization",
        "FourthOrganization",
        "Level",
        "BusinessLine",
        "LastStage",
    ]
    available_cols = [c for c in cols if c in df_fone.columns]
    df_result = df_fone[available_cols].copy()

    unmatched = df_result[df_result["match_status"].isin(["未匹配", "无建议"])]
    print(
        f"unique_lvl 映射分析: 总计 {len(df_result)}, 精确匹配 {len(df_result[df_result['match_status']=='精确匹配'])}, 需关注 {len(unmatched)}"
    )

    return df_result
