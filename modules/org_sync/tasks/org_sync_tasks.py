"""组织架构同步对比 Tasks"""

import os
import sys
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from prefect import task

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _build_hierarchy_paths(df: pd.DataFrame) -> Dict[str, list]:
    """基于 ParentID 构建每个组织的完整路径（从 root 到该节点）"""
    id_to_parent = dict(zip(df["fone_id"].astype(str), df["parent_id"].astype(str)))
    id_to_name = dict(zip(df["fone_id"].astype(str), df["fone_name"]))

    paths: Dict[str, list] = {}
    for org_id in id_to_name:
        path = []
        current = org_id
        visited = set()
        while current and current not in visited:
            visited.add(current)
            name = id_to_name.get(current, "")
            if name and str(name).strip() and str(name).strip() != "0":
                path.append(str(name).strip())
            parent = id_to_parent.get(current)
            if not parent or parent == "root" or parent == "0":
                break
            current = parent
        path.reverse()
        paths[org_id] = path
    return paths


def _get_level_from_path(path: list, level: int) -> str:
    """从路径中提取指定层级（level=1 为 root 下一级，排除 root 本身）"""
    # path[0] 通常是 root（新国都/抵销及其他），从 path[1] 开始算 1 级
    idx = level  # path[1] = 1级, path[2] = 2级, path[3] = 3级
    if idx < len(path):
        return path[idx]
    return ""


def _replace_benbu(val: str) -> str:
    """将名称以'本部'结尾的替换为'公共部门'"""
    if val and str(val).strip().endswith("本部"):
        return "公共部门"
    return str(val).strip() if val else ""


def _build_fone_path(row: pd.Series) -> str:
    """用 FONE 的各级组织拼接成带 '-' 的路径（保留用于展示）"""
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


def _build_db_corr_rel_from_path(row: pd.Series) -> str:
    """基于 ParentID 还原的 1/2/3 级组织拼接成 db_corr_rel 格式"""
    parts = []
    for col in ["lvl1_adj", "lvl2_adj", "lvl3_adj"]:
        val = row.get(col)
        if val and str(val).strip() and str(val).strip() != "0":
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
    对比 FONE 和 map_org，只找出新增 ID，并基于 ParentID 还原 1/2/3 级路径与 db_corr_rel 匹配。

    Returns:
        dict 包含以下 DataFrame:
        - added: FONE 有、map_org 无（新增），含 suggested_db_corr_rel / matched_db_corr_rel
        - summary: 汇总统计
    """
    # 预处理 FONE 数据
    df_fone = df_fone.copy()
    # 基于 ParentID 还原真实层级路径
    hierarchy_paths = _build_hierarchy_paths(df_fone)
    df_fone["hierarchy_path"] = (
        df_fone["fone_id"].astype(str).map(lambda x: hierarchy_paths.get(x, []))
    )
    df_fone["lvl1"] = (
        df_fone["fone_id"]
        .astype(str)
        .map(lambda x: _get_level_from_path(hierarchy_paths.get(x, []), 1))
    )
    df_fone["lvl2"] = (
        df_fone["fone_id"]
        .astype(str)
        .map(lambda x: _get_level_from_path(hierarchy_paths.get(x, []), 2))
    )
    df_fone["lvl3"] = (
        df_fone["fone_id"]
        .astype(str)
        .map(lambda x: _get_level_from_path(hierarchy_paths.get(x, []), 3))
    )
    # 将 "本部" 替换为 "公共部门"
    df_fone["lvl1_adj"] = df_fone["lvl1"].apply(_replace_benbu)
    df_fone["lvl2_adj"] = df_fone["lvl2"].apply(_replace_benbu)
    df_fone["lvl3_adj"] = df_fone["lvl3"].apply(_replace_benbu)
    # 组装建议的 db_corr_rel
    df_fone["suggested_db_corr_rel"] = (
        df_fone["lvl1_adj"].fillna("")
        + "-"
        + df_fone["lvl2_adj"].fillna("")
        + "-"
        + df_fone["lvl3_adj"].fillna("")
    ).str.strip("-")
    # 保留旧路径供展示
    df_fone["fone_path"] = df_fone.apply(_build_fone_path, axis=1)

    if only_last_stage:
        df_fone_active = df_fone[df_fone["LastStage"] == "是"].copy()
        print(f"筛选 LastStage='是' 后剩余 {len(df_fone_active)} 条记录")
    else:
        df_fone_active = df_fone.copy()

    # 预处理 map_org 数据
    df_map = df_map.copy()
    df_map["identifier_id"] = df_map["identifier_id"].astype(str).str.strip()
    df_map["db_corr_rel"] = df_map["db_corr_rel"].fillna("").astype(str).str.strip()

    # 排除系统级虚拟组织
    exclude_ids = {"DEFAULT", "ENONE"}
    df_fone_active = df_fone_active[~df_fone_active["fone_id"].astype(str).isin(exclude_ids)].copy()

    # 构建集合
    fone_ids = set(df_fone_active["fone_id"].astype(str).str.strip())
    map_ids = set(df_map["identifier_id"])

    # ========== 只核对新增 ID ==========
    added_ids = fone_ids - map_ids
    df_added = df_fone_active[df_fone_active["fone_id"].astype(str).isin(added_ids)].copy()
    print(f"新增组织: {len(df_added)} 个")

    # 尝试与 map_org.db_corr_rel 匹配
    db_corr_rels = set(df_map["db_corr_rel"].dropna().astype(str))

    def _fuzzy_match_db_corr_rel(suggested: str) -> Tuple[str, str]:
        if not suggested:
            return "", "无建议"
        if suggested in db_corr_rels:
            return suggested, "精确匹配"
        parts = suggested.split("-")
        if len(parts) >= 2:
            prefix = "-".join(parts[:2])
            candidates = [d for d in db_corr_rels if d.startswith(prefix)]
            if len(candidates) == 1:
                return candidates[0], "前缀模糊匹配"
            elif len(candidates) > 1:
                return candidates[0], f"多候选({len(candidates)}个)"
        return "", "未匹配"

    matches = df_added["suggested_db_corr_rel"].apply(_fuzzy_match_db_corr_rel)
    df_added["matched_db_corr_rel"] = matches.apply(lambda x: x[0])
    df_added["match_status"] = matches.apply(lambda x: x[1])

    # 汇总统计
    summary = pd.DataFrame(
        {
            "差异类型": ["新增", "合计(FONE)", "合计(map_org)"],
            "数量": [
                len(df_added),
                len(df_fone_active),
                len(df_map),
            ],
        }
    )

    return {
        "added": df_added,
        "modified": pd.DataFrame(),
        "removed": pd.DataFrame(),
        "unchanged": pd.DataFrame(),
        "summary": summary,
    }


@task(name="generate_org_diff_report", log_prints=True)
def generate_org_diff_report_task(
    diff_result: Dict[str, Any],
    output_dir: Optional[str] = None,
) -> str:
    """生成 Excel 差异报告（仅新增）"""
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
                "lvl1",
                "lvl2",
                "lvl3",
                "suggested_db_corr_rel",
                "matched_db_corr_rel",
                "match_status",
                "Level",
                "BusinessLine",
                "LastStage",
                "FONE_SYN_Time",
            ]
            available_cols = [c for c in cols if c in diff_result["added"].columns]
            diff_result["added"][available_cols].to_excel(writer, sheet_name="新增组织", index=False)

    print(f"差异报告已生成: {output_path}")
    return output_path


@task(name="build_db_corr_rel_mapping", log_prints=True)
def build_db_corr_rel_mapping_task(
    df_fone: pd.DataFrame,
    df_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    生成 FONE 组织 -> db_corr_rel 的映射建议表
    基于 ParentID 还原的 1/2/3 级路径与 map_org.db_corr_rel 匹配
    """
    df_fone = df_fone.copy()
    hierarchy_paths = _build_hierarchy_paths(df_fone)
    df_fone["hierarchy_path"] = (
        df_fone["fone_id"].astype(str).map(lambda x: hierarchy_paths.get(x, []))
    )
    df_fone["lvl1"] = (
        df_fone["fone_id"]
        .astype(str)
        .map(lambda x: _get_level_from_path(hierarchy_paths.get(x, []), 1))
    )
    df_fone["lvl2"] = (
        df_fone["fone_id"]
        .astype(str)
        .map(lambda x: _get_level_from_path(hierarchy_paths.get(x, []), 2))
    )
    df_fone["lvl3"] = (
        df_fone["fone_id"]
        .astype(str)
        .map(lambda x: _get_level_from_path(hierarchy_paths.get(x, []), 3))
    )
    df_fone["lvl1_adj"] = df_fone["lvl1"].apply(_replace_benbu)
    df_fone["lvl2_adj"] = df_fone["lvl2"].apply(_replace_benbu)
    df_fone["lvl3_adj"] = df_fone["lvl3"].apply(_replace_benbu)
    df_fone["suggested_db_corr_rel"] = (
        df_fone["lvl1_adj"].fillna("")
        + "-"
        + df_fone["lvl2_adj"].fillna("")
        + "-"
        + df_fone["lvl3_adj"].fillna("")
    ).str.strip("-")

    db_corr_rels = df_map["db_corr_rel"].dropna().astype(str).tolist()

    def _match(suggested: str) -> Tuple[str, str]:
        if not suggested:
            return "", "无建议"
        if suggested in db_corr_rels:
            return suggested, "精确匹配"
        parts = suggested.split("-")
        if len(parts) >= 2:
            prefix = "-".join(parts[:2])
            candidates = [d for d in db_corr_rels if d.startswith(prefix)]
            if len(candidates) == 1:
                return candidates[0], "前缀模糊匹配"
            elif len(candidates) > 1:
                return candidates[0], f"多候选({len(candidates)}个)"
        return "", "未匹配"

    results = df_fone["suggested_db_corr_rel"].apply(_match)
    df_fone["matched_db_corr_rel"] = results.apply(lambda x: x[0])
    df_fone["match_status"] = results.apply(lambda x: x[1])

    cols = [
        "fone_id",
        "fone_name",
        "lvl1",
        "lvl2",
        "lvl3",
        "suggested_db_corr_rel",
        "matched_db_corr_rel",
        "match_status",
        "Level",
        "BusinessLine",
        "LastStage",
    ]
    available_cols = [c for c in cols if c in df_fone.columns]
    df_result = df_fone[available_cols].copy()

    unmatched = df_result[df_result["match_status"].isin(["未匹配", "无建议"])]
    print(
        f"db_corr_rel 映射分析: 总计 {len(df_result)}, 精确匹配 {len(df_result[df_result['match_status']=='精确匹配'])}, 需关注 {len(unmatched)}"
    )

    return df_result
