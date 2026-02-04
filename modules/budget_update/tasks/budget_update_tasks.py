"""预算更新相关 Tasks：FONE 取数、严格映射检查、清洗、写库"""
from prefect import task
from typing import Optional, Dict, Any
import pandas as pd
import sys
import os

# 添加根目录到路径（prefect 目录）
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mypackage.utilities import connect_to_db, connect_to_fone, update_report_data
from mypackage.mapping import reverse_combined_column_mapping, combined_column_mapping

# 流水映射表（产品名称 -> 编码）
MAP_AMO = {
    "POS+": "1001",
    "立刷": "2001",
    "立刷电签版": "2101",
    "立刷商户版": "2004",
    "立刷微电签": "2103",
    "全球收单业务": "3001",
    "全球收款业务": "3002",
}


def _read_data(table_name: str, db_type: str = "FONE") -> pd.DataFrame:
    """从 FONE 或 PSQL 读取表数据（内部辅助，非 task）。"""
    if db_type == "FONE":
        conn, cur = connect_to_fone()
    elif db_type == "PSQL":
        conn, cur = connect_to_db()
    else:
        raise ValueError("Unsupported database. Please choose 'FONE' or 'PSQL'.")
    try:
        query = f"SELECT * FROM {table_name}"
        cur.execute(query)
        result = cur.fetchall()
        if result:
            if db_type == "PSQL":
                df = pd.DataFrame(
                    result,
                    columns=[
                        reverse_combined_column_mapping.get(desc[0])
                        for desc in cur.description
                    ],
                )
            else:
                df = pd.DataFrame(result, columns=[desc[0] for desc in cur.description])
            return df
        return pd.DataFrame()
    finally:
        cur.close()
        conn.close()


def _check_mapping_strict(
    df: pd.DataFrame,
    output_dir: str,
    *,
    cust_col: Optional[str] = None,
    dev_col: Optional[str] = None,
    exp_col: Optional[str] = None,
    ind_col: Optional[str] = None,
    org_col: Optional[str] = None,
    prod_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    严格映射检查：任一维度存在未映射则先导出 CSV 再 raise，中断执行。
    """
    if cust_col:
        map_cust = _read_data("map_cust", "PSQL")
        df = df.merge(
            map_cust[["标识", "客户类别", "区域"]],
            how="left",
            left_on=cust_col,
            right_on="标识",
        )
        unmapped = df[df["标识"].isna()]
        if not unmapped.empty:
            path = os.path.join(output_dir, "未映射的客户编码.csv")
            unmapped.to_csv(path, encoding="utf-8-sig")
            raise ValueError(
                f"存在未映射的客户编码，已导出至: {os.path.abspath(path)}，请维护映射后重试。"
            )
        df = df.drop(["标识"], axis=1)

    if dev_col:
        map_dev = _read_data("map_dev", "PSQL")
        map_dev = map_dev.rename({"项目名称": "研发项目名称"}, axis=1)
        df = df.merge(
            map_dev[["编码", "研发项目名称"]],
            how="left",
            left_on=dev_col,
            right_on="编码",
        )
        unmapped = df[df["编码"].isna()]
        if not unmapped.empty:
            path = os.path.join(output_dir, "未映射的研发项目编码.csv")
            unmapped.to_csv(path, encoding="utf-8-sig")
            raise ValueError(
                f"存在未映射的研发项目编码，已导出至: {os.path.abspath(path)}，请维护映射后重试。"
            )
        df = df.drop(["编码"], axis=1)

    if exp_col:
        map_exp = _read_data("map_exp", "PSQL")
        df = df.merge(
            map_exp[
                [
                    "唯一费用标识",
                    "费用项目编码",
                    "费用项目名称",
                    "损益类科目编码",
                    "损益类科目名称",
                ]
            ],
            how="left",
            left_on=exp_col,
            right_on="唯一费用标识",
        )
        unmapped = df[df["唯一费用标识"].isna()]
        if not unmapped.empty:
            path = os.path.join(output_dir, "未映射的预算费用项目编码.csv")
            unmapped.to_csv(path, encoding="utf-8-sig")
            raise ValueError(
                f"存在未映射的预算费用项目编码，已导出至: {os.path.abspath(path)}，请维护映射后重试。"
            )
        df = df.drop(["唯一费用标识"], axis=1)

    if ind_col:
        map_ind = _read_data("map_ind", "PSQL")
        df = df.merge(
            map_ind[["标识(ID)", "一级科目", "收入大类", "费用大类"]],
            how="left",
            left_on=ind_col,
            right_on="标识(ID)",
        )
        unmapped = df[df["标识(ID)"].isna()]
        if not unmapped.empty:
            path = os.path.join(output_dir, "未映射的指标编码.csv")
            unmapped.to_csv(path, encoding="utf-8-sig")
            raise ValueError(
                f"存在未映射的指标编码，已导出至: {os.path.abspath(path)}，请维护映射后重试。"
            )
        df = df.drop(["标识(ID)"], axis=1)

    if org_col:
        map_org = _read_data("map_org", "PSQL")
        df = df.merge(
            map_org[["标识(ID)", "数据库对照关系"]],
            how="left",
            left_on=org_col,
            right_on="标识(ID)",
        )
        unmapped = df[df["数据库对照关系"].isna()]
        if not unmapped.empty:
            path = os.path.join(output_dir, "未映射的组织编码.csv")
            unmapped.to_csv(path, encoding="utf-8-sig")
            raise ValueError(
                f"存在未映射的组织编码，已导出至: {os.path.abspath(path)}，请维护映射后重试。"
            )
        df = df.drop(["标识(ID)"], axis=1)

    if prod_col:
        map_prod = _read_data("map_prod", "PSQL")
        df = df.merge(
            map_prod[["标识", "产品映射"]],
            how="left",
            left_on=prod_col,
            right_on="标识",
        )
        unmapped = df[df["标识"].isna()]
        if not unmapped.empty:
            path = os.path.join(output_dir, "未映射的产品编码.csv")
            unmapped.to_csv(path, encoding="utf-8-sig")
            raise ValueError(
                f"存在未映射的产品编码，已导出至: {os.path.abspath(path)}，请维护映射后重试。"
            )
        df = df.drop(["标识"], axis=1)

    return df


@task(name="fetch_fone_budget_data", log_prints=True)
def fetch_fone_budget_data_task(
    budget_year: str,
    fone_version: str,
    output_dir: str = ".",
) -> Dict[str, Any]:
    """
    从 FONE 拉取预算原始表并过滤，返回 fone_exp, fone_emp, fone_biz, fone_amo, fone_pro, fone_shared_rate 及 map_ind。
    """
    try:
        fone_exp = _read_data("Fone2BI_Budget_FY", "FONE")
        fone_exp["资产类型"] = fone_exp["资产类型"].fillna("默认成员").replace("", "默认成员")
        fone_exp["费用标识"] = (
            fone_exp["部门属性"]
            + "-"
            + fone_exp["资产类型"]
            + "-"
            + fone_exp["预算科目编码"]
        )
        fone_exp["日期"] = (
            fone_exp["年度"].astype(str)
            + "-"
            + fone_exp["期间"].str.replace("月份", "")
            + "-1"
        )
        fone_exp["业务线名称"] = fone_exp["业务线名称"].replace("待分摊业务线", "抵销数")
        fone_exp = fone_exp[fone_exp["年度"] == budget_year]
        fone_exp = fone_exp[fone_exp["版本编码"] == fone_version]

        fone_emp = _read_data("Fone2BI_Budget_EMP", "FONE")
        fone_emp["日期"] = (
            fone_emp["年度"].astype(str)
            + "-"
            + fone_emp["期间"].str.replace("月份", "")
            + "-1"
        )
        fone_emp = fone_emp[fone_emp["年度"] == budget_year]
        fone_emp = fone_emp[fone_emp["版本编码"] == fone_version]

        fone_biz = _read_data("Fone2BI_Budget_BIZ", "FONE")
        fone_biz["日期"] = (
            fone_biz["年度"].astype(str)
            + "-"
            + fone_biz["期间"].str.replace("月份", "")
            + "-1"
        )
        fone_biz["业务线名称"] = fone_biz["业务线名称"].replace("待分摊业务线", "抵销数")
        fone_biz = fone_biz[fone_biz["年度"] == budget_year]
        fone_biz = fone_biz[fone_biz["版本编码"] == fone_version]

        fone_amo = _read_data("Fone2BI_Budget_Amount", "FONE")
        fone_amo["日期"] = (
            fone_amo["年度"].astype(str)
            + "-"
            + fone_amo["期间"].str.replace("月份", "")
            + "-1"
        )
        fone_amo["产品类型"] = fone_amo["产品名称"].map(MAP_AMO)
        fone_amo = fone_amo[fone_amo["预算科目编码"] == "BA0116"]
        fone_amo = fone_amo[fone_amo["年度"] == budget_year]
        fone_amo = fone_amo[fone_amo["版本编码"] == fone_version]

        fone_pro = _read_data("Fone2BI_Budget_Profit", "FONE")
        fone_pro["日期"] = (
            fone_pro["年度"].astype(str)
            + "-"
            + fone_pro["期间"].str.replace("月份", "")
            + "-1"
        )
        fone_pro["业务线名称"] = fone_pro["业务线名称"].replace("待分摊业务线", "抵销数")
        fone_pro = fone_pro[fone_pro["年度"] == budget_year]
        fone_pro = fone_pro[fone_pro["预算科目编码"] != "ARPT0122"]
        fone_pro = fone_pro[fone_pro["版本编码"] == fone_version]

        fone_shared_rate = _read_data("Fone2BI_Budget_Percent", "FONE")
        fone_shared_rate = fone_shared_rate[fone_shared_rate["YYYY"] == budget_year]
        fone_shared_rate = fone_shared_rate[fone_shared_rate["版本"] == fone_version]
        fone_shared_rate = fone_shared_rate.drop(
            ["序号", "id", "YYYY", "版本"], axis=1
        )
        fone_shared_rate = fone_shared_rate.melt(
            id_vars=["指标"], var_name="业务线", value_name="金额"
        )

        map_ind = _read_data("map_ind", "PSQL")

        print(
            f"FONE 预算数据拉取完成: 费用 {len(fone_exp)}, 人员 {len(fone_emp)}, 业务 {len(fone_biz)}, "
            f"流水 {len(fone_amo)}, 利润 {len(fone_pro)}, 综合比例 {len(fone_shared_rate)}"
        )
        return {
            "fone_exp": fone_exp,
            "fone_emp": fone_emp,
            "fone_biz": fone_biz,
            "fone_amo": fone_amo,
            "fone_pro": fone_pro,
            "fone_shared_rate": fone_shared_rate,
            "map_ind": map_ind,
        }
    except Exception as e:
        print(f"拉取 FONE 预算数据时发生错误: {str(e)}")
        raise


@task(name="process_expense_budget", log_prints=True)
def process_expense_budget_task(
    fone_exp: pd.DataFrame,
    version: str,
    output_dir: str = ".",
) -> pd.DataFrame:
    """费用预算：清洗 + 严格映射(组织、费用标识)，未映射则中断。"""
    try:
        df = _check_mapping_strict(
            fone_exp, output_dir, org_col="组织编码", exp_col="费用标识"
        )
        df = df[
            [
                "组织编码",
                "一级组织",
                "二级组织",
                "三级组织",
                "损益类科目编码",
                "损益类科目名称",
                "项目名称",
                "预算编制说明",
                "费用项目编码",
                "费用项目名称",
                "数值",
                "日期",
                "数据库对照关系",
                "业务线编码",
                "业务线名称",
                "费用标识",
            ]
        ]
        df = df.rename(
            columns={
                "组织编码": "标识",
                "损益类科目编码": "指标编码",
                "损益类科目名称": "一级科目",
                "项目名称": "项目",
                "预算编制说明": "费用说明",
                "费用项目名称": "核算项目",
                "数据库对照关系": "唯一层级",
                "数值": "预算系统金额",
                "业务线名称": "业务线",
            }
        )
        df["填报日期"] = version
        df["一级科目"] = df["一级科目"].str.slice(0, 4)
        df = df.drop(["费用标识"], axis=1)
        print(f"费用预算处理完成: {len(df)} 条")
        return df
    except Exception as e:
        print(f"费用预算处理时发生错误: {str(e)}")
        raise


@task(name="process_income_budget", log_prints=True)
def process_income_budget_task(
    fone_biz: pd.DataFrame,
    version: str,
    output_dir: str = ".",
) -> pd.DataFrame:
    """收入预算：清洗 + 严格映射(组织、指标、产品、客户)，未映射则中断。"""
    try:
        df = _check_mapping_strict(
            fone_biz,
            output_dir,
            org_col="组织编码",
            ind_col="预算科目编码",
            prod_col="产品编码",
            cust_col="客户群编码",
        )
        df = df[
            [
                "组织编码",
                "组织名称",
                "客户群编码",
                "客户群名称",
                "产品编码",
                "产品名称",
                "预算科目编码",
                "预算科目名称",
                "日期",
                "数值",
                "数据库对照关系",
                "区域",
                "客户类别",
                "产品映射",
                "收入大类",
                "业务线编码",
                "业务线名称",
            ]
        ]
        df = df.rename(
            columns={
                "组织编码": "组织标识",
                "组织名称": "组织",
                "客户群编码": "客户标识",
                "客户群名称": "客户群",
                "产品编码": "产品大类标识",
                "产品名称": "产品",
                "产品映射": "产品大类",
                "预算科目编码": "指标标识",
                "预算科目名称": "指标",
                "数值": "本月金额",
                "数据库对照关系": "唯一层级",
                "区域": "客户区域",
                "业务线名称": "业务线",
            }
        )
        df["填报日期"] = version
        df["一级科目"] = df["指标"].apply(
            lambda x: "营业收入" if "收入" in str(x) else "营业成本"
        )
        target_indicators = ["BA01050302", "BA01051404"]
        cond = df["指标标识"].isin(target_indicators)
        df.loc[cond, "本月金额"] = -df.loc[cond, "本月金额"]
        df.loc[cond, "一级科目"] = "营业成本"
        print(f"收入预算处理完成: {len(df)} 条")
        return df
    except Exception as e:
        print(f"收入预算处理时发生错误: {str(e)}")
        raise


@task(name="process_personnel_budget", log_prints=True)
def process_personnel_budget_task(
    fone_emp: pd.DataFrame,
    version: str,
    output_dir: str = ".",
) -> pd.DataFrame:
    """人数预算：清洗 + 严格映射(组织)，未映射则中断。"""
    try:
        df = _check_mapping_strict(fone_emp, output_dir, org_col="组织编码")
        df = df[
            [
                "组织编码",
                "组织名称",
                "部门全称",
                "月末人数",
                "日期",
                "数据库对照关系",
                "业务线编码",
                "业务线名称",
            ]
        ]
        df = df.rename(
            columns={
                "组织编码": "标识",
                "组织名称": "组织",
                "部门全称": "部门",
                "月末人数": "预算系统人数",
                "数据库对照关系": "唯一层级",
                "业务线名称": "业务线",
            }
        )
        df["填报日期"] = version
        print(f"人数预算处理完成: {len(df)} 条")
        return df
    except Exception as e:
        print(f"人数预算处理时发生错误: {str(e)}")
        raise


@task(name="process_cash_budget", log_prints=True)
def process_cash_budget_task(
    fone_amo: pd.DataFrame,
    version: str,
    map_amo: Optional[Dict[str, str]] = None,
    output_dir: str = ".",
) -> pd.DataFrame:
    """
    流水预算：清洗 + 产品名称 map_amo 映射；若存在未映射产品（产品类型 NaN）则导出 CSV 并 raise。
    """
    try:
        if map_amo is None:
            map_amo = MAP_AMO
        if "产品类型" not in fone_amo.columns:
            fone_amo = fone_amo.copy()
            fone_amo["产品类型"] = fone_amo["产品名称"].map(map_amo)
        unmapped = fone_amo[fone_amo["产品类型"].isna()]
        if not unmapped.empty:
            path = os.path.join(output_dir, "未映射的流水产品名称.csv")
            unmapped.to_csv(path, encoding="utf-8-sig")
            raise ValueError(
                f"流水预算存在未映射的产品名称，已导出至: {os.path.abspath(path)}，请维护 map_amo 后重试。"
            )
        df = fone_amo[
            ["组织名称", "产品名称", "预算编制说明", "预算数", "日期", "产品类型"]
        ].copy()
        df = df.rename(
            columns={
                "组织名称": "业务部门名称",
                "产品名称": "产品类型名称",
                "预算编制说明": "说明",
                "预算数": "金额",
            }
        )
        df["预算版本"] = version
        df["金额"] = df["金额"] * 10000
        print(f"流水预算处理完成: {len(df)} 条")
        return df
    except Exception as e:
        print(f"流水预算处理时发生错误: {str(e)}")
        raise


@task(name="process_profit_budget", log_prints=True)
def process_profit_budget_task(
    fone_pro: pd.DataFrame,
    version: str,
    output_dir: str = ".",
) -> pd.DataFrame:
    """利润预算：清洗 + 严格映射(组织、指标)，未映射则中断。"""
    try:
        df = _check_mapping_strict(
            fone_pro, output_dir, org_col="组织编码", ind_col="预算科目编码"
        )
        df = df[
            [
                "组织编码",
                "一级组织",
                "二级组织",
                "三级组织",
                "一级科目",
                "日期",
                "数据库对照关系",
                "数值",
                "业务线编码",
                "业务线名称",
                "预算科目编码",
            ]
        ]
        df = df.rename(
            columns={
                "组织编码": "标识",
                "业务线名称": "业务线",
                "数值": "预算系统金额",
                "数据库对照关系": "唯一层级",
            }
        )
        df["填报日期"] = version
        df = df.drop(["预算科目编码"], axis=1)
        print(f"利润预算处理完成: {len(df)} 条")
        return df
    except Exception as e:
        print(f"利润预算处理时发生错误: {str(e)}")
        raise


@task(name="process_shared_rate_budget", log_prints=True)
def process_shared_rate_budget_task(
    fone_shared_rate: pd.DataFrame,
    version: str,
) -> pd.DataFrame:
    """综合比例预算：仅清洗，无映射检查。"""
    try:
        df = fone_shared_rate.copy()
        df["填报日期"] = version
        print(f"综合比例预算处理完成: {len(df)} 条")
        return df
    except Exception as e:
        print(f"综合比例预算处理时发生错误: {str(e)}")
        raise


def _normalize_business_line(df: pd.DataFrame) -> pd.DataFrame:
    """业务线名称替换（能源硬件/政府消费券）。"""
    df = df.copy()
    if "业务线" in df.columns:
        df["业务线"] = df["业务线"].replace("能源硬件（禁用）", "能源硬件")
        df["业务线"] = df["业务线"].replace("政府消费券（禁用）", "政府消费券")
    return df


@task(name="write_budget_to_db", log_prints=True)
def write_budget_to_db_task(
    budget_type: str,
    report_date: pd.Timestamp,
    version: str,
    date_range_psql: pd.DatetimeIndex,
    date_range_fone: pd.DatetimeIndex,
    df_exp: pd.DataFrame,
    df_inc: pd.DataFrame,
    df_emp: pd.DataFrame,
    df_cash: pd.DataFrame,
    df_pro: pd.DataFrame,
    df_shared_rate: pd.DataFrame,
) -> None:
    """
    根据 budget_type 写库：年初预算直接写 6 张表；年中预算先取 PSQL 实际数/抵销数拼接后再写。
    """
    try:
        if budget_type == "年初预算":
            # 费用
            df = _normalize_business_line(df_exp)
            df.columns = [combined_column_mapping.get(c, c) for c in df.columns]
            update_report_data("bud_expense", df, "report_date", report_date)
            # 收入
            df = _normalize_business_line(df_inc)
            df = df.rename(columns={"客户群": "客户群名称"})
            df.columns = [combined_column_mapping.get(c, c) for c in df.columns]
            update_report_data("bud_income", df, "report_date", report_date)
            # 人员
            df = _normalize_business_line(df_emp)
            df.columns = [combined_column_mapping.get(c, c) for c in df.columns]
            update_report_data("bud_personnel", df, "report_date", report_date)
            # 利润
            df = _normalize_business_line(df_pro)
            df.columns = [combined_column_mapping.get(c, c) for c in df.columns]
            update_report_data("bud_profit", df, "report_date", report_date)
            # 流水
            df = df_cash.copy()
            df.columns = [combined_column_mapping.get(c, c) for c in df.columns]
            update_report_data("bud_cash_flow", df, "bud_version", report_date)
            # 综合比例
            df = _normalize_business_line(df_shared_rate)
            df.columns = [combined_column_mapping.get(c, c) for c in df.columns]
            update_report_data("bud_bus_shared_rate", df, "report_date", report_date)
            print("年初预算写库完成")
            return

        if budget_type == "年中预算":
            # 拉取 PSQL 实际数与抵销数
            psql_pro = _read_data("fact_bus_profit", "PSQL")
            psql_pro["填报日期"] = report_date
            psql_pro["会计期间"] = pd.to_datetime(psql_pro["会计期间"])
            psql_pro = psql_pro[psql_pro["会计期间"].isin(date_range_psql)]
            psql_pro = psql_pro.rename(
                columns={"会计期间": "日期", "金额": "预算系统金额"}
            )

            psql_exp = _read_data("fact_bus_expense", "PSQL")
            psql_exp["填报日期"] = report_date
            psql_exp["会计期间"] = pd.to_datetime(psql_exp["会计期间"])
            psql_exp = psql_exp[psql_exp["会计期间"].isin(date_range_psql)]
            psql_exp = psql_exp.drop(["日期", "唯一层级"], axis=1, errors="ignore")
            psql_exp = psql_exp.rename(
                columns={
                    "会计期间": "日期",
                    "费用金额": "预算系统金额",
                    "核算项目-费控": "核算项目",
                    "研发项目": "项目",
                    "摘要": "费用说明",
                    "来源层级": "唯一层级",
                }
            )

            psql_inc = _read_data("fact_bus_revenue", "PSQL")
            psql_inc["填报日期"] = report_date
            psql_inc["会计期间"] = pd.to_datetime(psql_inc["会计期间"])
            psql_inc = psql_inc[psql_inc["会计期间"].isin(date_range_psql)]
            psql_inc = psql_inc.drop(["唯一层级"], axis=1, errors="ignore")
            psql_inc = psql_inc.rename(
                columns={
                    "会计期间": "日期",
                    "客户群编码": "客户标识",
                    "客户群名称": "客户群",
                    "来源层级": "唯一层级",
                }
            )

            psql_cash = _read_data("T_JL_AREA_TRADE", "PSQL")
            psql_cash["填报日期"] = report_date
            psql_cash["日期"] = pd.to_datetime(
                psql_cash["统计月份"], format="%Y%m"
            ).dt.strftime("%Y-%m-01")
            psql_cash["日期"] = pd.to_datetime(psql_cash["日期"])
            psql_cash = psql_cash[psql_cash["日期"].isin(date_range_psql)]
            psql_cash["金额"] = psql_cash["金额"] / 100
            psql_cash = psql_cash.rename(
                columns={
                    "总交易金额": "金额",
                    "产品类型名称_中间库": "产品类型名称",
                    "产品类型_中间库": "产品类型",
                    "填报日期": "预算版本",
                }
            )

            psql_emp = _read_data("fact_personnel", "PSQL")
            psql_emp["填报日期"] = report_date
            psql_emp["日期"] = pd.to_datetime(psql_emp["日期"])
            psql_emp = psql_emp.rename(columns={"人数": "预算系统人数"})
            psql_emp = psql_emp[psql_emp["日期"].isin(date_range_psql)]

            offset = _read_data("fact_offset_by_month", "PSQL")
            offset["填报日期"] = report_date
            offset["会计期间"] = pd.to_datetime(offset["会计期间"])
            values = {
                "费用大类": "集团抵销",
                "核算项目": "集团抵销",
                "费用性质": "抵销费用",
                "收入大类": "集团抵销",
                "客户类别": "集团抵销",
                "客户群": "集团抵销",
                "客户区域": "集团抵销",
                "产品大类": "集团抵销",
                "物料编码": "PD99",
                "物料名称": "集团抵销",
                "费用项目编码": "F99",
                "业务线": "抵销数",
            }
            offset = offset.assign(**values)
            offset = offset[offset["会计期间"].isin(date_range_psql)].drop(
                ["类别"], axis=1, errors="ignore"
            )
            offset = offset.rename(
                columns={"会计期间": "日期", "金额": "预算系统金额"}
            )
            offset_inc = offset[
                offset["一级科目"].isin(["营业收入", "营业成本"])
            ].copy()
            offset_inc = offset_inc.rename(columns={"预算系统金额": "本月金额"})
            offset_exp = offset[
                offset["一级科目"].isin(
                    ["销售费用", "管理费用", "研发费用", "财务费用"]
                )
            ]

            # 费用：FONE 下半年 + 实际 + 抵销
            df = df_exp.copy()
            df["日期"] = pd.to_datetime(df["日期"])
            df = df[df["日期"].isin(date_range_fone)]
            df_con = pd.concat([df, psql_exp, offset_exp], ignore_index=True)
            df_con = df_con[df.columns]
            df_con.columns = [
                combined_column_mapping.get(c, c) for c in df_con.columns
            ]
            update_report_data("bud_expense", df_con, "report_date", report_date)

            # 收入
            df = df_inc.copy()
            df = df.rename(columns={"客户群": "客户群名称"})
            df["日期"] = pd.to_datetime(df["日期"])
            df = df[df["日期"].isin(date_range_fone)]
            df_con = pd.concat([df, psql_inc, offset_inc], ignore_index=True)
            df_con = df_con[df.columns]
            df_con.columns = [
                combined_column_mapping.get(c, c) for c in df_con.columns
            ]
            update_report_data("bud_income", df_con, "report_date", report_date)

            # 人员
            df = df_emp.copy()
            df["日期"] = pd.to_datetime(df["日期"])
            df = df[df["日期"].isin(date_range_fone)]
            df_con = pd.concat([df, psql_emp], ignore_index=True)
            df_con = df_con[df.columns]
            df_con.columns = [
                combined_column_mapping.get(c, c) for c in df_con.columns
            ]
            update_report_data("bud_personnel", df_con, "report_date", report_date)

            # 利润
            df = df_pro.copy()
            df["日期"] = pd.to_datetime(df["日期"])
            df = df[df["日期"].isin(date_range_fone)]
            df_con = pd.concat([df, psql_pro], ignore_index=True)
            df_con = df_con[df.columns]
            df_con.columns = [
                combined_column_mapping.get(c, c) for c in df_con.columns
            ]
            update_report_data("bud_profit", df_con, "report_date", report_date)

            # 流水
            df = df_cash.copy()
            df["日期"] = pd.to_datetime(df["日期"])
            df = df[df["日期"].isin(date_range_fone)]
            df_con = pd.concat([df, psql_cash], ignore_index=True)
            df_con = df_con[df.columns]
            df_con.columns = [
                combined_column_mapping.get(c, c) for c in df_con.columns
            ]
            update_report_data("bud_cash_flow", df_con, "bud_version", report_date)

            # 综合比例
            df = df_shared_rate.copy()
            df.columns = [combined_column_mapping.get(c, c) for c in df.columns]
            update_report_data("bud_bus_shared_rate", df, "report_date", report_date)
            print("年中预算写库完成")
            return

        raise ValueError(f"不支持的 budget_type: {budget_type}，应为「年初预算」或「年中预算」")
    except Exception as e:
        print(f"预算写库时发生错误: {str(e)}")
        raise
