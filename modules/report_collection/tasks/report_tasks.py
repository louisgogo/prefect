"""报表数据收集 - Tasks (基于Power Query M代码转换)

三步流程：
  阶段1 - 数据上报：从 1.上报数据 同步当月修改的文件到 1.补充数据/0.数据源
  阶段2 - 数据汇总：按收集汇总表的PQ结构，分表读取、过滤、汇总
  阶段3 - 名称转换：读取 map_translate 表，将中文名转换为英文名

参考PQ逻辑来源：收集汇总表（从本地刷新）.xlsx 的 Power Query M代码
"""

import os
import platform
import shutil
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from prefect import task

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 子表映射配置：原始子表名 -> 汇总表子表名
SHEET_MAPPING = {
    "收入成本明细": "2-2收入成本明细",
    "收单明细（嘉联）": "2-3收单明细",
    "应收明细": "2-5应收明细",
    "存货明细": "2-6存货明细",
    "在途存货": "2-13在途存货",
    "费用明细": "2-4费用明细",
    "其他项目拆分": "2-1利润拆分",
}

# 各子表的日期列配置
DATE_COLUMN_MAP = {
    "2-2收入成本明细": "会计期间",
    "2-3收单明细": None,  # 收单明细特殊处理，通过逆透视生成日期
    "2-5应收明细": "会计期间",
    "2-6存货明细": "会计期间",
    "2-13在途存货": "会计期间",
    "2-4费用明细": "会计期间",
    "2-1利润拆分": "日期",
}

# PQ定义的目标列结构（从M代码中提取）
TARGET_COLUMNS = {
    "2-2收入成本明细": [
        "财报合并",
        "财报单体",
        "一级组织",
        "二级组织",
        "三级组织",
        "单据编号",
        "业务日期",
        "客户编码",
        "客户",
        "客户群编码",
        "客户群名称",
        "是否内部销售",
        "收入大类",
        "产品大类",
        "币别",
        "汇率",
        "物料编码",
        "物料名称",
        "销售数量",
        "原币金额",
        "税额",
        "不含税金额本位币",
        "成本金额",
        "运费成本",
        "会计期间",
        "年份",
        "唯一层级",
    ],
    "2-5应收明细": [
        "财报合并",
        "财报单体",
        "一级组织",
        "二级组织",
        "三级组织",
        "客户编码",
        "往来单位",
        "往来性质",
        "客户类型",
        "销售区域",
        "应收账款余额",
        "逾期金额",
        "逾期30天以内金额",
        "逾期30天到90天金额",
        "逾期90天到180天金额",
        "逾期180天到360天金额",
        "逾期360天以上金额",
        "账龄1-2年",
        "账龄2-3年",
        "账龄3年以上",
        "账龄3个月以内",
        "账龄3-6个月",
        "账龄6-9个月",
        "账龄9-12个月",
        "本期借方发生额",
        "本期贷方发生额",
        "会计期间",
        "一级科目",
        "业务大类",
        "业务小类",
        "唯一层级",
    ],
    "2-6存货明细": [
        "财报合并",
        "财报单体",
        "一级组织",
        "二级组织",
        "三级组织",
        "物料编码",
        "物料名称",
        "存货类别",
        "客户编码",
        "客户名称",
        "客户类别",
        "仓库",
        "数量(库存)",
        "参考金额",
        "6个月以内数量",
        "6个月以内金额",
        "6-9个月数量",
        "6-9个月金额",
        "9个月-1年数量",
        "9个月-1年金额",
        "1-2年数量",
        "1-2年金额",
        "2-3年数量",
        "2-3年金额",
        "3年以上数量",
        "3年以上金额",
        "会计期间",
        "唯一层级",
    ],
    "2-4费用明细": [
        "财报合并",
        "财报单体",
        "一级组织",
        "二级组织",
        "三级组织",
        "项目编码",
        "研发项目",
        "日期",
        "凭证号",
        "摘要",
        "单据编号",
        "报销人",
        "一级科目",
        "核算项目-费控",
        "费用金额",
        "管报组织编码",
        "分摊业务线",
        "会计期间",
        "年份",
        "费用大类",
        "费用性质",
        "费用项目编码",
        "唯一层级",
    ],
    "2-13在途存货": [
        "财报合并",
        "财报单体",
        "一级组织",
        "二级组织",
        "三级组织",
        "订单号",
        "订单日期",
        "供应商编码",
        "供应商名称",
        "存货类别",
        "物料编码",
        "物料名称",
        "订单金额",
        "累计付款金额",
        "订单数量",
        "累计入库数量",
        "未入库数量",
        "单价",
        "币别",
        "汇率",
        "交货日期",
        "会计期间",
        "唯一层级",
    ],
    "2-1利润拆分": ["财报合并", "财报单体", "一级组织", "二级组织", "三级组织", "一级科目", "日期", "唯一层级", "年份", "备注", "本月金额"],
}


def _get_report_paths() -> Tuple[str, str]:
    """获取上报数据和数据源路径"""
    if platform.system() == "Windows":
        source_dir = r"Z:\11-业务报表\1.上报数据"
        target_dir = r"Z:\11-业务报表\1.补充数据\0.数据源"
    else:
        source_dir = r"/mnt/xgd_share/11-业务报表/1.上报数据"
        target_dir = r"/mnt/xgd_share/11-业务报表/1.补充数据/0.数据源"
    return source_dir, target_dir


def _get_current_month_range() -> Tuple[float, float]:
    """获取当前月份的时间范围（用于文件修改时间筛选）"""
    today = date.today()
    current_month_start = datetime(today.year, today.month, 1)
    if today.month == 12:
        next_month_start = datetime(today.year + 1, 1, 1)
    else:
        next_month_start = datetime(today.year, today.month + 1, 1)
    return current_month_start.timestamp(), next_month_start.timestamp()


def _get_last_month_range() -> Tuple[date, date]:
    """获取上个月份的日期范围（用于数据日期过滤）"""
    today = date.today()
    first_day_this_month = today.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return first_day_last_month, last_day_last_month


@task(name="sync_report_data_source", log_prints=True)
def sync_report_data_source_task() -> Dict[str, Any]:
    """阶段1：从 1.上报数据 同步当月修改的文件到 1.补充数据/0.数据源"""
    source_dir, target_dir = _get_report_paths()
    month_start, month_end = _get_current_month_range()

    print(f"--> 开始同步报表数据源")
    print(f"    源目录: {source_dir}")
    print(f"    目标目录: {target_dir}")
    print(f"    筛选条件: {datetime.fromtimestamp(month_start).strftime('%Y年%m月')}修改的文件")

    if not os.path.exists(source_dir):
        return {"success": False, "copied": 0, "skipped": 0, "message": f"源目录不存在: {source_dir}"}

    # 清空目标目录
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

    # 复制当月修改的文件
    copied_count = 0
    skipped_count = 0

    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if "~" in file or "$" in file:
                skipped_count += 1
                continue

            source_path = os.path.join(root, file)

            try:
                mtime = os.path.getmtime(source_path)
                if not (month_start <= mtime < month_end):
                    skipped_count += 1
                    continue
            except Exception as e:
                print(f"    [WARN] 无法获取文件时间 {source_path}: {e}")
                skipped_count += 1
                continue

            rel_path = os.path.relpath(source_path, source_dir)
            target_path = os.path.join(target_dir, rel_path)

            try:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
            except Exception as e:
                print(f"    [WARN] 创建目录失败 {os.path.dirname(target_path)}: {e}")
                continue

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
        "message": f"同步完成，复制 {copied_count} 个当月文件",
    }


@task(name="collect_by_sheet_pq", log_prints=True)
def collect_by_sheet_pq_task() -> Dict[str, pd.DataFrame]:
    """
    阶段2：按收集汇总表的PQ逻辑分表收集数据

    PQ逻辑转换：
    1. 扫描数据源目录所有Excel文件
    2. 对每个目标子表，展开特定列（类似Table.ExpandTableColumn）
    3. 类型转换（Table.TransformColumnTypes）
    4. 日期过滤（Date.IsInPreviousMonth）
    5. 特殊处理：收单明细需要逆透视月份列（UnpivotOtherColumns）
    """
    _, source_dir = _get_report_paths()
    last_month_start, last_month_end = _get_last_month_range()

    print(f"--> 开始按PQ逻辑分表收集数据")
    print(f"    数据源目录: {source_dir}")
    print(f"    日期过滤: 上个月 ({last_month_start} 至 {last_month_end})")

    # 初始化结果字典
    results = {sheet_name: [] for sheet_name in SHEET_MAPPING.values()}

    # 扫描数据源文件
    all_files = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith((".xlsx", ".xlsm")) and "~" not in file and "$" not in file:
                all_files.append(os.path.join(root, file))

    print(f"--> 发现 {len(all_files)} 个Excel文件")

    # 处理每个文件
    for filepath in all_files:
        try:
            xls = pd.ExcelFile(filepath)
            rel_path = os.path.relpath(filepath, source_dir)
            filename = os.path.basename(filepath)

            # 对每个目标原始子表
            for original_sheet, summary_sheet in SHEET_MAPPING.items():
                # 检查文件是否包含该子表
                matched_sheet = None
                for sheet in xls.sheet_names:
                    if original_sheet in sheet:
                        matched_sheet = sheet
                        break

                if not matched_sheet:
                    continue

                try:
                    # 读取原始数据
                    df = pd.read_excel(xls, sheet_name=matched_sheet, header=0)
                    if df.empty:
                        continue

                    # 根据子表类型应用不同的PQ转换逻辑
                    if summary_sheet == "2-3收单明细":
                        df_processed = _process_shoudan(
                            df, filename, rel_path, matched_sheet, last_month_start, last_month_end
                        )
                    elif summary_sheet == "2-2收入成本明细":
                        df_processed = _process_shouru_chengben(
                            df, filename, rel_path, matched_sheet, last_month_start, last_month_end
                        )
                    elif summary_sheet == "2-5应收明细":
                        df_processed = _process_yingshou(
                            df, filename, rel_path, matched_sheet, last_month_start, last_month_end
                        )
                    elif summary_sheet == "2-6存货明细":
                        df_processed = _process_cunhuo(
                            df, filename, rel_path, matched_sheet, last_month_start, last_month_end
                        )
                    elif summary_sheet == "2-4费用明细":
                        df_processed = _process_feiyong(
                            df, filename, rel_path, matched_sheet, last_month_start, last_month_end
                        )
                    elif summary_sheet == "2-13在途存货":
                        df_processed = _process_zaitu_cunhuo(
                            df, filename, rel_path, matched_sheet, last_month_start, last_month_end
                        )
                    elif summary_sheet == "2-1利润拆分":
                        df_processed = _process_lirun_chaifen(
                            df, filename, rel_path, matched_sheet, last_month_start, last_month_end
                        )
                    else:
                        df_processed = None

                    if df_processed is not None and not df_processed.empty:
                        results[summary_sheet].append(df_processed)
                        print(
                            f"    {filename}/{matched_sheet} -> {summary_sheet}: {len(df_processed)}行"
                        )

                except Exception as e:
                    print(f"    [WARN] 处理 {filename}/{matched_sheet} 失败: {e}")

        except Exception as e:
            print(f"    [WARN] 读取文件 {filepath} 失败: {e}")

    # 合并每个子表的结果
    final_results = {}
    for sheet_name, df_list in results.items():
        if df_list:
            df_combined = pd.concat(df_list, ignore_index=True)
            # 去除空白行列
            df_combined = df_combined.dropna(how="all")
            df_combined = df_combined.dropna(axis=1, how="all")
            # 对齐目标列
            if sheet_name in TARGET_COLUMNS:
                target_cols = TARGET_COLUMNS[sheet_name]
                existing_cols = [c for c in target_cols if c in df_combined.columns]
                df_combined = df_combined[
                    existing_cols + [c for c in df_combined.columns if c not in target_cols]
                ]
            final_results[sheet_name] = df_combined
            print(f"--> {sheet_name}: 汇总 {len(df_combined)} 行")
        else:
            final_results[sheet_name] = pd.DataFrame()
            print(f"--> {sheet_name}: 无数据")

    # ──── 利润拆分汇总逻辑（参考PQ: 利润拆分-上报）────
    # 利润拆分 = 其他项目拆分（已处理） + 收入成本明细汇总 + 费用明细汇总
    final_results = _aggregate_lirun_chaifen(final_results, last_month_start, last_month_end)

    return final_results


def _process_shoudan(df, filename, rel_path, matched_sheet, last_month_start, last_month_end):
    """
    PQ逻辑：收单明细特殊处理
    需要逆透视月份列（类似Table.UnpivotOtherColumns）
    """
    # 检查是否有月份列（10月-9月）
    month_cols = ["10月", "11月", "12月", "1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月"]
    available_month_cols = [c for c in month_cols if c in df.columns]

    if not available_month_cols:
        # 没有月份列，直接返回
        df_out = df.copy()
        df_out["_source_file"] = filename
        df_out["_source_path"] = rel_path
        df_out["_original_sheet"] = matched_sheet
        return df_out

    # PQ逆透视逻辑：将月份列转为行
    id_cols = [c for c in df.columns if c not in month_cols]
    df_melted = df.melt(
        id_vars=id_cols, value_vars=available_month_cols, var_name="月份", value_name="金额"
    )

    # 提取月份数字并构造日期
    df_melted["月份数字"] = df_melted["月份"].str.replace("月", "").astype(int)

    # 获取当前年份
    current_year = datetime.now().year

    # 构造日期：1-9月用当前年，10-12月用上一年
    def construct_date(row):
        month = row["月份数字"]
        year = current_year - 1 if month >= 10 else current_year
        return f"{year}-{month:02d}-01"

    df_melted["日期"] = pd.to_datetime(df_melted.apply(construct_date, axis=1))

    # 日期过滤（上个月）
    df_melted = df_melted[
        (df_melted["日期"] >= pd.Timestamp(last_month_start))
        & (df_melted["日期"] <= pd.Timestamp(last_month_end))
    ]

    if df_melted.empty:
        return None

    # 重命名和选择列（对齐PQ输出）
    df_melted = df_melted.rename(columns={"辅助列": "指标"})

    # 添加元信息
    df_melted["_source_file"] = filename
    df_melted["_source_path"] = rel_path
    df_melted["_original_sheet"] = matched_sheet

    return df_melted[["指标", "金额", "日期", "_source_file", "_source_path", "_original_sheet"]]


def _process_shouru_chengben(
    df, filename, rel_path, matched_sheet, last_month_start, last_month_end
):
    """PQ逻辑：收入成本明细处理"""
    # 删除验证列
    if "验证" in df.columns:
        df = df.drop(columns=["验证"])

    # 类型转换
    if "会计期间" in df.columns:
        df["会计期间"] = pd.to_datetime(df["会计期间"], errors="coerce")

    # 筛选非空一级组织
    if "一级组织" in df.columns:
        df = df[df["一级组织"].notna()]

    # 业务日期类型转换
    if "业务日期" in df.columns:
        df["业务日期"] = pd.to_datetime(df["业务日期"], errors="coerce")

    # 日期过滤（上个月）
    if "会计期间" in df.columns:
        df = df[
            (df["会计期间"] >= pd.Timestamp(last_month_start))
            & (df["会计期间"] <= pd.Timestamp(last_month_end))
        ]

    if df.empty:
        return None

    # 添加元信息
    df["_source_file"] = filename
    df["_source_path"] = rel_path
    df["_original_sheet"] = matched_sheet

    return df


def _process_yingshou(df, filename, rel_path, matched_sheet, last_month_start, last_month_end):
    """PQ逻辑：应收明细处理"""
    # 删除验证列
    if "验证" in df.columns:
        df = df.drop(columns=["验证"])

    # 类型转换
    if "会计期间" in df.columns:
        df["会计期间"] = pd.to_datetime(df["会计期间"], errors="coerce")
    if "财报合并" in df.columns:
        df["财报合并"] = df["财报合并"].astype(str)

    # 数值列类型转换和错误值替换
    num_cols = [
        "应收账款余额",
        "逾期金额",
        "逾期30天以内金额",
        "逾期30天到90天金额",
        "逾期90天到180天金额",
        "逾期180天到360天金额",
        "逾期360天以上金额",
        "账龄1-2年",
        "账龄2-3年",
        "账龄3年以上",
        "本期借方发生额",
        "本期贷方发生额",
        "账龄3个月以内",
        "账龄3-6个月",
        "账龄6-9个月",
        "账龄9-12个月",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 筛选非“-”的唯一层级
    if "唯一层级" in df.columns:
        df = df[df["唯一层级"] != "-"]

    # 日期过滤（上个月）
    if "会计期间" in df.columns:
        df = df[
            (df["会计期间"] >= pd.Timestamp(last_month_start))
            & (df["会计期间"] <= pd.Timestamp(last_month_end))
        ]

    if df.empty:
        return None

    # 添加元信息
    df["_source_file"] = filename
    df["_source_path"] = rel_path
    df["_original_sheet"] = matched_sheet

    return df


def _process_cunhuo(df, filename, rel_path, matched_sheet, last_month_start, last_month_end):
    """PQ逻辑：存货明细处理"""
    # 删除验证列
    if "验证" in df.columns:
        df = df.drop(columns=["验证"])

    # 类型转换
    if "会计期间" in df.columns:
        df["会计期间"] = pd.to_datetime(df["会计期间"], errors="coerce")

    # 筛选非“-”的唯一层级
    if "唯一层级" in df.columns:
        df = df[df["唯一层级"] != "-"]

    # 日期过滤（上个月）
    if "会计期间" in df.columns:
        df = df[
            (df["会计期间"] >= pd.Timestamp(last_month_start))
            & (df["会计期间"] <= pd.Timestamp(last_month_end))
        ]

    # 物料编码转为文本
    if "物料编码" in df.columns:
        df["物料编码"] = df["物料编码"].astype(str)

    if df.empty:
        return None

    # 添加元信息
    df["_source_file"] = filename
    df["_source_path"] = rel_path
    df["_original_sheet"] = matched_sheet

    return df


def _process_feiyong(df, filename, rel_path, matched_sheet, last_month_start, last_month_end):
    """PQ逻辑：费用明细处理"""
    # 删除验证列
    if "验证" in df.columns:
        df = df.drop(columns=["验证"])

    # 类型转换
    if "会计期间" in df.columns:
        df["会计期间"] = pd.to_datetime(df["会计期间"], errors="coerce")
    if "日期" in df.columns:
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")

    # 日期过滤（上个月）- 使用会计期间
    if "会计期间" in df.columns:
        df = df[
            (df["会计期间"] >= pd.Timestamp(last_month_start))
            & (df["会计期间"] <= pd.Timestamp(last_month_end))
        ]

    # 单据编号和费用项目编码转为文本
    for col in ["单据编号", "费用项目编码"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    if df.empty:
        return None

    # 添加元信息
    df["_source_file"] = filename
    df["_source_path"] = rel_path
    df["_original_sheet"] = matched_sheet

    return df


def _process_zaitu_cunhuo(df, filename, rel_path, matched_sheet, last_month_start, last_month_end):
    """PQ逻辑：在途存货处理"""
    # 筛选非空二级组织
    if "二级组织" in df.columns:
        df = df[df["二级组织"].notna()]

    # 筛选非“-”的唯一层级
    if "唯一层级" in df.columns:
        df = df[df["唯一层级"] != "-"]

    # 类型转换
    if "会计期间" in df.columns:
        df["会计期间"] = pd.to_datetime(df["会计期间"], errors="coerce")
    if "交货日期" in df.columns:
        df["交货日期"] = pd.to_datetime(df["交货日期"], errors="coerce")

    # 数值列类型转换
    num_cols = ["汇率", "单价", "累计入库数量", "订单数量", "累计付款金额", "订单金额"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 删除验证列
    if "验证" in df.columns:
        df = df.drop(columns=["验证"])

    # 日期过滤（上个月）
    if "会计期间" in df.columns:
        df = df[
            (df["会计期间"] >= pd.Timestamp(last_month_start))
            & (df["会计期间"] <= pd.Timestamp(last_month_end))
        ]

    if df.empty:
        return None

    # 添加元信息
    df["_source_file"] = filename
    df["_source_path"] = rel_path
    df["_original_sheet"] = matched_sheet

    return df


def _process_lirun_chaifen(df, filename, rel_path, matched_sheet, last_month_start, last_month_end):
    """PQ逻辑：利润拆分（其他项目拆分）处理"""
    # 日期过滤（上个月）- 使用日期列
    if "日期" in df.columns:
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df = df[
            (df["日期"] >= pd.Timestamp(last_month_start))
            & (df["日期"] <= pd.Timestamp(last_month_end))
        ]

    # 类型转换
    if "本月金额" in df.columns:
        df["本月金额"] = pd.to_numeric(df["本月金额"], errors="coerce")

    # 筛选特定一级科目（排除费用类科目，这些从费用明细汇总）
    # 同时排除一级科目为null的行，与PQ逻辑保持一致
    if "一级科目" in df.columns:
        df = df[df["一级科目"].notna()]
        exclude_subjects = ["研发费用", "管理费用", "营业成本", "营业收入", "财务费用", "销售费用"]
        df = df[~df["一级科目"].isin(exclude_subjects)]

    if df.empty:
        return None

    # 添加元信息
    df["_source_file"] = filename
    df["_source_path"] = rel_path
    df["_original_sheet"] = matched_sheet

    return df


def _aggregate_lirun_chaifen(
    final_results: Dict[str, pd.DataFrame], last_month_start: date, last_month_end: date
) -> Dict[str, pd.DataFrame]:
    """
    PQ逻辑：利润拆分汇总
    合并三部分数据：
      1. 其他项目拆分（已存在于 2-1利润拆分）
      2. 收入成本明细汇总 → 营业收入、营业成本
      3. 费用明细汇总 → 研发费用、管理费用、财务费用、销售费用
    然后按关键列分组汇总
    """
    lirun_key = "2-1利润拆分"
    shouru_key = "2-2收入成本明细"
    feiyong_key = "2-4费用明细"

    # 获取各部分数据
    df_lirun_base = final_results.get(lirun_key, pd.DataFrame())
    df_shouru = final_results.get(shouru_key, pd.DataFrame())
    df_feiyong = final_results.get(feiyong_key, pd.DataFrame())

    all_parts = []

    # 1. 基础利润拆分数据（其他项目拆分，已排除特定科目）
    if not df_lirun_base.empty:
        all_parts.append(df_lirun_base)
        print(f"--> 利润拆分汇总: 基础数据 {len(df_lirun_base)} 行")

    # 2. 收入成本明细汇总 → 营业收入、营业成本
    if not df_shouru.empty:
        df_shouru_agg = _aggregate_shouru_to_lirun(df_shouru, last_month_start, last_month_end)
        if not df_shouru_agg.empty:
            all_parts.append(df_shouru_agg)
            print(f"--> 利润拆分汇总: 收入成本明细聚合 {len(df_shouru_agg)} 行")

    # 3. 费用明细汇总 → 各费用科目
    if not df_feiyong.empty:
        df_feiyong_agg = _aggregate_feiyong_to_lirun(df_feiyong, last_month_start, last_month_end)
        if not df_feiyong_agg.empty:
            all_parts.append(df_feiyong_agg)
            print(f"--> 利润拆分汇总: 费用明细聚合 {len(df_feiyong_agg)} 行")

    # 合并所有部分
    if all_parts:
        df_combined = pd.concat(all_parts, ignore_index=True)

        # 分组汇总（PQ: Table.Group）
        group_cols = ["财报合并", "财报单体", "一级组织", "二级组织", "三级组织", "一级科目", "日期", "唯一层级", "年份", "备注"]
        # 只保留存在的列
        group_cols = [c for c in group_cols if c in df_combined.columns]

        if group_cols and "本月金额" in df_combined.columns:
            # 按分组列汇总本月金额
            df_grouped = df_combined.groupby(group_cols, as_index=False, dropna=False)["本月金额"].sum()
            # 保留元信息列（取第一个非空值）
            meta_cols = ["_source_file", "_source_path", "_original_sheet"]
            for meta_col in meta_cols:
                if meta_col in df_combined.columns:
                    df_grouped[meta_col] = "利润拆分汇总"
            final_results[lirun_key] = df_grouped
            print(f"--> 利润拆分汇总完成: 共 {len(df_grouped)} 行")
        else:
            final_results[lirun_key] = df_combined
            print(f"--> 利润拆分汇总完成（未分组）: 共 {len(df_combined)} 行")
    else:
        final_results[lirun_key] = pd.DataFrame()
        print(f"--> 利润拆分汇总: 无数据")

    return final_results


def _aggregate_shouru_to_lirun(
    df_shouru: pd.DataFrame, last_month_start: date, last_month_end: date
) -> pd.DataFrame:
    """
    PQ逻辑：收入成本明细-汇总至利润拆分
    计算营业收入、营业成本，并按组织维度汇总
    """
    df = df_shouru.copy()

    # 确保数值列为数字类型
    numeric_cols = ["不含税金额本位币", "成本金额", "运费成本", "软件成本", "关税成本"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 计算营业成本 = 成本金额 + 运费成本 + 软件成本 + 关税成本
    df["营业成本"] = df["成本金额"] + df["运费成本"] + df["软件成本"] + df["关税成本"]

    # 构建汇总数据：营业收入
    df_yingye_shouru = df.copy()
    df_yingye_shouru["一级科目"] = "营业收入"
    df_yingye_shouru["本月金额"] = df_yingye_shouru["不含税金额本位币"]

    # 构建汇总数据：营业成本
    df_yingye_chengben = df.copy()
    df_yingye_chengben["一级科目"] = "营业成本"
    df_yingye_chengben["本月金额"] = df_yingye_chengben["营业成本"]

    # 合并两部分
    df_result = pd.concat([df_yingye_shouru, df_yingye_chengben], ignore_index=True)

    # 选择利润拆分需要的列
    target_cols = ["财报合并", "财报单体", "一级组织", "二级组织", "三级组织", "一级科目", "会计期间", "唯一层级", "年份", "本月金额"]
    existing_cols = [c for c in target_cols if c in df_result.columns]
    df_result = df_result[existing_cols].copy()

    # 重命名会计期间为日期（PQ中的统一）
    if "会计期间" in df_result.columns:
        df_result = df_result.rename(columns={"会计期间": "日期"})

    # 过滤本月金额为0的行（与PQ逻辑保持一致）
    df_result = df_result[df_result["本月金额"] != 0]

    return df_result


def _aggregate_feiyong_to_lirun(
    df_feiyong: pd.DataFrame, last_month_start: date, last_month_end: date
) -> pd.DataFrame:
    """
    PQ逻辑：费用明细-汇总至利润拆分
    按一级科目（费用类型）汇总
    """
    df = df_feiyong.copy()

    # 确保数值列为数字类型
    if "费用金额" in df.columns:
        df["费用金额"] = pd.to_numeric(df["费用金额"], errors="coerce").fillna(0)

    # 本月金额 = 费用金额
    df["本月金额"] = df["费用金额"]

    # 选择利润拆分需要的列
    target_cols = ["财报合并", "财报单体", "一级组织", "二级组织", "三级组织", "一级科目", "会计期间", "唯一层级", "年份", "本月金额"]
    existing_cols = [c for c in target_cols if c in df.columns]
    df_result = df[existing_cols].copy()

    # 重命名会计期间为日期
    if "会计期间" in df_result.columns:
        df_result = df_result.rename(columns={"会计期间": "日期"})

    # 筛选一级科目为费用类的数据
    expense_subjects = ["研发费用", "管理费用", "财务费用", "销售费用"]
    if "一级科目" in df_result.columns:
        df_result = df_result[df_result["一级科目"].isin(expense_subjects)]

    # 过滤本月金额为0的行（与PQ逻辑保持一致）
    df_result = df_result[df_result["本月金额"] != 0]

    return df_result


# 利润表核对的16个一级科目（与PQ利润表-核对保持一致）
PROFIT_VALIDATION_SUBJECTS = [
    "营业收入",
    "营业成本",
    "税金及附加",
    "销售费用",
    "管理费用",
    "研发费用",
    "财务费用",
    "投资收益",
    "公允价值变动收益",
    "信用减值损失",
    "资产减值损失",
    "资产处置收益",
    "其他收益",
    "营业外收入",
    "营业外支出",
    "所得税费用",
]

EXCLUDED_PROJECTS = ["新国都", "体系外", "业报调整", "抵销数"]


@task(name="validate_profit_report", log_prints=True)
def validate_profit_report_task(df_profit: pd.DataFrame) -> Dict[str, Any]:
    """
    利润表校验任务：从数据库 fact_profit_stmt 读取累计数，
    按PQ逻辑计算当月实际数，并与收集汇总的 2-1利润拆分 按合并主体+科目核对。
    """
    from mypackage.utilities import connect_to_db

    last_month_start, last_month_end = _get_last_month_range()
    target_date = last_month_start
    prev_month_date = (target_date.replace(day=1) - timedelta(days=1)).replace(day=1)

    print(f"--> 开始利润表校验")
    print(f"    目标月份: {target_date}，上月累计: {prev_month_date}")

    # 1. 从数据库读取 fact_profit_stmt
    try:
        conn, cur = connect_to_db()
        subjects_sql = ",".join([f"'{s}'" for s in PROFIT_VALIDATION_SUBJECTS])
        sql = f"""
            SELECT subj, project, date, amt
            FROM fact_profit_stmt
            WHERE subj IN ({subjects_sql})
              AND date IN (%s, %s)
            ORDER BY project, subj, date
        """
        cur.execute(sql, (prev_month_date, target_date))
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df_db = pd.DataFrame(rows, columns=cols)
        cur.close()
        conn.close()
        print(f"--> 从数据库读取完成: {len(df_db)} 行")
    except Exception as e:
        print(f"[ERROR] 读取 fact_profit_stmt 失败: {e}")
        raise

    if df_db.empty:
        print("[WARN] 数据库中未找到利润表数据，跳过校验")
        return {"success": True, "matched": 0, "diff_count": 0, "details": []}

    # 2. 数据类型转换
    df_db["amt"] = pd.to_numeric(df_db["amt"], errors="coerce").fillna(0)
    df_db["date"] = pd.to_datetime(df_db["date"]).dt.date

    # 3. 计算当月实际数（PQ逻辑：1月=累计数，其他月=当月累计-上月累计）
    # 先 pivot 出当月和上月两列
    df_pivot = df_db.pivot_table(
        index=["subj", "project"],
        columns="date",
        values="amt",
        aggfunc="sum",
    ).reset_index()

    # 确定列名
    curr_col = target_date
    prev_col = prev_month_date

    df_pivot[curr_col] = pd.to_numeric(df_pivot.get(curr_col, 0), errors="coerce").fillna(0)
    df_pivot[prev_col] = pd.to_numeric(df_pivot.get(prev_col, 0), errors="coerce").fillna(0)

    # 当月实际数
    if target_date.month == 1:
        df_pivot["本月金额"] = df_pivot[curr_col]
    else:
        df_pivot["本月金额"] = df_pivot[curr_col] - df_pivot[prev_col]

    df_db_month = df_pivot[["subj", "project", "本月金额"]].copy()
    df_db_month = df_db_month.rename(columns={"subj": "科目", "project": "项目", "本月金额": "利润表本月金额"})
    df_db_month["日期"] = target_date

    # 排除不需要核对的项目
    exclude_pattern = "|".join(EXCLUDED_PROJECTS)
    df_db_month = df_db_month[
        ~df_db_month["项目"].str.contains(exclude_pattern, na=False, regex=True)
    ].copy()

    # 4. 收集汇总的 2-1利润拆分 按 {财报合并, 一级科目, 日期} 汇总
    if df_profit is None or df_profit.empty:
        print("[WARN] 2-1利润拆分 无数据，跳过校验")
        return {"success": True, "matched": 0, "diff_count": 0, "details": []}

    df_profit_copy = df_profit.copy()
    if "日期" in df_profit_copy.columns:
        df_profit_copy["日期"] = pd.to_datetime(df_profit_copy["日期"], errors="coerce").dt.date
    if "本月金额" in df_profit_copy.columns:
        df_profit_copy["本月金额"] = pd.to_numeric(df_profit_copy["本月金额"], errors="coerce").fillna(0)

    group_cols = [c for c in ["财报合并", "一级科目", "日期"] if c in df_profit_copy.columns]
    if not group_cols:
        print("[WARN] 2-1利润拆分 缺少必要列，跳过校验")
        return {"success": True, "matched": 0, "diff_count": 0, "details": []}

    df_collected = df_profit_copy.groupby(group_cols, as_index=False, dropna=False)["本月金额"].sum()
    df_collected = df_collected.rename(columns={"财报合并": "项目", "一级科目": "科目", "本月金额": "利润拆分本月金额"})

    # 排除不需要核对的项目
    df_collected = df_collected[
        ~df_collected["项目"].str.contains(exclude_pattern, na=False, regex=True)
    ].copy()

    # 5. 合并核对（LeftOuter: 利润表 -> 利润拆分）
    merged = pd.merge(
        df_db_month,
        df_collected,
        on=["项目", "科目", "日期"],
        how="outer",
        indicator=True,
        suffixes=("", "_col"),
    )

    # 填充空值
    merged["利润表本月金额"] = pd.to_numeric(merged.get("利润表本月金额", 0), errors="coerce").fillna(0)
    merged["利润拆分本月金额"] = pd.to_numeric(merged.get("利润拆分本月金额", 0), errors="coerce").fillna(0)

    # 计算差额（与PQ保持一致：Round(利润表本月金额 - 利润拆分本月金额, 2)）
    merged["利润表差额"] = (merged["利润表本月金额"] - merged["利润拆分本月金额"]).round(2)

    # 6. 输出核对结果
    diff_rows = merged[abs(merged["利润表差额"]) > 0.01]
    only_db = merged[merged["_merge"] == "left_only"]
    only_collected = merged[merged["_merge"] == "right_only"]

    print(f"\n【利润表校验结果】")
    print(f"  数据库行数: {len(df_db_month)}")
    print(f"  收集汇总行数: {len(df_collected)}")
    print(f"  匹配行数: {len(merged[merged['_merge'] == 'both'])}")
    print(f"  仅数据库有: {len(only_db)}")
    print(f"  仅收集汇总有: {len(only_collected)}")
    print(f"  差额>0.01 行数: {len(diff_rows)}")

    if not diff_rows.empty:
        print("\n  差额明细（前20条）:")
        for _, row in diff_rows.head(20).iterrows():
            print(
                f"    {row['项目']} | {row['科目']} | "
                f"利润表={row['利润表本月金额']:,.2f} | "
                f"利润拆分={row['利润拆分本月金额']:,.2f} | "
                f"差额={row['利润表差额']:,.2f}"
            )

    if not only_db.empty:
        print("\n  仅数据库有（前10条）:")
        for _, row in only_db.head(10).iterrows():
            print(f"    {row['项目']} | {row['科目']} | 利润表={row['利润表本月金额']:,.2f}")

    if not only_collected.empty:
        print("\n  仅收集汇总有（前10条）:")
        for _, row in only_collected.head(10).iterrows():
            print(f"    {row['项目']} | {row['科目']} | 利润拆分={row['利润拆分本月金额']:,.2f}")

    total_diff = diff_rows["利润表差额"].sum() if not diff_rows.empty else 0
    print(f"\n  总差额合计: {total_diff:,.2f}")

    has_error = len(diff_rows) > 0 or len(only_db) > 0 or len(only_collected) > 0
    if has_error:
        print("  [WARN] 利润表校验发现差异，请关注！")
    else:
        print("  [OK] 利润表校验通过，无差异")

    # 7. 导出差异清单Excel（仅差异行，单sheet）
    output_path = ""
    diff_export = merged.copy()
    diff_export["差异类型"] = ""
    diff_export.loc[diff_export["_merge"] == "left_only", "差异类型"] = "仅数据库有"
    diff_export.loc[diff_export["_merge"] == "right_only", "差异类型"] = "仅收集汇总有"
    diff_export.loc[
        (diff_export["_merge"] == "both") & (abs(diff_export["利润表差额"]) > 0.01),
        "差异类型",
    ] = "金额不匹配"

    diff_export_rows = diff_export[diff_export["差异类型"] != ""].copy()
    diff_export_rows = diff_export_rows[["差异类型", "项目", "科目", "日期", "利润表本月金额", "利润拆分本月金额", "利润表差额"]]

    if not diff_export_rows.empty:
        if platform.system() == "Windows":
            output_path = r"Z:\11-业务报表\1.补充数据\9.手工刷新\利润表核对差异明细.xlsx"
        else:
            output_path = r"/mnt/xgd_share/11-业务报表/1.补充数据/9.手工刷新/利润表核对差异明细.xlsx"
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                diff_export_rows.to_excel(writer, sheet_name="差异明细", index=False)
            print(f"  差异清单已导出: {output_path} (共 {len(diff_export_rows)} 行)")
        except (PermissionError, OSError):
            # 文件被占用时写入带时间戳的备用文件
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            alt_path = os.path.join(os.path.dirname(output_path), f"利润表核对差异明细_{ts}.xlsx")
            with pd.ExcelWriter(alt_path, engine="openpyxl") as writer:
                diff_export_rows.to_excel(writer, sheet_name="差异明细", index=False)
            output_path = alt_path
            print(f"  差异清单已导出: {output_path} (共 {len(diff_export_rows)} 行)")
        except Exception as e:
            print(f"  [WARN] 导出差异清单失败: {e}")
    else:
        print("  无差异，跳过Excel导出")

    # 返回结构化结果
    details = merged[["项目", "科目", "日期", "利润表本月金额", "利润拆分本月金额", "利润表差额"]].to_dict("records")

    return {
        "success": True,
        "matched": int(len(merged[merged["_merge"] == "both"])),
        "diff_count": int(len(diff_rows)),
        "only_db_count": int(len(only_db)),
        "only_collected_count": int(len(only_collected)),
        "total_diff": float(total_diff),
        "has_error": bool(has_error),
        "diff_file": output_path,
        "details": details,
    }


@task(name="load_map_translate", log_prints=True)
def load_map_translate_task() -> pd.DataFrame:
    """从数据库读取 map_translate 表"""
    from mypackage.utilities import connect_to_db

    print("--> 从数据库读取 map_translate 表")

    try:
        conn, cur = connect_to_db()
        cur.execute("SELECT * FROM map_translate")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        print(f"--> 读取 map_translate 完成，共 {len(df)} 条映射")
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"[ERROR] 读取 map_translate 失败: {e}")
        raise


@task(name="translate_and_export_pq", log_prints=True)
def translate_and_export_pq_task(
    results: Dict[str, pd.DataFrame], df_map: pd.DataFrame
) -> Dict[str, str]:
    """
    阶段3：转换列名并导出各子表为CSV
    """
    # 构建映射字典（name_ch -> name_en）
    mapping = {}
    if "name_ch" in df_map.columns and "name_en" in df_map.columns:
        for _, row in df_map.iterrows():
            if pd.notna(row["name_ch"]) and pd.notna(row["name_en"]):
                mapping[row["name_ch"]] = row["name_en"]

    print(f"--> 列名映射字典: {len(mapping)} 条")

    # 输出目录
    if platform.system() == "Windows":
        output_dir = r"Z:\11-业务报表\1.补充数据\9.手工刷新"
    else:
        output_dir = r"/mnt/xgd_share/11-业务报表/1.补充数据/9.手工刷新"

    os.makedirs(output_dir, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")

    exported_files = {}

    for sheet_name, df in results.items():
        if df.empty:
            print(f"    [跳过] {sheet_name}: 无数据")
            continue

        # 转换列名
        rename_dict = {}
        for col in df.columns:
            if col in mapping:
                rename_dict[col] = mapping[col]

        if rename_dict:
            df_translated = df.rename(columns=rename_dict)
            print(f"    {sheet_name}: 转换 {len(rename_dict)} 个列名")
        else:
            df_translated = df.copy()

        # 导出CSV
        safe_name = sheet_name.replace("-", "_").replace(" ", "_")
        filename = f"report_{safe_name}_{today_str}.csv"
        output_path = os.path.join(output_dir, filename)

        try:
            df_translated.to_csv(output_path, index=False, encoding="utf-8-sig")
            exported_files[sheet_name] = output_path
            print(f"    [已导出] {sheet_name}: {output_path} ({len(df_translated)} 行)")
        except Exception as e:
            print(f"    [ERROR] 导出 {sheet_name} 失败: {e}")

    return exported_files
