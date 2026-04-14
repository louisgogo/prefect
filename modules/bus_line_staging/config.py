from datetime import datetime, timedelta

import pandas as pd
from mypackage.utilities import connect_to_db


def get_bus_lines():
    """从数据库 fact_bus_line 动态获取业务线列表"""
    try:
        conn, cur = connect_to_db()
        cur.execute(
            "SELECT DISTINCT bus_line FROM fact_bus_line WHERE bus_line IS NOT NULL AND bus_line != ''"
        )
        lines = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return lines
    except Exception as e:
        print(f"获取业务线列表失败: {e}")
        # 降级返回原有硬编码列表
        return [
            "国际业务",
            "美国业务",
            "国内硬件",
            "小POS",
            "立充",
            "大POS",
            "澳门业务",
            "跨境总部",
            "跨境欧洲",
            "跨境新加坡",
            "能源运营",
            "AGI",
            "web3",
            "资产运营",
            "政府事务",
        ]


def get_date_range(start_date=None, end_date=None):
    """
    获取日期范围。如果提供 start_date 和 end_date，则返回该范围；
    否则默认返回上个月全月。
    """
    if start_date and end_date:
        return pd.date_range(start=start_date, end=end_date, freq="D").date

    # 默认上个月
    current_date = datetime.now().date()
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    return pd.date_range(
        start=first_day_of_previous_month, end=last_day_of_previous_month, freq="D"
    ).date


# --- 组织架构分组 (硬编码) ---

groups_backend = [
    "bsec_center",  # 董事会秘书处
    "bsiao_center",  # 资产运营部
    "pstec_center",  # 在建项目组
    "bsiga_center",  # 政府事务中心
    "bsisd_center",  # 战略发展部
    "bas_center",  # 审计监察部
    "bsibm_center",  # 品牌管理部
    "simc_center",  # 信息管理中心
    "shrc_center",  # 人力资源中心
    "sac_center",  # 行政中心
    "slc_center",  # 法务中心
]

groups_middle = [
    "phoc_center",  # 国际渠道运营中心
    "phtc_center",  # 支付硬件技术中心
    "pstc_center",  # 支付服务培训中心
    "pstec_center",  # 支付服务技术中心
    "phpc_center",  # 国际渠道产品中心
    "psrac_center",  # 监管事务中心
    "smcp_center",  # 支付硬件集采中心
    "smasc_center",  # 收单供应中心
    "pssc_center",  # 支付安全中心
    "pscpo_center",  # 消保中心
    "phqc_center",  # 支付硬件质量中心
    "pspc_center",  # 支付服务产品中心
    "smmc_center",  # 智造管理中心
]

groups_frontend = [
    "dcpmc_center",  # 合伙人营销中心
    "psacc_center",  # 审核能力中心
    "omc_center",  # 国内渠道运营中心
    "cec_center",  # 消费电子中心
]
