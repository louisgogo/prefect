-- 业务线费用拆分中间表
-- 列名与 fact_expense 保持一致，增加 sec_dist_lvl / bus_line_name / allocated_amount
CREATE TABLE IF NOT EXISTS public.staging_bus_expense (
    id SERIAL PRIMARY KEY,
    source_no VARCHAR(255),          -- 来源编号
    unique_lvl VARCHAR(255),         -- 唯一层级
    prim_org VARCHAR(255),           -- 一级组织
    sec_org VARCHAR(255),            -- 二级组织
    third_org VARCHAR(255),          -- 三级组织
    doc_no VARCHAR(255),             -- 单据编号
    claimant VARCHAR(255),           -- 报销人
    summary TEXT,                    -- 摘要
    acct_period DATE,                -- 会计期间
    exp_nature VARCHAR(255),         -- 费用性质
    exp_major_cat VARCHAR(255),      -- 费用大类
    acc_proj_cost VARCHAR(255),      -- 核算项目-费控
    rd_proj VARCHAR(255),            -- 研发项目
    proj_code VARCHAR(255),          -- 项目编码
    exp_amt DECIMAL(18, 4),          -- 费用金额（原始）
    year INT,                        -- 年份
    sec_dist_lvl VARCHAR(255),       -- 数据来源（分摊前原始唯一层级）
    dist_bus_line VARCHAR(255),      -- 分摊业务线
    bus_line_name VARCHAR(255),      -- 拆分到的业务线名称
    allocated_amount DECIMAL(18, 4), -- 拆分后的金额
    audit_status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 业务线收入拆分中间表
-- 列名与 fact_revenue 保持一致，增加 bus_line_name / allocated_amount
CREATE TABLE IF NOT EXISTS public.staging_bus_revenue (
    id SERIAL PRIMARY KEY,
    source_no VARCHAR(255),
    prim_org VARCHAR(255),
    sec_org VARCHAR(255),
    third_org VARCHAR(255),
    acct_period DATE,                -- 会计期间
    inc_major_cat VARCHAR(255),      -- 收入大类
    prod_major_cat VARCHAR(255),     -- 产品大类
    mat_name VARCHAR(255),           -- 物料名称
    amt_tax_exc_loc DECIMAL(18, 4),  -- 不含税金额本位币
    cost_amt DECIMAL(18, 4),         -- 成本金额
    freight_cost DECIMAL(18, 4),     -- 运费成本
    tariff_cost DECIMAL(18, 4),      -- 关税成本
    soft_cost DECIMAL(18, 4),        -- 软件成本
    unique_lvl VARCHAR(255),
    year INT,
    bus_line_name VARCHAR(255),
    allocated_amount DECIMAL(18, 4),
    audit_status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 业务线其他损益项中间表
-- 列名与 fact_profit_bd 保持一致，增加 bus_line_name / allocated_amount
CREATE TABLE IF NOT EXISTS public.staging_bus_profit_bd (
    id SERIAL PRIMARY KEY,
    source_no VARCHAR(255),
    fin_con VARCHAR(255),            -- 财报合并
    prim_org VARCHAR(255),
    sec_org VARCHAR(255),
    third_org VARCHAR(255),
    date DATE,                       -- 日期
    prim_subj VARCHAR(255),          -- 一级科目
    mo_amt DECIMAL(18, 4),           -- 本月金额（原始）
    unique_lvl VARCHAR(255),
    year INT,
    bus_line_name VARCHAR(255),
    allocated_amount DECIMAL(18, 4),
    audit_status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 业务线存货中间表
-- 列名与 fact_inventory 保持一致，增加 bus_line_name / allocated_amount
-- fact_inventory 无 year 列，由任务层派生
CREATE TABLE IF NOT EXISTS public.staging_bus_inventory (
    id SERIAL PRIMARY KEY,
    source_no VARCHAR(255),
    prim_org VARCHAR(255),
    sec_org VARCHAR(255),
    third_org VARCHAR(255),
    acct_period DATE,                -- 会计期间
    mat_name VARCHAR(255),           -- 物料名称 (mat_name)
    ref_amt DECIMAL(18, 4),          -- 参考金额 (ref_amt)
    unique_lvl VARCHAR(255),
    year INT,                        -- 派生自 acct_period
    bus_line_name VARCHAR(255),
    allocated_amount DECIMAL(18, 4),
    audit_status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 业务线应收中间表
-- 列名与 fact_receivable 保持一致，增加 bus_line_name / allocated_amount
-- fact_receivable 无 cust_name/mat_name/year，ar_balance 为应收余额金额
CREATE TABLE IF NOT EXISTS public.staging_bus_receivable (
    id SERIAL PRIMARY KEY,
    source_no VARCHAR(255),
    prim_org VARCHAR(255),
    sec_org VARCHAR(255),
    third_org VARCHAR(255),
    acct_period DATE,                -- 会计期间
    ar_balance DECIMAL(18, 4),       -- 应收账款余额 (ar_balance)
    unique_lvl VARCHAR(255),
    year INT,                        -- 派生自 acct_period
    bus_line_name VARCHAR(255),
    allocated_amount DECIMAL(18, 4),
    audit_status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 业务线在途存货中间表
-- 列名与 fact_inventory_on_way 保持一致，增加 bus_line_name / allocated_amount
-- fact_inventory_on_way 无 year 列，由任务层派生
CREATE TABLE IF NOT EXISTS public.staging_bus_in_transit_inventory (
    id SERIAL PRIMARY KEY,
    source_no VARCHAR(255),
    prim_org VARCHAR(255),
    sec_org VARCHAR(255),
    third_org VARCHAR(255),
    acct_period DATE,                -- 会计期间
    mat_name VARCHAR(255),           -- 物料名称 (mat_name)
    order_amount DECIMAL(18, 4),     -- 订单金额 (order_amount)
    unique_lvl VARCHAR(255),
    year INT,                        -- 派生自 acct_period
    bus_line_name VARCHAR(255),
    allocated_amount DECIMAL(18, 4),
    audit_status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
