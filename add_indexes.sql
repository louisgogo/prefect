-- 为 Staging 表添加性能优化索引
-- 这些索引可以显著提升按日期和业务线查询的速度

-- staging_bus_expense 索引
CREATE INDEX IF NOT EXISTS idx_staging_bus_expense_acct_period ON staging_bus_expense(acct_period);
CREATE INDEX IF NOT EXISTS idx_staging_bus_expense_bus_line ON staging_bus_expense(bus_line);
CREATE INDEX IF NOT EXISTS idx_staging_bus_expense_unique_lvl ON staging_bus_expense(unique_lvl);

-- staging_bus_revenue 索引
CREATE INDEX IF NOT EXISTS idx_staging_bus_revenue_acct_period ON staging_bus_revenue(acct_period);
CREATE INDEX IF NOT EXISTS idx_staging_bus_revenue_bus_line ON staging_bus_revenue(bus_line);
CREATE INDEX IF NOT EXISTS idx_staging_bus_revenue_unique_lvl ON staging_bus_revenue(unique_lvl);

-- staging_bus_profit_bd 索引
CREATE INDEX IF NOT EXISTS idx_staging_bus_profit_bd_date ON staging_bus_profit_bd(date);
CREATE INDEX IF NOT EXISTS idx_staging_bus_profit_bd_bus_line ON staging_bus_profit_bd(bus_line);
CREATE INDEX IF NOT EXISTS idx_staging_bus_profit_bd_unique_lvl ON staging_bus_profit_bd(unique_lvl);

-- staging_bus_inventory 索引
CREATE INDEX IF NOT EXISTS idx_staging_bus_inventory_acct_period ON staging_bus_inventory(acct_period);
CREATE INDEX IF NOT EXISTS idx_staging_bus_inventory_bus_line ON staging_bus_inventory(bus_line);
CREATE INDEX IF NOT EXISTS idx_staging_bus_inventory_unique_lvl ON staging_bus_inventory(unique_lvl);

-- staging_bus_receivable 索引
CREATE INDEX IF NOT EXISTS idx_staging_bus_receivable_acct_period ON staging_bus_receivable(acct_period);
CREATE INDEX IF NOT EXISTS idx_staging_bus_receivable_bus_line ON staging_bus_receivable(bus_line);
CREATE INDEX IF NOT EXISTS idx_staging_bus_receivable_unique_lvl ON staging_bus_receivable(unique_lvl);

-- staging_bus_in_transit_inventory 索引
CREATE INDEX IF NOT EXISTS idx_staging_bus_in_transit_acct_period ON staging_bus_in_transit_inventory(acct_period);
CREATE INDEX IF NOT EXISTS idx_staging_bus_in_transit_bus_line ON staging_bus_in_transit_inventory(bus_line);
CREATE INDEX IF NOT EXISTS idx_staging_bus_in_transit_unique_lvl ON staging_bus_in_transit_inventory(unique_lvl);

-- 为源表添加索引（如果还没有的话）
CREATE INDEX IF NOT EXISTS idx_fact_expense_acct_period ON fact_expense(acct_period);
CREATE INDEX IF NOT EXISTS idx_fact_revenue_acct_period ON fact_revenue(acct_period);
CREATE INDEX IF NOT EXISTS idx_fact_profit_bd_date ON fact_profit_bd(date);
CREATE INDEX IF NOT EXISTS idx_fact_inventory_acct_period ON fact_inventory(acct_period);
CREATE INDEX IF NOT EXISTS idx_fact_receivable_acct_period ON fact_receivable(acct_period);
CREATE INDEX IF NOT EXISTS idx_fact_inventory_on_way_acct_period ON fact_inventory_on_way(acct_period);
