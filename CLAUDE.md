# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

```bash
# 1. 激活虚拟环境（必须）
source venv/bin/activate

# 2. 安装/更新依赖
pip install -r requirements.txt

# 3. 配置 pre-commit hooks（首次或更新 .pre-commit-config.yaml 后）
pre-commit install

# 4. 手动运行 pre-commit 检查
pre-commit run --all-files
```

## Project Overview

This is a **Prefect 2.x** workflow orchestration project for business line accounting (业务线核算). It manages ETL pipelines that calculate profit/loss, import data from Excel, update budgets from FONE, and perform intercompany reconciliation.

## Environment Variables

项目根目录的 `.env` 文件会自动加载，无需每次手动设置。Claude Code 启动时会自动读取。

当前配置的环境变量：
- `PREFECT_API_URL=http://127.0.0.1:4200/api`

如需添加其他变量，直接编辑 `.env` 文件即可。

## Virtual Environment

项目使用 `venv` 目录存放虚拟环境，所有开发和工具都应在虚拟环境中进行。

```bash
# 激活虚拟环境
source venv/bin/activate

# 退出虚拟环境
deactivate
```

> 注意：Claude Code 执行命令前会自动检测并使用项目虚拟环境。

## Common Commands

### Development

```bash
# 激活虚拟环境后安装依赖
source venv/bin/activate
pip install -r requirements.txt

# Start local Prefect Server
prefect server start

# Deploy flows for local testing (runs multiple flow servers in parallel)
python deploy_local.py

# Deploy to production (with default parameters for last month)
python deploy_production.py
```

### Server Deployment

```bash
# On the server (10.18.8.191), run flows locally connected to server Prefect
export PREFECT_API_URL=http://127.0.0.1:4200/api
python deploy_to_server.py

# Or use the convenience script
./run_workers_on_server.sh

# Check Prefect configuration
prefect config view
prefect config get PREFECT_API_URL

# List registered flows/deployments
prefect flow ls
prefect deployment ls
```

### 修改代码后重启服务（重要）

**每次修改代码后进行测试前，必须重启 Prefect 服务**，否则运行的是旧代码缓存。

Prefect 涉及两个服务：

1. **Prefect Server**（调度服务）- 通常不需要重启
2. **Prefect Workers**（任务执行服务）- **必须重启**

⚠️ **新增/删除 flow 时，必须同步修改部署脚本**

> `deploy_local.py` 和 `deploy_to_server.py` 里通过 `flow.serve()` 将每个 flow 注册到 Prefect Server。**只写 flow 代码、不修改部署脚本，UI 里看不到新 flow**；同理，删除 flow 后也要从部署脚本里移除，否则会持续推送已废弃的流程。
>
> 修改完部署脚本后，再重启 workers 服务才能生效。**光重启服务、不改部署脚本 = 没有变化。**

⚠️ **生产环境必须使用 systemd 服务**（已注册为系统服务），**禁止**使用 `pkill` + `nohup` 或手动运行 `python deploy_to_server.py`，否则会导致：
- 多个进程冲突
- 代码更新后无法自动拉取
- 服务异常退出后无法自动恢复

```bash
# 生产环境（服务器 10.18.8.191）- 使用 systemctl
sudo systemctl restart prefect-workers

# 验证服务状态
sudo systemctl status prefect-server
sudo systemctl status prefect-workers

# 查看日志
sudo journalctl -u prefect-workers -f
```

### Systemd Service Management (Production)

服务器已注册两个 systemd 服务，**所有启停操作必须通过 systemctl**：

| 服务名 | 功能 | 状态检查 |
|--------|------|----------|
| `prefect-server.service` | Prefect Server (API & UI, port 4200) | `systemctl status prefect-server` |
| `prefect-workers.service` | Prefect Workers (flow.serve) | `systemctl status prefect-workers` |

**常用命令**：

```bash
# 查看服务状态
sudo systemctl status prefect-server
sudo systemctl status prefect-workers

# 启动/停止/重启
sudo systemctl start prefect-server      # 通常保持运行，不需重启
sudo systemctl restart prefect-workers   # 代码更新后执行
sudo systemctl stop prefect-workers

# 查看日志
sudo journalctl -u prefect-server -f
sudo journalctl -u prefect-workers -f

# 服务配置路径（如需修改）
/etc/systemd/system/prefect-server.service
/etc/systemd/system/prefect-workers.service
```

**workers 服务特点**：
- 启动前自动执行 `git pull` 拉取最新代码
- 配置了 `Restart=always`，异常退出后自动重启
- 依赖 `prefect-server.service`，确保 Server 先启动

> **环境区分**
> - **生产环境（服务器 10.18.8.191）**：必须使用 `systemctl`，禁止手动启停进程
> - **本地开发**：使用 `python deploy_local.py`，没有 systemd 服务

## Architecture

### Module Structure

All flows are organized under `modules/` following a consistent pattern:

```
modules/
├── __init__.py           # Exports all flows for easy importing
├── bus_line_cal/         # Main profit calculation module
│   ├── flows/            # Flow definitions (orchestration logic)
│   │   ├── business_line_profit_flow.py   # Main entry flow
│   │   ├── revenue_expense_profit_flow.py
│   │   └── asset_detail_flow.py
│   └── tasks/            # Task definitions (business logic)
│       ├── revenue_tasks.py
│       ├── expense_tasks.py
│       ├── profit_tasks.py
│       └── asset_tasks.py
├── bus_line_staging/     # EAV-format staging data extraction
├── data_import/          # Excel to PostgreSQL data import
├── budget_update/        # FONE budget integration
├── shared_rate/          # Shared rate calculations
├── profit_refresh/       # Profit table refresh
└── recon/                # Intercompany reconciliation
```

### Flow Relationships

**Main orchestration flow**: `business_line_profit_flow`
1. Calls `fetch_budget_shared_rate_flow()` first (ensures latest rates)
2. Loops through months calling:
   - `revenue_expense_profit_flow()` - Income/expense/profit details
   - `asset_detail_flow()` - Asset details (AR, inventory, in-transit)
3. Finally calls `profit_refresh_flow()` - Refreshes profit tables

**Independent flows**:
- `data_import_flow` - Imports Excel data (manual or scheduled)
- `budget_update_flow` - Updates budget from FONE (manual trigger)
- `recon_flow` - Monthly reconciliation process

### Key Parameters Pattern

Most flows accept either `month` (single) or `months` (list) parameters:

```python
# Single month
business_line_profit_flow(year=2025, month=3)

# Batch processing (recommended for memory management)
business_line_profit_flow(year=2025, months=[1, 2, 3])
```

### External Dependency: mypackage

This project depends on an internal package `mypackage` (not in PyPI) for database connections and utilities. It's referenced in requirements.txt but must be provided via:
- Local editable install: `-e /path/to/mypackage`
- Git URL: `git+https://.../mypackage.git@main`
- Built package: `mypackage==0.1.0`

See `docs/多项目共用mypackage_打包与使用.md` for packaging instructions.

### Deployment Model

**Local Development**: `deploy_local.py` uses multiprocessing to run multiple `flow.serve()` processes in parallel. Each flow is available at `http://127.0.0.1:4200` for manual triggering.

**Production Server**: Code runs directly on the server (10.18.8.191). The `deploy_to_server.py` script connects to `http://127.0.0.1:4200/api` (local server), making flows execute on the server when triggered via UI.

**Prefect API URL Resolution** (in order of priority):
1. Environment variable `PREFECT_API_URL`
2. Config file `~/.prefect/config.toml`
3. Default: `http://127.0.0.1:4200/api`

### Staging Flow Architecture

`bus_line_staging_flow` extracts data in EAV (Entity-Attribute-Value) format:
- Revenue data → `staging_revenue_unassigned`
- Expense data → `staging_expense`
- Asset data → `staging_asset`
- Unassigned items → `staging_revenue_unassigned`

Configuration in `modules/bus_line_staging/config.py` defines business lines and organizational groups (backend/middle/frontend centers).

## File Naming Conventions

- Flow files: `{name}_flow.py` in `flows/` directories
- Task files: `{name}_tasks.py` in `tasks/` directories
- Deployment scripts: `deploy_{environment}.py`
- Documentation: Chinese filenames with deployment instructions

## Code Quality (pre-commit)

项目配置了 `pre-commit` hooks 来保证代码质量，每次 `git commit` 会自动运行以下检查：

| Hook | 作用 |
|------|------|
| trailing-whitespace | 去除行尾空格 |
| end-of-file-fixer | 确保文件以空行结尾 |
| check-yaml/json/toml | 配置文件语法检查 |
| check-ast | Python 语法检查 |
| check-merge-conflict | 防止提交合并冲突标记 |
| debug-statements | 禁止遗留 `breakpoint()` / `print()` |
| check-added-large-files | 限制文件大小 < 1MB |
| black | 代码自动格式化 |
| isort | 导入语句自动排序 |
| flake8 | 代码风格检查 |

### 常用命令

```bash
# 手动运行所有检查
pre-commit run --all-files

# 仅检查特定文件
pre-commit run --files modules/bus_line_cal/flows/*.py

# 跳过 hooks 提交（紧急情况下使用）
git commit --no-verify -m "紧急修复"

# 更新 hooks 版本
pre-commit autoupdate
```

## Skills (专项技能)

### Power Query 解析技能 (pq-extractor)

当需要从 Excel 文件中提取 Power Query (M代码) 逻辑并转换为 Python/pandas 代码时，使用此技能。

**加载方式**：
```
/skill pq-extractor
# 或直接提及 Power Query, M代码 等相关关键词
```

**使用场景**：
- 解析 Excel 中 Power Query 的数据处理逻辑
- 将 M 代码转换为 pandas 等价实现
- 理解 Table.SelectRows, Table.Join, Table.Group, Table.Unpivot 等操作

**核心文件位置**：
- Skill 路径：`/root/.claude/plugins/cache/pq-extractor/1.0.0/skills/pq-extractor/SKILL.md`
- 项目内备份：`/root/prefect/claude-skills/pq-extractor/SKILL.md`
- ExcelToPython 框架：`/root/prefect/ExcelToPython/`

**快速使用**：
```python
# 1. 提取 M 代码
from ExcelToPython.extractor import extract_m_code_from_excel
m_codes = extract_m_code_from_excel("your_file.xlsx", output_dir="extracted_m_codes")

# 2. 查看提取的 M 代码
for name, code in m_codes.items():
    print(f"=== {name} ===")
    print(code)
```

**常见 M 代码 → pandas 对照**：
| M 代码 | pandas 等价代码 |
|--------|----------------|
| `Table.SelectRows(源, each [列] = 值)` | `df[df['列'] == 值]` |
| `Table.Group(源, {"分组列"}, {{"合计", each List.Sum([金额])}})` | `df.groupby('分组列')['金额'].sum().reset_index()` |
| `Table.NestedJoin(左, {"键"}, 右, {"键"}, "新列", JoinKind.LeftOuter)` | `df.merge(right_df, on='键', how='left')` |
| `Table.UnpivotOtherColumns(源, {"保留列"}, "属性", "值")` | `df.melt(id_vars=['保留列'], var_name='属性', value_name='值')` |

完整对照表参见 skill 文档。

---

## Important Notes

- **必须使用虚拟环境**：所有开发和工具都在 `venv` 中运行
- Database: PostgreSQL (accessed via SQLAlchemy/psycopg2)
- Default date behavior: Most flows default to "last month" if no date specified
- Budget update has special date logic: Nov-Feb → annual budget, Apr-Jul → mid-year budget
- Memory management: Always use `months` parameter for multi-month processing to avoid memory issues

---

## Database Schema Reference

数据库: `mydb` (PostgreSQL)。以下为业务相关核心表结构，用于后续数据核对和脚本开发参考。

### 数据分层流程

本系统的数据流程分为三层：

1. **Fact 表（中间层，不带 bus）**
   - `fact_revenue`、`fact_expense`、`fact_inventory`、`fact_inventory_on_way`、`fact_receivable`、`fact_profit`、`fact_profit_bd`
   - 存储从各公司 Excel 上报数据中提取的**原始/汇总数据**，不含业务线拆分结果。
   - `report_collection_flow` 生成的 CSV 数据应与这一层对齐。

2. **Staging 表（拆分规则层）**
   - `staging_bus_revenue`、`staging_bus_expense` 等
   - 存储**无法直接归属业务线的残余数据**或拆分规则，用于后续的 EAV 拆分计算。
   - 例如 `staging_bus_revenue` 只保留"无归属收入"和"非业务线收入"。

3. **Fact_bus 表（最终层，带 bus）**
   - `fact_bus_revenue`、`fact_bus_expense`、`fact_bus_inventory` 等
   - 存储**经过 staging 拆分后，带有 `bus_line` 的最终业务线数据**。

### Report Collection 子表与数据库表映射

| Report Collection 子表 | 对应 Fact 表（不带 bus） | 对应 Staging 表 | 对应 Fact_bus 表（带 bus） |
|------------------------|-------------------------|----------------|---------------------------|
| 2-2收入成本明细 | `fact_revenue` | `staging_bus_revenue` | `fact_bus_revenue` |
| 2-3收单明细 | (暂无对应表，需新建) | (暂无对应表，需新建) | (暂无对应表，需新建) |
| 2-5应收明细 | `fact_receivable` | `staging_bus_receivable` | `fact_bus_receivable` |
| 2-6存货明细 | `fact_inventory` | `staging_bus_inventory` | `fact_bus_inventory` |
| 2-13在途存货 | `fact_inventory_on_way` | `staging_bus_in_transit_inventory` | `fact_bus_inventory_on_way` |
| 2-4费用明细 | `fact_expense` | `staging_bus_expense` | `fact_bus_expense` |
| 2-1利润拆分 | `fact_profit_bd` | `staging_bus_profit_bd` | `fact_bus_profit_bd` |

### 核心 Fact 表结构（中间层，不带 bus）

#### public.fact_revenue

| 列名 | 说明 |
|------|------|
| `id` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `doc_no` | |
| `bus_date` | |
| `cust_code` | |
| `customer` | |
| `cust_cat` | |
| `cust_region` | |
| `is_internal_sale` | |
| `inc_major_cat` | |
| `prod_major_cat` | |
| `currency` | |
| `exchange_rate` | |
| `mat_code` | |
| `mat_name` | |
| `sales_qty` | |
| `tax_incl_price` | |
| `orig_curr_amt` | |
| `tax_amt` | |
| `tax_rate` | |
| `tax_incl_det` | |
| `amt_tax_exc_loc` | |
| `cost_amt` | |
| `freight_cost` | |
| `prod_margin` | |
| `acct_period` | |
| `year` | |
| `unique_lvl` | |
| `remarks` | |
| `soft_cost` | |
| `tariff_cost` | |
| `source_no` | |
| `last_modified` | |
| `custgp_code` | |
| `custgp_name` | |

#### public.fact_expense

| 列名 | 说明 |
|------|------|
| `id` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `proj_code` | |
| `rd_proj` | |
| `date` | |
| `voucher_no` | |
| `summary` | |
| `doc_no` | |
| `prim_subj` | |
| `exp_item_code` | |
| `acc_proj_cost` | |
| `exp_amt` | |
| `orig_prim_dept` | |
| `orig_sec_dept` | |
| `orig_third_dept` | |
| `acct_period` | |
| `year` | |
| `exp_major_cat` | |
| `exp_nature` | |
| `sec_subj` | |
| `unique_lvl` | |
| `product_sub_category` | |
| `source_no` | |
| `last_modified` | |
| `dist_bus_line` | |
| `claimant` | |
| `reporting_org_code` | |

#### public.fact_inventory

| 列名 | 说明 |
|------|------|
| `id` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `mat_code` | |
| `mat_name` | |
| `inv_cat` | |
| `cust_cat` | |
| `cust_code` | |
| `cust_name` | |
| `warehouse` | |
| `is_stock_mat` | |
| `qty_inv` | |
| `ref_price_base` | |
| `ref_amt` | |
| `qty_6m_less` | |
| `amt_6m_less` | |
| `qty_6_9m` | |
| `amt_6_9m` | |
| `qty_9m_1y` | |
| `amt_9m_1y` | |
| `qty_1_2y` | |
| `amt_1_2y` | |
| `qty_2_3y` | |
| `amt_2_3y` | |
| `qty_3y_plus` | |
| `amt_3y_plus` | |
| `acct_period` | |
| `unique_lvl` | |
| `source_no` | |

#### public.fact_inventory_on_way

| 列名 | 说明 |
|------|------|
| `id` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `order_number` | |
| `order_date` | |
| `supplier_code` | |
| `supplier_name` | |
| `inv_cat` | |
| `mat_code` | |
| `mat_name` | |
| `order_amount` | |
| `total_payment_amount` | |
| `order_count` | |
| `unit_price` | |
| `currency` | |
| `exchange_rate` | |
| `delivery_date` | |
| `acct_period` | |
| `unique_lvl` | |
| `total_inventory_received` | |
| `fin_con` | |
| `fin_ind` | |
| `source_no` | |
| `unreceived_inventory` | |

#### public.fact_receivable

| 列名 | 说明 |
|------|------|
| `id` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `prim_subj` | |
| `cust_code` | |
| `txn_unit` | |
| `txn_nature` | |
| `cust_type` | |
| `sales_dept` | |
| `sales_region` | |
| `unaudited_sales_amt` | |
| `unaudited_prepay_amt` | |
| `unaudited_inst_amt` | |
| `ar_balance` | |
| `ovd_amt` | |
| `undue_amt` | |
| `ovd_30d_less_amt` | |
| `ovd_30_90d_amt` | |
| `ovd_90_180d_amt` | |
| `ovd_180_360d_amt` | |
| `ovd_360d_plus_amt` | |
| `acct_age_9_12m` | |
| `acct_age_1_2y` | |
| `acct_age_2_3y` | |
| `acct_age_3y_plus` | |
| `yr_debit_occ` | |
| `yr_credit_occ` | |
| `sales_module` | |
| `last_mo_ovd_amt` | |
| `ovd_change` | |
| `yr_repay_amt` | |
| `acct_period` | |
| `unique_lvl` | |
| `bus_major_cat` | |
| `bus_sub_cat` | |
| `source_no` | |
| `ar_status` | |
| `remarks` | |
| `acct_age_3m_less` | |
| `acct_age_3_6m` | |
| `acct_age_6_9m` | |
| `processed_amount` | |
| `mr_debit_occ` | |
| `mr_credit_occ` | |
| `bad_debt_amount` | |

#### public.fact_profit

| 列名 | 说明 |
|------|------|
| `source_no` | |
| `unique_lvl` | |
| `acct_period` | |
| `prim_subj` | |
| `amt` | |
| `fin_con` | |
| `fin_ind` | |
| `class` | |

#### public.fact_profit_bd

| 列名 | 说明 |
|------|------|
| `id` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `prim_subj` | |
| `mo_amt` | |
| `date` | |
| `unique_lvl` | |
| `year` | |
| `remarks` | |
| `source_no` | |
| `last_modified` | |

### 核心 Fact_bus 表结构（最终层，带 bus）

#### public.fact_bus_revenue

| 列名 | 说明 |
|------|------|
| `source_no` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `doc_no` | |
| `bus_date` | |
| `cust_code` | |
| `customer` | |
| `cust_cat` | |
| `cust_region` | |
| `is_internal_sale` | |
| `inc_major_cat` | |
| `prod_major_cat` | |
| `currency` | |
| `exchange_rate` | |
| `mat_code` | |
| `mat_name` | |
| `sales_qty` | |
| `tax_incl_price` | |
| `orig_curr_amt` | |
| `tax_amt` | |
| `tax_rate` | |
| `tax_incl_det` | |
| `prod_margin` | |
| `acct_period` | |
| `year` | |
| `unique_lvl` | |
| `remarks` | |
| `bus_line` | |
| `sec_dist_lvl` | |
| `category` | |
| `rate` | |
| `prim_subj` | |
| `mo_amt` | |
| `id` | |
| `custgp_code` | |
| `custgp_name` | |

#### public.fact_bus_expense

| 列名 | 说明 |
|------|------|
| `source_no` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `proj_code` | |
| `rd_proj` | |
| `date` | |
| `voucher_no` | |
| `summary` | |
| `doc_no` | |
| `prim_subj` | |
| `exp_item_code` | |
| `acc_proj_cost` | |
| `exp_amt` | |
| `orig_prim_dept` | |
| `orig_sec_dept` | |
| `orig_third_dept` | |
| `acct_period` | |
| `year` | |
| `exp_major_cat` | |
| `exp_nature` | |
| `sec_subj` | |
| `unique_lvl` | |
| `product_sub_category` | |
| `bus_line` | |
| `sec_dist_lvl` | |
| `category` | |
| `rate` | |
| `id` | |
| `dist_bus_line` | |
| `claimant` | |
| `reporting_org_code` | |

#### public.fact_bus_inventory

| 列名 | 说明 |
|------|------|
| `id` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `mat_code` | |
| `mat_name` | |
| `inv_cat` | |
| `cust_cat` | |
| `cust_code` | |
| `cust_name` | |
| `warehouse` | |
| `is_stock_mat` | |
| `qty_inv` | |
| `ref_price_base` | |
| `ref_amt` | |
| `qty_6m_less` | |
| `amt_6m_less` | |
| `qty_6_9m` | |
| `amt_6_9m` | |
| `qty_9m_1y` | |
| `amt_9m_1y` | |
| `qty_1_2y` | |
| `amt_1_2y` | |
| `qty_2_3y` | |
| `amt_2_3y` | |
| `qty_3y_plus` | |
| `amt_3y_plus` | |
| `acct_period` | |
| `unique_lvl` | |
| `source_no` | |

#### public.fact_bus_inventory_on_way

| 列名 | 说明 |
|------|------|
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `order_number` | |
| `order_date` | |
| `supplier_code` | |
| `supplier_name` | |
| `inv_cat` | |
| `mat_code` | |
| `mat_name` | |
| `order_amount` | |
| `total_payment_amount` | |
| `order_count` | |
| `unit_price` | |
| `currency` | |
| `exchange_rate` | |
| `delivery_date` | |
| `acct_period` | |
| `unique_lvl` | |
| `total_inventory_received` | |
| `fin_con` | |
| `fin_ind` | |
| `source_no` | |
| `id` | |
| `sec_dist_lvl` | |
| `rate` | |
| `category` | |
| `bus_line` | |
| `unreceived_inventory` | |

#### public.fact_bus_receivable

| 列名 | 说明 |
|------|------|
| `source_no` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `prim_subj` | |
| `cust_code` | |
| `txn_unit` | |
| `txn_nature` | |
| `cust_type` | |
| `sales_dept` | |
| `sales_region` | |
| `unaudited_sales_amt` | |
| `unaudited_prepay_amt` | |
| `unaudited_inst_amt` | |
| `ar_balance` | |
| `ovd_amt` | |
| `undue_amt` | |
| `ovd_30d_less_amt` | |
| `ovd_30_90d_amt` | |
| `ovd_90_180d_amt` | |
| `ovd_180_360d_amt` | |
| `ovd_360d_plus_amt` | |
| `acct_age_9_12m` | |
| `acct_age_1_2y` | |
| `acct_age_2_3y` | |
| `acct_age_3y_plus` | |
| `yr_debit_occ` | |
| `yr_credit_occ` | |
| `sales_module` | |
| `last_mo_ovd_amt` | |
| `ovd_change` | |
| `yr_repay_amt` | |
| `acct_period` | |
| `unique_lvl` | |
| `bus_major_cat` | |
| `bus_sub_cat` | |
| `id` | |
| `sec_dist_lvl` | |
| `rate` | |
| `category` | |
| `bus_line` | |
| `ar_status` | |
| `remarks` | |
| `acct_age_3m_less` | |
| `acct_age_3_6m` | |
| `acct_age_6_9m` | |
| `processed_amount` | |
| `mr_debit_occ` | |
| `mr_credit_occ` | |
| `bad_debt_amount` | |

#### public.fact_bus_profit_bd

| 列名 | 说明 |
|------|------|
| `source_no` | |
| `fin_con` | |
| `fin_ind` | |
| `prim_org` | |
| `sec_org` | |
| `third_org` | |
| `prim_subj` | |
| `mo_amt` | |
| `date` | |
| `unique_lvl` | |
| `year` | |
| `remarks` | |
| `bus_line` | |
| `sec_dist_lvl` | |
| `category` | |
| `rate` | |
| `id` | |

#### public.fact_bus_profit

| 列名 | 说明 |
|------|------|
| `source_no` | |
| `unique_lvl` | |
| `acct_period` | |
| `prim_subj` | |
| `amt` | |
| `bus_line` | |
| `fin_con` | |
| `fin_ind` | |
| `id` | |

#### public.fact_bus_line

| 列名 | 说明 |
|------|------|
| `id` | |
| `source_no` | |
| `sec_dist_lvl` | |
| `bus_line` | |
| `rate` | |
| `class` | |
| `acct_period` | |
| `unique_lvl` | |
| `category` | |

#### public.fact_bus_shared_rate

| 列名 | 说明 |
|------|------|
| `id` | |
| `date` | |
| `bus_line` | |
| `rate` | |

### 核心 Staging 表结构

#### public.staging_bus_revenue

| 列名 | 说明 |
|------|------|
| `id` | |
| `唯一层级` | |
| `一级组织` | |
| `二级组织` | |
| `三级组织` | |
| `来源编号` | |
| `会计期间` | |
| `收入大类` | |
| `产品大类` | |
| `物料名称` | |
| `不含税金额本位币` | |
| `成本金额` | |
| `运费成本` | |
| `关税成本` | |
| `软件成本` | |
| `年份` | |
| `数据来源` | |
| `AGI` | |
| `国内硬件` | |
| `国际业务` | |
| `大POS` | |
| `审核业务` | |
| `小POS` | |
| `政府消费券` | |
| `数币` | |
| `本地生活` | |
| `消费电子` | |
| `澳门业务` | |
| `立充` | |
| `美国业务` | |
| `能源硬件` | |
| `能源运营` | |
| `资产运营` | |
| `跨境总部` | |
| `跨境新加坡` | |
| `跨境欧洲` | |
| `审核状态` | |
| `创建时间` | |

#### public.staging_bus_expense

| 列名 | 说明 |
|------|------|
| `id` | |
| `唯一层级` | |
| `一级组织` | |
| `二级组织` | |
| `三级组织` | |
| `来源编号` | |
| `单据编号` | |
| `报销人` | |
| `摘要` | |
| `会计期间` | |
| `费用性质` | |
| `费用大类` | |
| `核算项目-费控` | |
| `研发项目` | |
| `项目编码` | |
| `费用金额` | |
| `年份` | |
| `数据来源` | |
| `分摊业务线` | |
| `AGI` | |
| `国内硬件` | |
| `国际业务` | |
| `大POS` | |
| `审核业务` | |
| `小POS` | |
| `政府消费券` | |
| `数币` | |
| `本地生活` | |
| `消费电子` | |
| `澳门业务` | |
| `立充` | |
| `美国业务` | |
| `能源硬件` | |
| `能源运营` | |
| `资产运营` | |
| `跨境总部` | |
| `跨境新加坡` | |
| `跨境欧洲` | |
| `审核状态` | |
| `创建时间` | |

#### public.staging_bus_inventory

| 列名 | 说明 |
|------|------|
| `id` | |
| `唯一层级` | |
| `一级组织` | |
| `二级组织` | |
| `三级组织` | |
| `来源编号` | |
| `财报合并` | |
| `财报单体` | |
| `物料编码` | |
| `物料名称` | |
| `存货类别` | |
| `客户类别` | |
| `客户编码` | |
| `客户名称` | |
| `仓库` | |
| `是否为备货物料` | |
| `数量(库存)` | |
| `参考价(基本)` | |
| `参考金额` | |
| `6个月以内数量` | |
| `6个月以内金额` | |
| `6-9个月数量` | |
| `6-9个月金额` | |
| `9个月-1年数量` | |
| `9个月-1年金额` | |
| `1-2年数量` | |
| `1-2年金额` | |
| `2-3年数量` | |
| `2-3年金额` | |
| `3年以上数量` | |
| `3年以上金额` | |
| `会计期间` | |
| `年份` | |
| `数据来源` | |
| `AGI` | |
| `国内硬件` | |
| `国际业务` | |
| `大POS` | |
| `审核业务` | |
| `小POS` | |
| `政府消费券` | |
| `数币` | |
| `本地生活` | |
| `消费电子` | |
| `澳门业务` | |
| `立充` | |
| `美国业务` | |
| `能源硬件` | |
| `能源运营` | |
| `资产运营` | |
| `跨境总部` | |
| `跨境新加坡` | |
| `跨境欧洲` | |
| `审核状态` | |
| `创建时间` | |

#### public.staging_bus_in_transit_inventory

| 列名 | 说明 |
|------|------|
| `id` | |
| `唯一层级` | |
| `一级组织` | |
| `二级组织` | |
| `三级组织` | |
| `来源编号` | |
| `财报合并` | |
| `财报单体` | |
| `订单号` | |
| `订单日期` | |
| `供应商编码` | |
| `供应商名称` | |
| `存货类别` | |
| `物料编码` | |
| `物料名称` | |
| `订单金额` | |
| `在途订单金额` | |
| `累计付款金额` | |
| `订单数量` | |
| `累计入库数量` | |
| `未入库数量` | |
| `交货日期` | |
| `会计期间` | |
| `年份` | |
| `数据来源` | |
| `AGI` | |
| `国内硬件` | |
| `国际业务` | |
| `大POS` | |
| `审核业务` | |
| `小POS` | |
| `政府消费券` | |
| `数币` | |
| `本地生活` | |
| `消费电子` | |
| `澳门业务` | |
| `立充` | |
| `美国业务` | |
| `能源硬件` | |
| `能源运营` | |
| `资产运营` | |
| `跨境总部` | |
| `跨境新加坡` | |
| `跨境欧洲` | |
| `审核状态` | |
| `创建时间` | |

#### public.staging_bus_receivable

| 列名 | 说明 |
|------|------|
| `id` | |
| `唯一层级` | |
| `一级组织` | |
| `二级组织` | |
| `三级组织` | |
| `来源编号` | |
| `财报合并` | |
| `财报单体` | |
| `一级科目` | |
| `客户编码` | |
| `往来单位` | |
| `往来性质` | |
| `客户类型` | |
| `销售部门` | |
| `销售区域` | |
| `赊销未核金额` | |
| `预收未核金额` | |
| `分期未核金额` | |
| `应收账款余额` | |
| `逾期金额` | |
| `未到期金额` | |
| `逾期30天以内金额` | |
| `逾期30天到90天金额` | |
| `逾期90天到180天金额` | |
| `逾期180天到360天金额` | |
| `逾期360天以上金额` | |
| `账龄3个月以内` | |
| `账龄3-6个月` | |
| `账龄6-9个月` | |
| `账龄9-12个月` | |
| `账龄1-2年` | |
| `账龄2-3年` | |
| `账龄3年以上` | |
| `本年借方发生额` | |
| `本年贷方发生额` | |
| `销售模块` | |
| `上个月逾期金额` | |
| `逾期变动` | |
| `本年回款金额` | |
| `会计期间` | |
| `业务大类` | |
| `业务小类` | |
| `应收状态` | |
| `备注` | |
| `年份` | |
| `数据来源` | |
| `AGI` | |
| `国内硬件` | |
| `国际业务` | |
| `大POS` | |
| `审核业务` | |
| `小POS` | |
| `政府消费券` | |
| `数币` | |
| `本地生活` | |
| `消费电子` | |
| `澳门业务` | |
| `立充` | |
| `美国业务` | |
| `能源硬件` | |
| `能源运营` | |
| `资产运营` | |
| `跨境总部` | |
| `跨境新加坡` | |
| `跨境欧洲` | |
| `审核状态` | |
| `创建时间` | |

#### public.staging_bus_profit_bd

| 列名 | 说明 |
|------|------|
| `id` | |
| `唯一层级` | |
| `一级组织` | |
| `二级组织` | |
| `三级组织` | |
| `来源编号` | |
| `财报合并` | |
| `日期` | |
| `一级科目` | |
| `本月金额` | |
| `年份` | |
| `数据来源` | |
| `AGI` | |
| `国内硬件` | |
| `国际业务` | |
| `大POS` | |
| `审核业务` | |
| `小POS` | |
| `政府消费券` | |
| `数币` | |
| `本地生活` | |
| `消费电子` | |
| `澳门业务` | |
| `立充` | |
| `美国业务` | |
| `能源硬件` | |
| `能源运营` | |
| `资产运营` | |
| `跨境总部` | |
| `跨境新加坡` | |
| `跨境欧洲` | |
| `审核状态` | |
| `创建时间` | |

### 其他常用表

| 表名 | 用途 |
|------|------|
| `map_translate` | |
| `dim_bus_line` | |
| `dim_org_struc` | |
| `dim_material_master` | |
| `dim_prim_acc` | |
| `dim_customer_info` | |
| `dim_exp_item` | |
| `dim_prod_cat` | |
| `dim_inv_cat` | |
| `dim_region` | |
| `dim_txn_nat` | |
| `dim_date` | |
| `bud_income` | |
| `bud_expense` | |
| `bud_profit` | |
| `bud_cash_flow` | |
| `recon_result_sales` | |
| `recon_result_wanglai` | |
| `recon_result_cashflow` | |

---

## 变更日志 (Changelog)

### 2026-04-10 - recon_flow 数据源自动同步

**背景**：往来对账流程(`recon_flow`)原本需要手动从共享盘 `2-往来对账填报表` 复制文件到 `9-数据源`，操作繁琐且容易遗漏。

**变更内容**：
1. **新增 Task**：`modules/recon/tasks/recon_fetch_tasks.py` 新增 `sync_data_source_task()`
   - 自动从 `2-往来对账填报表` 扫描所有子目录
   - 只保留 **修改日期为 2026年4月** 的文件（47个）
   - 清理 `9-数据源` 旧文件后重新同步

2. **流程改造**：`modules/recon/flows/recon_flow.py`
   - 新增【阶段0】数据源同步（在阶段1采集之前自动执行）
   - 流程结构：阶段0同步 → 阶段1采集 → 阶段2核对

**影响范围**：
- `recon_flow` 运行时自动同步数据源，无需手动操作
- 共享盘路径兼容 Windows(`Z:\`) 和 Linux(`/mnt/xgd_share/`)

**部署状态**：
- ✅ 代码已提交，pre-commit 检查通过
- ✅ `prefect-workers.service` 已重启，变更已生效
- ✅ 服务运行正常 (10.18.8.191)

### 2026-04-10 - recon_flow 排除房租文件

**背景**：阶段0同步时，将文件名包含"房租"的统计表也同步到了数据源，但这些表格不是往来对账的主体数据。

**变更内容**：
- `sync_data_source_task()` 新增排除规则：文件名包含"房租"的文件跳过不同步
- 已识别3个房租相关文件（原始填报表中），其中2个为4月更新会被同步

**影响范围**：
- 下次运行 `recon_flow` 时，数据源中不会包含房租统计表
- 数据源更精简，避免无关数据干扰对账

**部署状态**：
- ✅ 代码已提交并推送
- ✅ `prefect-workers.service` 已重启
- ✅ 服务运行正常
