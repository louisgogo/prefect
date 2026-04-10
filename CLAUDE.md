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

## Important Notes

- **必须使用虚拟环境**：所有开发和工具都在 `venv` 中运行
- Database: PostgreSQL (accessed via SQLAlchemy/psycopg2)
- Default date behavior: Most flows default to "last month" if no date specified
- Budget update has special date logic: Nov-Feb → annual budget, Apr-Jul → mid-year budget
- Memory management: Always use `months` parameter for multi-month processing to avoid memory issues

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
