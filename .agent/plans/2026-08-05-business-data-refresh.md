# Automate business-report reference and acquiring data refreshes

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Finance users currently depend on a manually executed notebook to replace customer, material, R&D project, and acquiring transaction data. Supplier validation has no dedicated master at all. After this change, one Prefect deployment named `子流程-业报基础数据更新` will refresh five logical datasets from their authoritative sources, run every day at 06:00 Asia/Shanghai, and also support self-service runs from the FastAPI business-report data-sync dialog.

The observable result is a durable flow with per-dataset task states and aggregate counts. A user can update all datasets or retry one failed dataset without logging into the server or opening a notebook. External reads finish and pass validation before PostgreSQL target replacement starts, so a source or network failure cannot leave a target table empty.

## Progress

- [x] (2026-08-05 08:45Z) Inspected the legacy supplemental-data notebook and identified five logical datasets and eight target tables.
- [x] (2026-08-05 08:45Z) Verified the Kingdee `BD_Supplier` API and Holdings organization `1000` coverage for all 324 inventory-in-transit supplier codes.
- [x] (2026-08-05 08:45Z) Created `/root/worktrees/prefect/business-data-refresh` from the actual deployed integration branch `origin/session/prefect`.
- [x] (2026-08-05 09:31Z) Added source connectors, normalization, row-drop and duplicate guards, transactional replacement helpers, and eight focused unit tests.
- [x] (2026-08-05 09:31Z) Added five dataset tasks and the unified flow with durable per-dataset state and partial-failure reporting.
- [x] (2026-08-05 09:31Z) Exported the flow and registered `子流程-业报基础数据更新` in all three deployment scripts with the 06:00 Asia/Shanghai schedule.
- [x] (2026-08-05 09:31Z) Coordinated the FastAPI schema, status/trigger API, editor permission gate, supplier validation, and business-report UI integration.
- [x] (2026-08-06 03:20Z) Rechecked the production worker runtime: Kingdee tokens are configured, while SQL Server/Oracle credentials and drivers are absent.
- [x] (2026-08-06 03:20Z) Changed the scheduled server/production default to `supplier` only so the daily run cannot fail four unconfigured datasets.
- [ ] Merge the Prefect pull request to `session/prefect`, restart the authorized production worker after the FastAPI schema migration, and verify a selected `supplier` run.

## Surprises & Discoveries

- The deployed Prefect integration branch is `session/prefect`, not `main`. `origin/main` is 131 commits behind and does not contain the current Kingdee voucher or Hermes callback infrastructure.
- The notebook combines unrelated source types: three Kingdee SQL Server views, one Kingdee BillQuery API form, and five Oracle acquiring metric tables.
- The notebook truncates Oracle target tables before external reads finish. A source failure can therefore commit empty targets. The replacement flow must fetch first and swap data transactionally.
- Current production targets contain about 319,000 acquiring rows, 67,000 customers, 104,000 materials, and 663 R&D projects. Full snapshot refreshes are operationally reasonable but require row-drop guards and summary-only logging.
- The Prefect requirements file does not currently declare `oracledb` or `pyodbc`, even though the legacy notebook depends on both source types.
- Holdings supplier organization `1000` contains one enabled status-`D` supplier used by inventory-in-transit. Valid suppliers must therefore include document statuses `C` and `D`, not only `C`.
- The current host does not have a SQL Server ODBC driver, and the Prefect worker environment does not yet contain the SQL Server/Oracle source credentials. Supplier-only test synchronization is possible now; full five-dataset execution requires those worker settings before deployment activation.

## Decision Log

- Decision: Implement one deployment with dataset codes `customer`, `material`, `rd_project`, `supplier`, and `acquiring_metrics`.
  Rationale: One deployment provides a single schedule, lock, UI integration, and audit surface while still allowing individual task retries and selected-dataset manual runs.
  Date/Author: 2026-08-05 / Codex

- Decision: Treat the five Oracle `T_JL_*` targets as one atomic logical dataset.
  Rationale: They represent one acquiring snapshot and must not expose mixed refresh generations.
  Date/Author: 2026-08-05 / Codex

- Decision: Fetch and validate each external snapshot before opening the target replacement transaction.
  Rationale: Network or source failures must leave the previous successful target untouched. Temporary PostgreSQL staging followed by one transaction provides deterministic recovery.
  Date/Author: 2026-08-05 / Codex

- Decision: Run SQL Server customer, material, and R&D extracts sequentially, while allowing the Oracle and Kingdee API tasks to execute independently.
  Rationale: Sequential reads avoid unnecessary pressure on one SQL Server source. Independent sources can still make progress and commit their own validated datasets.
  Date/Author: 2026-08-05 / Codex

- Decision: Mark the overall run failed when any requested dataset fails, while preserving committed successful datasets and returning an explicit per-dataset summary.
  Rationale: Prefect and the UI must surface partial success without discarding unrelated good refreshes.
  Date/Author: 2026-08-05 / Codex

- Decision: Schedule the production deployment daily at 06:00 in `Asia/Shanghai`, and allow business-report editors to trigger all or selected datasets manually.
  Rationale: This is the user-approved self-service and automatic-refresh boundary. Read-only users remain excluded, and a global concurrency lock prevents duplicate runs.
  Date/Author: 2026-08-05 / Codex

- Decision: Default the scheduled server and production deployment to `supplier` until the other source runtimes are configured.
  Rationale: The production worker has the Kingdee token needed by suppliers, but lacks the SQL Server/Oracle credentials, `pyodbc`, `oracledb`, and a SQL Server ODBC driver. A supplier-only default preserves the requested daily synchronization without creating predictable partial failures. Explicit manual dataset selection remains available for later rollout.
  Date/Author: 2026-08-06 / Codex

## Outcomes & Retrospective

Implementation and focused validation are complete. The selected supplier path fetched 25,645 Holdings rows and wrote the same count to `test_mydb` twice, with 25,582 active rows and zero missing codes across the test inventory-in-transit population. The production rollout is now authorized by the user's 2026-08-06 request, but the worker restart and selected production run remain pending until the FastAPI schema migration is installed. Full customer/material/R&D/acquiring execution still awaits worker credentials and the SQL Server ODBC runtime.

## Context and Orientation

The legacy notebook reads Kingdee SQL Server views `V_XGD_BD_CUSTOMER`, `V_XGD_BD_MATERIAL`, and `V_XGD_BD_YFPROJ`, then replaces finance PostgreSQL tables `dim_customer_info`, `dim_material_master`, and `dim_rd_code`. It filters customers to organization `1000`, materials to organizations `1000`, `1700`, and `1200`, and adds the synthetic values `C99`, `PD99`, and the public R&D project row.

The same notebook reads Oracle tables `T_JL_AREA_MERCH_NETIN`, `T_JL_AREA_TRADE`, `T_JL_BRCH_MERCH_NETIN`, `T_JL_BRCH_TERM`, and `T_JL_BRCH_ACTIVITE_MERCH`, then replaces the same lower-case target table names in finance PostgreSQL.

The new supplier source is the existing AIHub Kingdee proxy `BillQuery` with `FormId=BD_Supplier`. It selects use organization `1000` and stores every returned source status in `dim_supplier_info`; only enabled document statuses `C` and `D` are active for business-report validation.

Current Prefect flow packages live under `modules/`. Public flows are exported through package `__init__.py` files and root `modules/__init__.py`, then registered in all three deployment entry points. `modules/common/tasks/notify_hermes_task.py` provides lifecycle notifications. Production workers run `/root/prefect/deploy_to_server.py` under `prefect-workers.service`, load `/root/prefect/.env`, and pull the checked-out `session/prefect` branch before startup.

FastAPI will expose the deployment through `ToolsService`, record or read per-dataset status from finance PostgreSQL, and add a third card to the existing business-report data-sync dialog. The Prefect implementation must therefore return stable dataset codes, counts, watermarks, and error messages.

## Plan of Work

Create `modules/business_data_refresh/` with task and flow packages. Shared task helpers will resolve credentials from environment variables, connect read-only to SQL Server, Oracle, and the Kingdee proxy, normalize source rows, enforce expected columns and unique business keys, and bulk-load PostgreSQL temporary tables.

Implement one task per logical dataset. Customer, material, and R&D tasks use deterministic filtering and synthetic rows, then atomically replace one target each. Supplier uses the FastAPI-created `dim_supplier_info` schema and transactionally marks stale rows inactive before upserting the complete Holdings snapshot. Acquiring metrics fetch all five Oracle tables before opening one finance transaction, copy each snapshot into a temporary table, then replace and read back all five targets together.

Every dataset task will acquire a dataset advisory lock, validate non-empty source data, compare source rows with the previous successful target, reject unexpected large drops unless an explicit server-side override is configured, commit one dataset transaction, and return `dataset`, `source_rows`, `target_rows`, `watermark`, and duration. No task logs individual business rows.

The flow accepts optional `datasets` and `requested_by` parameters. It creates a run record through the finance status tables supplied by the FastAPI migration, executes requested tasks, updates per-dataset states, sends Hermes started/completed/failed notifications, and raises at the end if any dataset failed. The deployment defaults to all datasets and includes a daily 06:00 Asia/Shanghai schedule.

Finally, align package exports and deployment registrations, add dependencies, run unit tests and formatting, and verify deployment metadata. A representative test run must target `test_mydb`, use read-only external source calls, back up affected test tables, and validate exact target row counts and rerun idempotence. Production worker restart and production target refresh are not authorized in this run.

## Concrete Steps

All commands run in `/root/worktrees/prefect/business-data-refresh` with `source /root/prefect/venv/bin/activate` unless stated otherwise.

1. Add `modules/business_data_refresh/tasks/business_data_refresh_tasks.py`, flow files, package exports, and unit tests.
2. Add `oracledb` and `pyodbc` to `requirements.txt`; verify the host has a compatible SQL Server ODBC driver before a live run.
3. Register `子流程-业报基础数据更新` in `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`. The server/production registration uses daily 06:00 Asia/Shanghai scheduling and defaults to `supplier`; the local registration retains all-dataset development behavior.
4. Run focused unit tests and pre-commit checks. Validate imports and inspect the flow/deployment parameter schema.
5. Commit and push `feature/business-data-refresh`, open a ready pull request to `session/prefect`, merge with a merge commit when checks allow, and delete the remote topic branch.
6. After the FastAPI migration reaches test, update the test Prefect runtime branch/deployment without restarting production workers, trigger a selected-dataset or approved full test run, inspect Flow/Task Runs, and compare database results.

## Validation and Acceptance

Unit tests must cover dataset selection, status-`D` supplier eligibility, duplicate-code rejection, deterministic material organization precedence, row-drop rejection, empty snapshot rejection, API pagination, and overall partial-failure summarization.

Package exports and all deployment scripts must reference the flow. The deployment must expose `datasets` and `requested_by`, default to all five datasets, and declare a daily 06:00 Asia/Shanghai schedule in the server/production registrations.

A safe test run is accepted when every requested task reaches a terminal state, the flow summary reports exact source/target counts, all five acquiring targets share the same completed run generation, all 324 inventory-in-transit supplier codes match active supplier rows, and a second identical run leaves counts and keys unchanged. A failed source prototype must demonstrate that the previous target remains intact.

No production worker restart, production database write, or scheduled production refresh is part of this authorized validation.

## Idempotence and Recovery

External reads and normalization happen before target transactions. SQL Server and Oracle dataset replacements use temporary staging and one transaction, so a failure rolls back to the previous target. Supplier synchronization marks old rows stale and upserts the snapshot in one transaction; rerunning restores current source state.

Dataset-level advisory locks prevent concurrent replacement of the same targets. The flow-level status prevents users from starting overlapping all-dataset runs. Successful datasets remain committed if another dataset fails, and the UI can retry only failed dataset codes.

Before a live test refresh, back up all affected test tables. Row-count and source-watermark guards fail closed on empty or abnormally reduced snapshots. Recovery is forward: fix credentials/source/schema and rerun the failed dataset. No task drops target schemas or permanently deletes backup data.

## Artifacts and Notes

Prototype evidence on 2026-08-05: production finance targets currently contain roughly 319,000 acquiring rows across five tables. The latest acquiring watermark is 2026-07. The supplier API returns one unique row per code for use organization `1000`, and all 324 current inventory-in-transit codes are enabled and present.

## Interfaces and Dependencies

The flow depends on Prefect, pandas, SQLAlchemy, psycopg2, requests, `oracledb`, `pyodbc`, the SQL Server ODBC driver, the Kingdee AIHub bearer token, SQL Server and Oracle read-only credentials, finance PostgreSQL, Hermes callbacks, and the FastAPI-created `dim_supplier_info` plus sync-run metadata tables.

Credentials must be provided only by the worker environment. Expected configuration names will include separate SQL Server URL, Oracle user/password/DSN, Kingdee token/base URL, and finance database URL. The legacy notebook's embedded credentials must be rotated and the notebook retired after test verification.

## Revision Notes

- 2026-08-05: Initial plan created after the user approved the unified five-dataset flow, daily schedule, and business-report self-service UI design.
- 2026-08-05: Completed implementation and supplier-path test validation. Recorded the remaining operational prerequisite: configure source credentials and a SQL Server ODBC driver before activating all datasets.
- 2026-08-06: Production rollout was authorized for the supplier feature. Rechecked worker prerequisites and narrowed the scheduled default to suppliers so unconfigured source systems are never invoked automatically.
