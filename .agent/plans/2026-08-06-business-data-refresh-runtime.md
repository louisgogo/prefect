# Make all business-data refresh sources production-ready

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The Prefect deployment `子流程-业报基础数据更新` must refresh customer, material, R&D project, supplier, and acquiring metrics individually or together. The production worker must have the required drivers and environment configuration, must fail with actionable credential-safe messages when prerequisites are missing, and must complete verified test and production runs without silent partial data changes.

## Progress

- [x] (2026-08-06 03:46Z) Confirmed all five dataset codes exist in the flow and the failed run stopped during dataset resolution before writes.
- [x] (2026-08-06 03:46Z) Confirmed the worker environment lacks SQL Server/Oracle variables and Python drivers.
- [x] (2026-08-06 04:09Z) Installed and verified `oracledb`, `pyodbc`, unixODBC, and Microsoft ODBC Driver 18 for SQL Server.
- [x] (2026-08-06 04:09Z) Migrated SQL Server and Oracle values into root-only worker runtime configuration without committing or logging credential values.
- [x] (2026-08-06 04:09Z) Verified read-only SQL Server and Oracle connectivity and source row availability.
- [x] (2026-08-06 04:09Z) Backed up the eleven affected test tables and completed individual customer, material, R&D, supplier, and acquiring runs.
- [x] (2026-08-06 04:09Z) Completed test combined run `e2adcc72-00b4-45ba-b7a1-c247731f6872`: five completed datasets and zero failures; it also served as an idempotent rerun after individual refreshes.
- [x] (2026-08-06 04:09Z) Changed server/production 06:00 defaults from supplier-only to all five datasets and passed 11 focused tests plus pre-commit checks.
- [ ] Back up production targets, deploy, verify each dataset and one combined production run.
- [ ] Archive this plan after operational acceptance.

## Surprises & Discoveries

- The checked-in requirements declare `oracledb` and `pyodbc`, but the active worker environment does not contain them and the system has no registered SQL Server ODBC driver.
- The legacy notebook contains embedded external database credentials. Values will be migrated directly into runtime configuration without printing or committing them; credential rotation is required after successful migration.
- The Oracle `T_JL_AREA_TRADE` source currently has 221,000 rows and two repeated seven-column dimension groups whose metric values differ. The mirror tables have no primary/unique constraint and the legacy load preserved every row, so rejecting repeated dimensions was an incorrect safety rule. Validation now retains all source rows while still enforcing non-empty snapshots, exact column shape, and row-drop thresholds.
- Test acceptance counts after the combined run are 67,096 customers, 103,569 materials, 663 R&D projects, 25,649 suppliers (25,586 active), and 319,071 acquiring rows with watermark `202607`.

## Decision Log

- Decision: Keep secrets solely in the worker environment and add preflight messages that name missing variable/driver categories without echoing values.
  Rationale: This makes failures actionable from the frontend while preventing credential leakage in Prefect logs and API responses.
  Date/Author: 2026-08-06 / Codex.
- Decision: Validate a SQL Server client available on this host before selecting implementation; prefer the already declared `pyodbc` path only if a compatible system driver is present, otherwise use a tested pure/package driver path.
  Rationale: Installing Python `pyodbc` alone cannot work without an ODBC system driver.
  Date/Author: 2026-08-06 / Codex.
- Decision: Preserve every Oracle acquiring source row and remove the unsupported uniqueness assumption on partial dimensions.
  Rationale: Two source groups legitimately share the visible dimension columns but carry different measures, and the target schema intentionally has no unique key. Aggregating or dropping either row would change source meaning.
  Date/Author: 2026-08-06 / Codex.

## Outcomes & Retrospective

Implementation and rollout are in progress.

## Context and Orientation

`modules/business_data_refresh/flows/business_data_refresh_flow.py` resolves requested dataset codes and orchestrates tasks. `modules/business_data_refresh/tasks/business_data_refresh_tasks.py` reads three master-data views from SQL Server, supplier data from Kingdee, five acquiring tables from Oracle, and writes finance PostgreSQL tables. `deploy_to_server.py` registers the deployment used by FastAPI. Production workers are systemd-managed by `prefect-workers` from `/root/prefect`.

## Plan of Work

Inspect source connectors and target safety checks. Add a small, testable connection preflight and, if required, a SQL Server connector fallback that preserves row shape and transaction behavior. Ensure every raised error is concise, includes dataset/source context, and excludes connection strings and secrets.

Install exact dependencies into `/root/prefect/venv` and configure the worker environment without committing values. Validate read-only source connectivity and source schemas before triggering target writes. Back up test targets, run datasets individually, compare row counts/uniqueness/watermarks, then run all five together and verify rerun behavior. Repeat with production backups and acceptance checks under the user's explicit production authorization.

## Concrete Steps

Work in `/root/worktrees/prefect/business-data-refresh-runtime` for source changes. Use `/root/prefect/venv` for runtime checks. Use systemd-managed `prefect-workers`; never start an unmanaged worker. Keep all credential output suppressed.

## Validation and Acceptance

- Unit tests cover dataset resolution and source preflight/fallback behavior.
- Read-only connections can query the three SQL Server views and five Oracle tables plus the Kingdee supplier endpoint.
- Each test dataset completes with expected nonzero row count, unique keys, acceptable count variance, and expected watermark.
- A test combined run completes all five items, then an idempotent rerun also completes.
- After backups, production meets the same checks and the worker/service remain active.
- Any missing dependency/configuration failure appears in FastAPI/frontend as an actionable credential-safe message.

## Idempotence and Recovery

Use existing per-dataset PostgreSQL transactions and snapshot-count guards. Back up every affected target plus sync status tables before production writes. If source connectivity, schema, count thresholds, uniqueness, or watermark checks fail, do not force the update. Leave the previous committed snapshot intact or restore from the backup as appropriate, correct the prerequisite, and rerun.

## Artifacts and Notes

- Failed parameter-validation run: `e06c00f5-31fb-4960-8287-dc7b185559da`; no business task ran.
- Do not record credential values or source data samples in this file.

## Interfaces and Dependencies

- Python packages: Prefect, PostgreSQL driver, Kingdee HTTP client, Oracle client, and one verified SQL Server client.
- Environment variables: finance target, Kingdee, SQL Server, Oracle, and Prefect API settings.
- External sources: SQL Server master-data views, Kingdee supplier API, Oracle acquiring tables.
- Target tables: customer, material, R&D project, supplier, acquiring metrics, and synchronization status tables.

## Revision Notes

- 2026-08-06: Created after runtime audit found missing source configuration and drivers.
