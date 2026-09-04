# Build and reconcile the quarterly inventory impairment subflow

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Add a standalone Prefect subflow that reproduces the supplied Power BI inventory impairment rules from PostgreSQL `fact_inventory` plus `fact_inventory_on_way`, calculates the impairment movement for every reporting entity and quarter, and exposes a reconciliation against the existing `fact_profit_bd` rows where `fin_con = '业报调整'` and `prim_subj = '资产减值损失'`. The first acceptance period is the quarter ending 2026-06-30. The implementation must make the accounting period explicit, return auditable intermediate totals, and perform only read-only validation against the configured financial database during development; it will not overwrite `fact_profit_bd` or trigger a production deployment.

## Progress

- [x] (2026-07-21 09:45Z) Read repository instructions, existing asset/profit flows, deployment entry points, and the completion-notification skill.
- [x] (2026-07-21 09:53Z) Resolved all three database schemas, entity and hierarchy keys, the in-transit amount formula, fixed-day aging boundaries, and June 2026 comparison totals.
- [x] (2026-07-21 09:58Z) Implemented pure calculation/reconciliation helpers and the read-only Prefect flow with explicit year and quarter parameters.
- [x] (2026-07-21 10:00Z) Exported and registered the new flow in all three deployment entry points.
- [x] (2026-07-21 10:02Z) Added 10 focused unit tests for rule precedence, day boundaries, quarter arithmetic, zero-activity filtering, warehouse mapping, reconciliation, and flow parameters.
- [x] (2026-07-21 10:04Z) Passed focused pre-commit, lint, compilation, all 40 repository tests, and read-only 2026-Q1/Q2 Prefect reconciliations.
- [x] (2026-07-21 10:05Z) Sent the configured WeCom completion notification and archived this completed ExecPlan.

## Surprises & Discoveries

- The existing business-line asset flow reads both source tables but only creates business-line detail tables; it does not calculate inventory impairment.
- The supplied Power BI process calculates an impairment balance per month and then reverses the sign of the month-over-month balance movement. Summing the three monthly movements in a quarter telescopes to prior-quarter-end balance minus current-quarter-end balance.
- The supplied M condition checks `Text.Contains([仓库], ...)` before its intended null-warehouse branch. The Python implementation will preserve the stated business result (null warehouse for 嘉联 maps to 集团仓库) rather than reproduce that evaluation error.
- The in-transit transition table does not use calendar-month offsets. Exact reconciliation required fixed day buckets: less than 180 days, 180-269, 270-359, 360-719, 720-1079, and at least 1080 days. The initially plausible calendar-month implementation matched five Q2 entity-level rows but overstated 新国都技术 by 70,987.45; the fixed-day implementation matches all six rows exactly.
- June 2026 `fact_profit_bd` contains 16 source rows that aggregate to six reporting entity/hierarchy combinations. One combination (国际) has offsetting nonzero monthly rows and a zero quarter total, so the implementation filters only entities with no nonzero month activity rather than filtering zero quarter totals.
- The real Q2 run loaded 45,521 inventory rows and 31,149 in-transit rows. The real Q1 run loaded 42,471 inventory rows and 38,075 in-transit rows.

## Decision Log

- Decision: Treat this request as a financial calculation and reconciliation feature requiring an ExecPlan.
  Rationale: It introduces a new Prefect flow, translates accounting rules, reads production-like financial tables, and requires independently verifiable reconciliation against an existing quarter.
  Date/Author: 2026-07-21 / Codex

- Decision: Keep validation read-only and do not insert calculated rows into `fact_profit_bd` in this task.
  Rationale: The user requested calculation and validation against existing June 2026 data but did not authorize replacement or insertion of accounting data. A returned reconciliation DataFrame and summarized Prefect logs provide safe evidence before any future write integration.
  Date/Author: 2026-07-21 / Codex

- Decision: Use explicit `year` and `quarter` flow parameters instead of a dynamic previous-month default.
  Rationale: Repository operational safety rules require explicit periods for financial recomputation, and deterministic parameters make reruns and historical reconciliation reproducible.
  Date/Author: 2026-07-21 / Codex

- Decision: Reproduce the observed Power BI fixed-day in-transit aging boundaries rather than interpreting month labels as calendar offsets.
  Rationale: A systematic boundary search against the three monthly 新国都技术 adjustments identified 179/269/359 as the exact upper-day limits. Applying 180/270/360-day breakpoints reconciled every 2026-Q2 entity and also matched 2026-Q1 within the configured one-cent tolerance.
  Date/Author: 2026-07-21 / Codex

- Decision: Return detailed, monthly, quarterly, recorded, and reconciliation DataFrames but keep the flow read-only.
  Rationale: These outputs support audit and future integration while avoiding an unauthorized financial-table write. Source-period coverage checks fail the flow if either source table lacks one of the four required months.
  Date/Author: 2026-07-21 / Codex

## Outcomes & Retrospective

Implementation and read-only validation are complete. `inventory_impairment_flow` now calculates inventory and in-transit impairment, preserves auditable monthly balances, aggregates quarterly adjustments by reporting entity and mapped hierarchy, and reconciles against `fact_profit_bd`. The 2026-Q2 run matched all six entity/hierarchy combinations exactly: both calculated and recorded totals were 3,771,362.43 with a 0.00 residual. The 2026-Q1 run matched all five combinations within the configured one-cent tolerance: calculated 4,775,688.21 versus recorded 4,775,688.20.

The implementation changed code and documentation only. It registered the flow in deployment scripts but did not execute a deployment, restart a worker, or write a database row.

## Context and Orientation

`fact_inventory` stores monthly inventory and aging buckets. `fact_inventory_on_way` stores monthly in-transit purchase-order inventory. `fact_profit_bd` stores reporting-profit detail, including the manually calculated comparison rows under reporting consolidation `业报调整` and primary subject `资产减值损失`. Existing database access is through `mypackage.utilities.connect_to_db`.

The new package will live under `modules/inventory_impairment/`, with task and flow subpackages. Pure pandas helpers will implement the business rules so they can be tested without a database or Prefect server. The Prefect tasks will load the four required balance dates (the prior quarter end plus the three target-quarter months, subject to the actual source availability), calculate monthly impairment balances, derive quarterly movements, load the target `fact_profit_bd` comparison rows, and reconcile by reporting entity. The flow will return structured DataFrames and summary metrics and will not write to the database.

All public flows are exported from `modules/__init__.py`. Deployment registrations are maintained in `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`; adding a flow requires keeping all applicable scripts aligned.

## Plan of Work

First inspect PostgreSQL metadata and small aggregated result sets for the three tables. Confirm English column names, date conventions, reporting-entity fields, available months, and how in-transit records can be represented in the same aging schema as inventory records. Record the observed comparison key and totals for June 2026 without exposing row-level sensitive data.

Second implement pure functions that normalize both source tables, append in-transit inventory using the Power BI-compatible transition rule, classify warehouse location, calculate impairment balances with explicit rule precedence, map organizational hierarchy, and aggregate monthly balances. Derive the quarterly amount from the prior-quarter-end and current-quarter-end balances while also retaining monthly movement details for auditability. Missing prior balances will be surfaced in validation metrics and treated according to the confirmed data behavior rather than silently dropped.

Third wrap loaders, calculation, and reconciliation in Prefect tasks and a standalone flow. The comparison will aggregate `fact_profit_bd` by the same reporting-entity key and calculate absolute and signed differences. Logs will contain counts and aggregate totals only, not per-record output.

Fourth export and register the flow, add focused tests, run formatting/lint/compilation/test checks, then execute the flow or equivalent task path read-only for 2026-Q2. Record matched entities, unmatched entities, calculated total, database total, and residual difference in this plan. Do not deploy, restart workers, or write financial rows.

## Concrete Steps

Work from `/root/prefect` with the checked-in virtual environment.

1. Inspect schemas and aggregate comparison data with parameterized read-only queries through `venv/bin/python` and `mypackage.utilities.connect_to_db`.
2. Add `modules/inventory_impairment/tasks/inventory_impairment_tasks.py`, `modules/inventory_impairment/flows/inventory_impairment_flow.py`, package exports, and focused tests using `apply_patch`.
3. Update `modules/__init__.py`, `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py` so the flow appears consistently in Prefect UI registrations.
4. Validate with commands equivalent to:

       venv/bin/black --line-length 100 <changed-python-files>
       venv/bin/isort --profile black <changed-python-files>
       venv/bin/flake8 <changed-python-files>
       venv/bin/python -m py_compile <changed-python-files>
       venv/bin/python -m unittest -v <focused-test-module>
       pre-commit run --files <changed-files>
       git diff --check

5. Run the read-only calculation for `year=2026`, `quarter=2` and compare it with June 2026 `fact_profit_bd` rows for `业报调整` / `资产减值损失`.

## Validation and Acceptance

Acceptance requires:

- The flow requires an explicit valid year and quarter and deterministically resolves the prior quarter end, all target-quarter months, and the quarter-end comparison date.
- The calculation implements the supplied rule matrix and its precedence: 中正 zero; outbound goods rules before special-warehouse rules; special warehouses fully impaired; ordinary inventory at 0/30/50/100 percent by aging bucket.
- Inventory and the compatible in-transit transition data are both represented in the calculation, with source row counts and amounts reported separately.
- Results contain one quarter amount per reporting entity and the mapped unique hierarchy, with monthly balances retained for audit.
- Reconciliation loads only June 2026 `fact_profit_bd` rows where reporting consolidation is `业报调整` and primary subject is `资产减值损失`, aggregates using the confirmed entity key, and reports calculated amount, recorded amount, and difference.
- Focused unit tests pass, changed files pass formatting/lint/compilation, and no production database row is inserted, updated, or deleted.
- The final handoff clearly distinguishes exact matches, explained mapping differences, and any unresolved business-rule discrepancy.

## Idempotence and Recovery

The new flow is read-only, so rerunning the same year and quarter is idempotent provided source data is unchanged. Database connections must close on success and failure. Calculation or reconciliation errors raise to Prefect and leave source tables untouched. No recovery procedure is required beyond correcting inputs or source data and rerunning. Any future feature that writes calculated adjustments into `fact_profit_bd` must be separately authorized and must add transactional delete-and-reload semantics, uniqueness checks, and pre/post financial reconciliation.

## Artifacts and Notes

- Existing unrelated untracked paths at task start: `.codegraph/`, `check/`, and `docs/caiwu-data-pipeline-api.md`. They must remain untouched.
- Initial comparison period: 2026-Q2, using June 2026 `fact_profit_bd` rows.
- No deployment, worker restart, flow registration run, or database write is authorized by this plan.
- Q2 Prefect flow run: `f6732ff5-811f-4f91-ae5f-fcc9637e29eb`; six matched combinations; calculated and recorded totals both 3,771,362.43.
- Q1 Prefect flow run: `03d35f98-19e1-4889-8e0f-5aa74129c635`; five matched combinations under `tolerance=0.01`; aggregate residual 0.01.
- Validation: focused pre-commit passed; flake8 passed; Python compilation passed; `git diff --check` passed; all 40 repository unit tests passed.
- Durable operating and accounting documentation: `docs/inventory_impairment_flow.md`.

## Interfaces and Dependencies

- New Prefect flow: `inventory_impairment_flow(year: int, quarter: int, ...)` with a Chinese deployment name indicating quarterly inventory impairment calculation.
- Source tables: `fact_inventory`, `fact_inventory_on_way`, `fact_profit_bd` in the configured PostgreSQL database.
- Existing dependencies: pandas, Prefect, psycopg2 through `mypackage`; no new runtime package is planned.
- Deployment entry points: `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`.

## Revision Notes

- 2026-07-21: Initial plan created after reviewing the supplied Power BI logic and current Prefect asset/profit modules. The first implementation target is a read-only 2026-Q2 calculation and reconciliation.
- 2026-07-21: Completed implementation and validation. Recorded the exact fixed-day in-transit aging rule, zero-activity filtering behavior, flow-run evidence, tests, and deployment-registration scope.
