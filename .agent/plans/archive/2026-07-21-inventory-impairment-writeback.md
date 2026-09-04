# Add safe quarterly defaults and fact_profit_bd writeback

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Extend the completed `inventory_impairment_flow` so a normal quarterly run defaults to the most recently completed quarter and can atomically replace that quarter's `fact_profit_bd` rows for `业报调整 / 资产减值损失`. The observable result will be one calculated row per reporting entity and hierarchy, deterministic source numbers, no duplicate quarter rows after reruns, and a post-write readback reconciliation. This implementation task will change and test code but will not execute the real database write or deploy/restart Prefect services.

## Progress

- [x] (2026-07-21 10:06Z) Confirmed `fact_profit_bd` foreign keys, unique `source_no`, current row shape, and existing calculation/deployment code.
- [x] (2026-07-21 10:10Z) Implemented runtime recent-completed-quarter defaults and deterministic quarter-level target rows.
- [x] (2026-07-21 10:12Z) Implemented advisory-lock-protected transactional delete, insert, readback verification, commit, and rollback behavior.
- [x] (2026-07-21 10:15Z) Updated flow/deployment defaults, operator documentation, and focused transaction tests.
- [x] (2026-07-21 10:17Z) Passed focused pre-commit, compilation, lint, all 46 repository tests, and a read-only default-period Q2 preview/reconciliation.
- [x] (2026-07-21 10:18Z) Sent the configured WeCom completion notification and archived this completed plan.

## Surprises & Discoveries

- `fact_profit_bd.source_no` has a unique constraint, so generated rows need deterministic globally distinct values.
- Existing June 2026 comparison data has 16 monthly-source rows but aggregates to six entity/hierarchy quarter totals. The new quarterly flow will intentionally replace them with six quarter-level rows while preserving the same subject total and entity/hierarchy totals.
- The requested delete scope (`date`, `fin_con='业报调整'`, `prim_subj='资产减值损失'`) currently corresponds to the inventory impairment dataset validated in the preceding task.
- No existing `fact_profit_bd.source_no` begins with the new `INVIMP-` prefix, so the deterministic namespace is currently collision-free.
- A read-only run with both period parameters omitted resolved to 2026-Q2 on 2026-07-21, produced six target rows totaling 3,771,362.43, and matched all six existing entity/hierarchy totals without executing the write task.

## Decision Log

- Decision: Resolve omitted year and quarter to the most recently completed quarter at runtime; reject partially supplied periods.
  Rationale: July should resolve to Q2, October to Q3, January to prior-year Q4, and reruns outside the immediate following month should remain deterministic.
  Date/Author: 2026-07-21 / Codex

- Decision: Replace all rows in the exact requested quarter/consolidation/subject scope in one PostgreSQL transaction.
  Rationale: This matches the user's duplicate-prevention requirement and prevents a committed delete without its replacement inserts.
  Date/Author: 2026-07-21 / Codex

- Decision: Use deterministic `INVIMP-YYYYMM-<hash>` source numbers and mark rows with an automatic inventory-impairment remark.
  Rationale: `source_no` is globally unique; stable identifiers make reruns auditable and safe after the scoped delete.
  Date/Author: 2026-07-21 / Codex

## Outcomes & Retrospective

Implementation is complete. The flow now accepts omitted `year` and `quarter`, resolves them to the latest completed quarter, builds one `fact_profit_bd` row per entity/hierarchy, and defaults to replacing the target scope. The write task serializes concurrent quarter runs with a PostgreSQL advisory transaction lock, captures the old count/total, deletes only the target date/consolidation/subject, inserts deterministic rows, validates the readback count and total, and commits. Insert and readback failures are covered by rollback tests.

The local-test deployment defaults to read-only, while remote and production registration defaults enable writeback and post-write reconciliation. No real database write, deployment, schedule, worker restart, or service change was executed during this implementation.

## Context and Orientation

The calculation code is under `modules/inventory_impairment/`. `inventory_impairment_flow` currently requires explicit `year` and `quarter`, calculates monthly and quarterly results, and optionally performs read-only comparison against `fact_profit_bd`. Deployment entry points are `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`.

`fact_profit_bd` has a primary key on `id`, a unique constraint on `source_no`, and foreign keys for date, reporting consolidation, reporting entity, primary subject, and unique hierarchy. The write path must omit `id` and `last_modified`, use existing dimension values, and insert columns `fin_con`, `fin_ind`, `prim_org`, `sec_org`, `third_org`, `prim_subj`, `mo_amt`, `date`, `unique_lvl`, `year`, `remarks`, and `source_no`.

## Plan of Work

Add a pure period resolver and target-row builder. The resolver will use the first day of the current calendar quarter minus one month to identify the most recently completed quarter. The row builder will validate one row per entity/hierarchy, map the quarter amount to `mo_amt`, add accounting metadata, and generate deterministic source numbers.

Add a Prefect write task using the existing psycopg connection. It will acquire a transaction-scoped advisory lock, read old count/total, delete the exact quarter/consolidation/subject scope, insert all replacement rows with `execute_values`, read back count/total, assert them against the prepared rows, and commit. Any error will roll back and raise to Prefect.

Update the flow so omitted periods resolve automatically and writeback is enabled by default, with an explicit switch available for read-only calculations. Reconciliation will run after the write so it validates committed/read-back target data. Deployment scripts will provide concrete most-recent-quarter defaults in Prefect UI. Documentation and tests will cover January rollover, normal quarter resolution, deterministic IDs, delete scope, rollback, and flow wiring.

## Concrete Steps

Work from `/root/prefect` using `venv`.

1. Edit tasks, flow, deployments, docs, and tests with `apply_patch`.
2. Test the write task with fake connections/cursors only; do not call it against the configured database.
3. Run focused pre-commit, compilation, flake8, all repository unit tests, and `git diff --check`.
4. Inspect the final flow parameter schema and generated 2026-Q2 rows without invoking database writes.

## Validation and Acceptance

- July 2026 defaults to year 2026 quarter 2; January 2027 defaults to year 2026 quarter 4.
- Supplying only year or only quarter raises before database access.
- Prepared rows are unique by entity/hierarchy and `source_no`, use quarter month-start dates, and contain the required accounting metadata.
- The write transaction deletes only matching `date + 业报调整 + 资产减值损失` rows, inserts the prepared quarter rows, verifies count and total, and rolls back on any failure.
- A rerun produces the same source numbers and final row set without duplicates.
- No real financial database write, deployment, or service restart occurs during implementation validation.

## Idempotence and Recovery

Successful reruns are idempotent at the target scope because every run deletes and replaces the same quarter rows using deterministic source numbers. A PostgreSQL advisory transaction lock serializes concurrent runs for the same quarter. Delete, insert, and readback occur in one transaction; failures roll back to the prior committed dataset. After a committed write, any downstream failure can be recovered by rerunning the same year and quarter.

## Artifacts and Notes

- Existing unrelated untracked paths remain out of scope.
- The preceding read-only Q2 reconciliation total was 3,771,362.43 across six entity/hierarchy rows.
- Default-period read-only Prefect flow run: `ba277d26-4a0b-4614-9dc4-8dee429bab8f`; resolved to 2026-Q2 and matched all six combinations.
- Generated Q2 target row count: 6; generated total: 3,771,362.43; existing legacy target row count: 16.
- Validation: focused pre-commit passed, flake8 passed, compilation passed, `git diff --check` passed, and all 46 repository unit tests passed.

## Interfaces and Dependencies

- Flow: `inventory_impairment_flow`.
- Destination: `public.fact_profit_bd`.
- Existing dependencies: pandas, Prefect, psycopg2, and `mypackage.utilities.connect_to_db`.
- No schema migration or new dependency is planned.

## Revision Notes

- 2026-07-21: Initial writeback plan created after confirming the user-authorized replace-and-insert behavior and target table constraints.
- 2026-07-21: Completed runtime defaults, deterministic row preparation, transactional writeback, deployment defaults, documentation, and no-write validation.
