# Implement versioned business-line staging batches

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The business-line staging flow currently deletes all rows for a requested accounting period before rebuilding six editable staging tables. Because business-line ratios and audit status are stored on those same rows, a corrected source-data refresh destroys completed front-end work. After this change, every successful extraction creates a distinct batch, copies ratios and audit status from the previous editable batch when `(source_no, unique_lvl)` matches, and makes one batch the default editable batch for each period. Ordinary users continue selecting an accounting period; external front ends can resolve the default batch through database helpers or the current-batch view rather than asking users to choose a batch identifier.

This implementation changes repository code and supplies an idempotent SQL migration. It does not apply the migration to the configured database, trigger a Prefect flow, publish accounting results, or restart a worker because those operational writes are not implied by source-code implementation alone.

## Progress

- [x] (2026-07-16 08:30Z) Inspected the current staging extraction, dynamic wide-table schema, ratio upload path, downstream ratio reads, and live read-only uniqueness characteristics.
- [x] (2026-07-16 08:35Z) Confirmed that `(source_no, unique_lvl)` is unique inside each current staging table; expense requires the level because one source expands to several target levels.
- [x] (2026-07-16 09:05Z) Added an idempotent migration, batch lifecycle utilities, and administrator CLI.
- [x] (2026-07-16 09:10Z) Made all staging inserts batch-aware and added ratio plus audit-status inheritance from the preceding editable batch.
- [x] (2026-07-16 09:15Z) Added current editable/published batch resolution helpers and per-table current-batch views.
- [x] (2026-07-16 09:20Z) Kept `fact_bus_line` as the unversioned final approved ratio table, per user clarification.
- [x] (2026-07-16 10:20Z) Simplified batch management to one monthly table with human-readable `batch_no`; completed 9 focused unit tests, durable workflow documentation, focused pre-commit, compile, CLI-import, and diff checks.
- [x] (2026-07-16 09:50Z) Recorded final validation evidence and archived the completed ExecPlan; completion notification is sent after the final repository status check.

## Surprises & Discoveries

- The checked-in `modules/bus_line_staging/sql/create_staging_tables.sql` describes an older English-column schema. Runtime tables are created dynamically by `modules/bus_line_staging/utils.py` with Chinese columns and one column per business line. The migration and code must target the runtime schema.
- Across the current database history, `source_no` is unique within each of the six source fact tables, but it is a short prefixed sequence rather than a UUID. Source identifiers collide across data types, especially inventory and in-transit inventory, so cross-table data must always retain its table/class context.
- The current `fact_bus_line` has about 1.7 million rows and contains exact duplicate logical ratio rows. The migration must not add a uniqueness constraint there until a separately authorized cleanup/backfill is performed.
- No front-end or API implementation for staging fill-in exists in this repository. This change will provide database-level resolution helpers/views and Python utilities; the external front end must consume them in its own codebase.

## Decision Log

- Decision: Store only `batch_id` on business rows; do not persist `record_key`, `source_hash`, or `change_status`.
  Rationale: The user explicitly chose direct carry-forward whenever the existing composite key matches.
  Date/Author: 2026-07-16 / Codex
- Decision: Match staging rows within each table on `(source_no, unique_lvl)` and copy both business-line ratios and audit status.
  Rationale: Read-only profiling confirmed uniqueness for current staging data, including expanded expense rows.
  Date/Author: 2026-07-16 / Codex
- Decision: Resolve the current batch by monthly status rather than treating `MAX(batch_id)` as current.
  Rationale: The newest batch can be generating, failed, or waiting for administrator activation; partial unique indexes enforce one `FILLING` and one `PUBLISHED` batch per month.
  Date/Author: 2026-07-16 / Codex
- Decision: Automatically activate a successful batch only when no editable batch exists for the period. Otherwise leave it `READY` for administrator activation.
  Rationale: Silently switching an active fill-in session risks writing to an unexpected version; external front-end edit tracking is not available in this repository.
  Date/Author: 2026-07-16 / Codex
- Decision: Do not add `batch_id` to `fact_bus_line` or change downstream business-line calculations.
  Rationale: The user clarified that `fact_bus_line` is the final approved upload result, while version history belongs only to the editable staging layer. Existing upload logic is external to this repository and assumes one current final ratio set.
  Date/Author: 2026-07-16 / Codex

## Outcomes & Retrospective

The repository now creates immutable staging extraction batches instead of deleting a period before every run. The first batch-aware run lazily assigns existing unversioned rows to a legacy editable batch, so the next extraction can inherit the already-filled ratios. New rows carry a UUID `batch_id`; matching rows copy all currently configured business-line columns and `审核状态` from the preceding editable or published staging batch.

Normal users do not select a batch. Each staging table receives current-edit and current-published views backed by the single batch table's monthly status. An administrator CLI lists, activates, and marks batches published. `fact_bus_line` remains unchanged as the final approved upload result; publication metadata must be updated only after the external fill-in system successfully uploads that batch to `fact_bus_line`.

No database migration or flow run was executed against the configured database. Rollout still requires coordinating the external front end so reads use the current-edit views (or the monthly `FILLING` batch) and writes include/validate `batch_id` before the first batch-aware extraction is triggered.

## Context and Orientation

`modules/bus_line_staging/flows/bus_line_staging_flow.py` orchestrates source extraction for expense, revenue, other profit items, inventory, receivables, and in-transit inventory. It currently calls `cleanup_staging_month` for all six tables before running tasks. Each task ultimately calls `insert_to_staging_table` in `modules/bus_line_staging/utils.py`; that function dynamically creates the wide staging table and copies rows into PostgreSQL.

Business-line ratios are stored as dynamic columns on the staging rows. An external fill-in application later converts these columns to vertical rows in `fact_bus_line`; the conversion implementation is installed in `mypackage.utilities`, outside this repository. `fact_bus_line` remains the single final approved ratio table and is not versioned by this change. Downstream calculations therefore remain unchanged.

The single monthly batch table uses statuses `GENERATING`, `READY`, `FILLING`, `PUBLISHED`, `SUPERSEDED`, and `FAILED`. Partial unique indexes guarantee at most one `FILLING` and one `PUBLISHED` batch per accounting month. Normal fill-in reads resolve the `FILLING` batch; historical baseline batches use `BLS-YYYYMM-000` and later reruns increment the suffix.

## Plan of Work

First add a repository-owned SQL migration that creates one monthly batch table and adds nullable `batch_id` columns plus batch-aware indexes to the six runtime staging tables. Current legacy rows are registered under a version-zero baseline batch lazily by the first batch-aware flow run, without deleting data. The SQL must be idempotent and safe to review before database execution.

Next add Python batch utilities for creating, failing, completing, activating, publishing, and resolving batches. Status transitions must be transactional and protected with advisory locking plus partial unique indexes so two extractions for the same period cannot both become current.

Then modify the staging flow to create a `GENERATING` batch before any extraction, stop deleting previous rows, pass the batch identifier into every task insertion, copy matching ratios and audit status from the preceding editable batch after all rows are loaded, and mark the new batch `READY` or `FILLING`. On failure, delete only rows belonging to the failed batch and mark the batch `FAILED`; prior batches remain untouched.

The automatic revenue-ratio update must be scoped by `batch_id`. Dynamic table creation and column discovery must include `batch_id`. Each table receives a unique index on `(batch_id, source_no, unique_lvl)` after validation. Because the runtime tables use Chinese quoted identifiers, all SQL identifiers will be composed from known table/column allowlists rather than user input.

Finally add current-batch resolution helpers/views for external consumers and an administrator CLI for activation/publication. Publication metadata is updated only after the external upload to `fact_bus_line` succeeds. Add isolated tests using fake DB cursors/connections for lifecycle and inheritance SQL, plus unit tests for dynamic column selection. Update `docs/business_line_accounting_process.md` with the operational lifecycle and rollout requirements.

## Concrete Steps

From `/root/prefect` with the checked-in environment:

1. Inspect and edit with `apply_patch`; do not run the migration against the configured database.
2. Run focused tests with `venv/bin/python -m pytest tests/test_bus_line_staging_batches.py` if pytest is installed; otherwise use `venv/bin/python -m unittest` for the committed tests.
3. Run syntax compilation for changed Python modules with `venv/bin/python -m compileall modules/bus_line_staging modules/bus_line_cal`.
4. Run focused pre-commit checks on changed files, then `pre-commit run --all-files` if practical. Existing unrelated working-tree changes must be preserved and any unrelated failures reported separately.
5. Inspect `git diff --check` and `git diff --` for the files changed by this task.

For a later authorized staging rollout, apply the migration in a transaction, verify row counts by period before and after, register legacy batches, update the external front end to resolve the monthly `FILLING` batch and submit `batch_id` on writes, then run an explicit safe accounting period through extraction, fill-in, publish, calculation, and rollback.

## Validation and Acceptance

- A second extraction for the same period does not delete or update rows in the previous batch.
- Matching `(source_no, unique_lvl)` rows inherit all current business-line percentage columns and `审核状态`; new keys retain null ratios and `PENDING`.
- Failed extraction cleanup affects only its own `batch_id`.
- Normal current-batch resolution returns only a `FILLING` batch; published resolution returns only the configured published batch.
- A new successful extraction cannot silently replace an existing `FILLING` batch.
- Revenue automatic-ratio filling affects only the new batch.
- `fact_bus_line` remains schema- and behavior-compatible and continues to contain only the final approved ratios.
- Tests cover lifecycle transitions, inheritance column selection, missing previous batches, duplicate-key detection, and failed-batch cleanup.
- No production flow, migration, accounting calculation, or service operation is executed during source implementation.

## Idempotence and Recovery

The migration uses `IF NOT EXISTS`, conditional column creation, and conflict-safe legacy batch registration. Reapplying it must not create duplicate batches or rewrite ratios. Batch creation is retry-safe through a flow-run identifier/idempotency key. A failed flow removes only its own uncommitted batch rows and marks the header `FAILED`.

Activation and publication change monthly statuses in one transaction after locking the batch row. Historical rows are immutable except for external fill-in updates to the current editable batch. Recovery from an implementation or flow failure leaves the previous editable and published batches unchanged. Restoring an old published batch requires a separately authorized `fact_bus_line` upload and downstream recalculation for the explicit accounting period.

## Artifacts and Notes

Read-only profiling evidence from 2026-07-16:

- Latest-period source tables had no duplicate `source_no` values.
- Current staging uniqueness: revenue 8,295/8,295; expense 56,982 distinct `(source_no, unique_lvl)` from 9,222 source numbers; profit 221/221; inventory 66,628/66,628; receivable 5,599/5,599; in-transit 38,354/38,354.
- `fact_bus_line` latest period contained 456 duplicate `(source_no, sec_dist_lvl, bus_line)` groups, all with identical rates.
- Focused unit tests: 9 passed with `venv/bin/python -m unittest tests.test_bus_line_staging_batches -v`.
- Focused pre-commit checks passed for all files changed by this feature.
- Python compilation passed for the staging module, administrator CLI, tests, and three deployment entry points.
- `git diff --check` passed.
- Full-tree pre-commit was intentionally not run because the working tree already contains unrelated user changes and untracked work that formatters could rewrite.

## Interfaces and Dependencies

- Prefect flow: `modules.bus_line_staging.bus_line_staging_flow` keeps its existing `start_date` and `end_date` parameters and returns batch metadata.
- New database table: `bus_line_staging_batch`.
- Modified tables: only the six `staging_bus_*` tables receive `batch_id` support.
- External fill-in applications must resolve the monthly `FILLING` batch, filter reads by its `batch_id`, and submit it with writes. No such application exists in this repository.
- Downstream calculations continue reading the unversioned final `fact_bus_line` table.
- No new Python package dependency is planned.

## Revision Notes

- 2026-07-16: Initial implementation plan created after repository inspection and user confirmation of direct key-based ratio/status inheritance.
- 2026-07-16: Revised scope after user clarification: `fact_bus_line` remains the final unversioned approved output; only staging rows are versioned.
- 2026-07-16: Completed source implementation and focused validation; production migration, front-end rollout, flow trigger, and `fact_bus_line` upload remain separate authorized operational steps.
- 2026-07-16: Simplified the un-applied schema from three management tables to one monthly batch table after confirming batches never cross accounting months; added `batch_no`/`version_no` and version-zero legacy baselines.
