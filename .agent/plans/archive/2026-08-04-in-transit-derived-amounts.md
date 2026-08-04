# Stop persisting redundant in-transit inventory amounts

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Prefect must stop reading or writing the stored in-transit fields `order_amount`, `total_payment_amount`, and `total_inventory_received`. Visible order amount is derived as `order_count × unit_price`; in-transit value is derived as `unreceived_inventory × unit_price × COALESCE(exchange_rate, 1)`. Report collection, data import, business-line staging, business-line fact generation, and inventory impairment must continue working after the finance database columns are removed by the coordinated FastAPI migration.

## Progress

- [x] (2026-08-04 09:28Z) Traced the affected Prefect tasks and confirmed the calculation rules.
- [x] (2026-08-04 09:44Z) Updated report collection and import projection to tolerate old workbooks while no longer persisting the removed fields.
- [x] (2026-08-04 09:44Z) Updated business-line staging and fact generation to use outstanding quantity and calculated values.
- [x] (2026-08-04 09:44Z) Removed the inventory-impairment dependency on stored order amount.
- [x] (2026-08-04 09:44Z) Updated focused tests, bootstrap SQL, example notebook, and workflow documentation.
- [x] (2026-08-04 09:44Z) Passed all 90 unit tests, scoped pre-commit, JSON validation, and diff validation without triggering database-writing flows.
- [x] (2026-08-04 09:53Z) Merged PR #10 into `session/prefect` as `35e4a51` and reran all 90 unit tests from the integrated branch.

## Surprises & Discoveries

- `modules/bus_line_staging/tasks/asset_tasks.py` uses `order_amount != 0` both in SQL and pandas filtering. This excluded valid in-transit rows when the input order amount was missing.
- `modules/inventory_impairment/tasks/inventory_impairment_tasks.py` requires and converts `order_amount` but does not use it to calculate impairment.
- Legacy source workbooks may still contain the three removed columns, so the import path must project them away before database insertion during the rollout.

## Decision Log

- Decision: Preserve order amount only as a calculated output and do not write it to PostgreSQL or Prefect staging tables.
  Rationale: The user explicitly chose calculation over storage.
  Date/Author: 2026-08-04 / Codex

- Decision: Use non-zero unreceived quantity as the staging inclusion rule.
  Rationale: It expresses whether inventory is still in transit and retains zero-price or missing-price quantity records.
  Date/Author: 2026-08-04 / Codex

## Outcomes & Retrospective

The Prefect code is integrated into `session/prefect` and compatible with both the pre-migration and post-migration schemas. All 90 unit tests pass from the merged branch. No worker restart, deployment registration, database-writing flow, or production data change was performed; those operational actions were outside the authorized scope and require separate approval.

## Context and Orientation

`modules/report_collection/tasks/report_tasks.py` gathers monthly Excel sheets. `modules/data_import/tasks/data_import_tasks.py` writes them to fact tables. `modules/bus_line_staging/tasks/asset_tasks.py` prepares editable business-line staging rows using the Chinese schema in `modules/bus_line_staging/utils.py`. `modules/bus_line_cal/tasks/asset_tasks.py` writes the final business-line fact table. `modules/inventory_impairment/tasks/inventory_impairment_tasks.py` performs quarterly impairment calculations. The coordinated application and database work is retained in the FastAPI repository at `.agent/plans/archive/2026-08-04-in-transit-derived-amounts.md`.

## Plan of Work

Remove cumulative payment and received fields from report collection schemas and explicitly discard all three legacy input columns before mapped DataFrames reach database writes. Keep sufficient inputs to calculate order amount and in-transit amount when producing user-visible outputs.

Change business-line staging to query rows by non-zero unreceived quantity, calculate in-transit amount from quantity, price, and exchange rate, and omit the three removed stored columns from staging inserts. Change business-line fact allocation to scale order count and unreceived quantity only; unit price and exchange rate remain unit attributes.

Remove `order_amount` from inventory impairment required/select/normalization columns because the calculation already uses unreceived quantity, unit price, and exchange rate. Update tests and documentation to encode the rule.

## Concrete Steps

Work in `/root/worktrees/prefect/remove-in-transit-unused-columns` and activate `source venv/bin/activate`. Run the focused inventory impairment and business-line staging tests, then `pre-commit run --all-files` when practical. Do not trigger database-writing flows during local validation.

## Validation and Acceptance

- A legacy DataFrame containing the three old fields is projected to the new fact-table schema without insertion errors.
- A source row with missing order amount and non-zero unreceived quantity is selected for staging.
- Staging in-transit amount equals `unreceived_inventory × unit_price × coalesce(exchange_rate, 1)`.
- Business-line allocation scales order count and unreceived quantity, without requiring the removed fields.
- Inventory impairment tests pass with no `order_amount` source column.
- Searches show no live Prefect SQL or DataFrame access to the removed stored fields, apart from deliberate legacy-input dropping or documentation of migration behavior.

## Idempotence and Recovery

Code changes are safe before the database migration because they ignore still-present legacy columns. They must be deployed before the production schema drop. No production flow trigger, worker restart, or database write is authorized by this plan alone. If legacy files retain the columns, projection safely ignores them; this compatibility can remain as defensive input sanitization while those external files are active.

## Artifacts and Notes

The 2026-02 historical staging omission is a separate data correction and is not automatically rewritten by this code change. Integration evidence is PR #10 with merge commit `35e4a5149de9a795be9e0796b55f0932aa5cc2fc`.

## Interfaces and Dependencies

No flow names, schedules, package exports, or deployment registrations change. The workflows continue to use the existing finance connection and pandas calculations. The FastAPI migration removes the physical columns only after this code is compatible.

## Revision Notes

- 2026-08-04: Created after the user confirmed derived order amount and in-transit amount rules.
- 2026-08-04: Recorded completed code changes and local validation; deployment remains pending authorization and integration.
- 2026-08-04: Recorded integration into `session/prefect`, repeated 90-test validation, explicit production exclusions, and archived the completed plan.
