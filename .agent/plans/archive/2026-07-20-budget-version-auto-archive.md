# Automatically archive the previous official budget version

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The `budget_update_flow` currently exposes both `version` and `report_date`. Operators must understand that one labels inserted rows while the other selects rows to delete, which creates an avoidable risk of deleting the previous budget draft. After this change, the Prefect form will no longer ask for either date. It will derive the official version from the explicit budget year and type (`YYYY-01-01` for annual budgets and `YYYY-07-01` for midyear budgets) and expose one clear `save_previous_version` switch. When the switch is enabled, the workflow will atomically move the existing official version to the first unused date in the same month (`2026-07-02`, then `2026-07-03`, and so on) before writing the new official budget back to day 1. When it is disabled, the existing day-1 version will be replaced without an archive. Downstream jobs can therefore continue treating day 1 as the official budget while operators explicitly control whether the prior submission is retained.

The work changes code and documentation only. It does not authorize or perform a Prefect deployment, flow run, production worker restart, or database write.

## Progress

- [x] (2026-07-20 01:07Z) Inspected current flow parameters, six-table write paths, downstream day-1 filters, installed `mypackage.update_report_data`, and existing 2026 budget version dates.
- [x] (2026-07-20 01:15Z) Removed the two public version-date selectors, derived the official date from budget year/type, added one `save_previous_version` switch, and aligned deployment defaults, notifications, and documentation.
- [x] (2026-07-20 01:17Z) Implemented one-transaction archive-and-insert behavior across all six budget tables, including a PostgreSQL advisory lock and deterministic first-unused archive date selection.
- [x] (2026-07-20 01:21Z) Added 12 focused tests for official-date derivation, archive-date selection, first import, direct overwrite, six-table archive preparation, exception propagation, and flow parameter wiring without writing to the configured financial database.
- [x] (2026-07-20 01:22Z) Passed focused pre-commit, compilation, Prefect schema assertions, and all 12 unit tests; completed read-only verification against current July version dates.

## Surprises & Discoveries

- The installed `mypackage.utilities.update_report_data` can update an old version to a new date via `change_date`, but its insert uses a separate SQLAlchemy engine connection and it catches exceptions without re-raising. It therefore cannot provide a reliable six-table transaction boundary for this financial update.
- Existing 2026 budget tables already contain `2026-01-01`, `2026-01-02`, `2026-01-03`, and `2026-07-01`, confirming the established convention that day 1 is official and later days retain drafts.
- `bud_cash_flow` stores its version in `bud_version`; the other five tables use `report_date`.
- Downstream AI budget-profit and shared-rate tasks explicitly select day-1 versions, so keeping the new official version on day 1 preserves current consumers.
- A final read-only check found that `2026-07-02` appeared in all six budget tables during implementation. The next automatic archive for the July official version will therefore be `2026-07-03`; the selector correctly avoids the occupied date. No database write was performed by this implementation task.
- `pre-commit run --all-files` found and reformatted three unrelated legacy files. Those out-of-scope automatic changes were restored exactly; focused pre-commit on all changed files passes.

## Decision Log

- Decision: Do not expose a version date. Derive the official day-1 date from `budget_year` and `budget_type`, and expose one Boolean `save_previous_version` action switch.
  Rationale: The user clarified that the meaningful choice is whether to retain the current official submission. Requiring a user to type `1` or a date to encode that action is still ambiguous. A named switch makes overwrite versus archive explicit, while annual and midyear official dates are already deterministic.
  Date/Author: 2026-07-20 / Codex

- Decision: Default `save_previous_version` to `False` so Prefect quick runs overwrite the current official version; preserving history requires explicitly enabling the switch.
  Rationale: The user explicitly selected direct overwrite as the desired quick-run behavior after reviewing the safer preserve-by-default alternative.
  Date/Author: 2026-07-20 / Codex

- Decision: Archive the current official version to the first unused date within the same month before inserting the replacement official version.
  Rationale: This preserves chronological draft history and matches existing `01`, `02`, `03` data without adding a schema migration.
  Date/Author: 2026-07-20 / Codex

- Decision: Perform all archive updates and inserts in one SQLAlchemy transaction owned by this repository rather than calling `mypackage.update_report_data` six times.
  Rationale: A single transaction prevents partial financial-version updates and ensures exceptions propagate to Prefect. The current helper does not provide that guarantee.
  Date/Author: 2026-07-20 / Codex

## Outcomes & Retrospective

Implementation is complete. The Prefect parameter schema now contains `save_previous_version` and no `version` or `report_date`. Annual official versions derive to January 1 and midyear official versions derive to July 1. When preservation is enabled, all current day-1 rows are updated to one common unused archive date and all six replacements are inserted inside the same SQLAlchemy transaction. When disabled, the day-1 rows are deleted and replaced without an archive. The write task returns the chosen archive date for logs and Hermes notifications.

Focused pre-commit, Python compilation, Prefect schema assertions, and 12 unit tests pass. A real Prefect run was intentionally not performed because it would write financial data and was not authorized. Deployment registration must be refreshed before the changed parameter schema appears in the Prefect UI.

## Context and Orientation

`modules/budget_update/flows/budget_update_flow.py` defines the manually triggered Prefect flow. It calculates default year/type/source-version parameters, processes six FONE budget datasets, invokes `write_budget_to_db_task`, and refreshes AI budget data. The current public `version` parameter becomes the inserted row label, while `report_date` selects the version deleted by the write helper.

`modules/budget_update/tasks/budget_update_tasks.py` prepares expense, income, personnel, profit, cash-flow, and shared-rate DataFrames. Five destination tables use the database column `report_date`; `bud_cash_flow` uses `bud_version`. The year-mid branch combines actual data through `actual_through_month` with remaining FONE budget months and processes tables sequentially to avoid excessive memory use.

`deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py` register the flow and prefill parameters. They must all reflect the new single-parameter contract. `docs/ai_budget_mapping_guide.md` is the current operator-facing budget update documentation and must describe automatic archiving.

The configured PostgreSQL database is financial data. Tests must not call the real write task against that database. Database behavior should be verified with mocked/fake SQLAlchemy connections or another isolated mechanism. A representative real flow run remains a separate, explicitly authorized operational step.

## Plan of Work

First, remove `version` and `report_date` from the flow contract and add `save_previous_version: bool = True`. Derive the official version as January 1 for annual budgets and July 1 for midyear budgets, pass it to all processing tasks as the inserted label, and pass it to the write task as the official version. Update notifications, AI refresh arguments, deployment descriptions, and operator documentation.

Second, add local write helpers in `budget_update_tasks.py`. A pure helper will find the first unused archive date after day 1 within the official version's month. A database helper will query version dates across all six tables, choose one common archive date, update every current official row to that date, and insert each prepared DataFrame through the same SQLAlchemy connection and transaction. The year-mid path will keep sequential DataFrame preparation and release to preserve its memory behavior. If no official rows exist, the first import will insert without archiving. If the month has no unused date, the task will fail before modifying data.

Third, add focused tests. Tests will cover empty history, existing `01`, existing `01/02/03`, gaps, month-end exhaustion, day-1 validation, and the table-to-version-column mapping. Where practical, mock the engine/connection to assert that the same archive date is used for all tables and that errors propagate rather than being swallowed.

Finally, run Black, flake8, Python compilation, focused pytest, and `git diff --check`. Do not run the Prefect deployment or the database-writing flow. Document that an authorized staging run should compare all six tables before and after: previous official counts must move to the chosen archive date, new rows must occupy day 1, totals must reconcile, and no partial table state may remain after a forced failure.

## Concrete Steps

Work from `/root/prefect` using the checked-in virtual environment.

1. Edit the flow, task, deployment, documentation, and focused test files with `apply_patch`.
2. Format changed Python files with:

       venv/bin/black --line-length 100 <changed-python-files>

3. Validate locally without database writes:

       venv/bin/python -m py_compile <changed-python-files>
       venv/bin/flake8 <changed-python-files>
       venv/bin/python -m unittest -v tests.test_budget_update_versioning
       git diff --check

4. Inspect the final diff and confirm that unrelated untracked paths (`.codegraph/`, `check/`, and `docs/caiwu-data-pipeline-api.md`) remain untouched.

Observed focused output was successful compilation, formatting, lint, and 12 passing tests. No Prefect flow-run ID or database write evidence was produced in this implementation-only task.

## Validation and Acceptance

Acceptance requires all of the following:

- The Prefect flow schema exposes `save_previous_version` and no longer exposes `version`, `report_date`, or another manually entered budget-version date.
- Annual budgets derive `YYYY-01-01`; midyear budgets derive `YYYY-07-01` before FONE reads or database writes.
- Given official `2026-07-01` and no later July version, the archive helper selects `2026-07-02`.
- Given `2026-07-01`, `2026-07-02`, and `2026-07-03`, it selects `2026-07-04`.
- The same archive date applies to `bud_expense`, `bud_income`, `bud_personnel`, `bud_profit`, `bud_cash_flow`, and `bud_bus_shared_rate`.
- The old official rows are updated to the archive date and the new rows are inserted at the original day-1 `budget_version` within one transaction.
- An insertion or update failure raises to Prefect and rolls back the transaction.
- The year-mid path retains SQL date filtering and sequential intermediate DataFrame cleanup.
- All three deployment scripts and operator documentation explain that day 1 remains official and prior official data is archived automatically.

Operational acceptance, when separately authorized, should use explicit year, budget type, FONE version, official budget version, actual-through month, and database target. Compare row counts and financial totals for all six tables before and after, and inspect Prefect task logs for the chosen archive date.

## Idempotence and Recovery

When `save_previous_version=True`, each successful run intentionally creates one new archived snapshot of the previous official version and replaces day 1 with newly fetched data. It is therefore history-preserving but not logically idempotent: rerunning the same input creates another archive date. When `save_previous_version=False`, rerunning replaces day 1 without creating another archive. Mapping or validation failures happen before the write transaction and create no archive. Database failures during archive or insertion roll back all six table changes. AI refresh happens after the database transaction; if AI refresh alone fails, the base budget update has already succeeded, so operators should rerun the AI ETL flow rather than rerunning the budget update and creating an unnecessary additional archive.

No automatic cleanup or rollback of committed archives will be added. Any production recovery that changes financial rows requires explicit authorization and pre/post reconciliation.

## Artifacts and Notes

- Current 2026 versions observed read-only before implementation: annual drafts on January 1, 2, and 3; midyear official version on July 1.
- Current July versions observed read-only at completion: July 1 and July 2 in all six tables; the helper selected July 3 as the next archive date.
- Current 2026 FONE midyear source version observed read-only: `AdjustVersion1`.
- Existing uncommitted documentation edits from the immediately preceding task are part of this refactor and will be revised rather than discarded.
- Focused validation: pre-commit passed; Python compilation passed; 12 unit tests passed; Prefect parameter schema contains `budget_year`, `fone_version`, `budget_type`, `save_previous_version`, `actual_through_month`, `refresh_ai_data_etl`, and `output_dir`.

## Interfaces and Dependencies

- Prefect flow: `budget_update_flow` / deployment `主流程-预算更新`.
- Public parameters retained: `budget_year`, `fone_version`, `budget_type`, `save_previous_version`, `actual_through_month`, `refresh_ai_data_etl`, and `output_dir`.
- Destination tables: `bud_expense`, `bud_income`, `bud_personnel`, `bud_profit`, `bud_cash_flow`, `bud_bus_shared_rate`.
- Version columns: `report_date` for five tables; `bud_version` for `bud_cash_flow`.
- External systems: FONE reads and configured PostgreSQL writes through `mypackage` connection configuration.
- Dependencies already present: pandas, SQLAlchemy, psycopg2, Prefect. No new runtime dependency is planned.

## Revision Notes

- 2026-07-20: Initial plan created after confirming the requested rule: archive the prior day-1 official version automatically and keep the newly imported official budget on day 1.
- 2026-07-20: Revised the public interface after user clarification. The official date is now derived automatically, and one `save_previous_version` switch controls archive versus direct overwrite.
- 2026-07-20: Completed implementation and moved the plan to the archive. Recorded focused validation and the separately authorized operational verification that remains.
- 2026-07-20: Changed the quick-run default to direct overwrite (`save_previous_version=False`) at the user's explicit request and added a schema-default regression test.
