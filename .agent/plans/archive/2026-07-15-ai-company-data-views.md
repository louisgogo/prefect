# Add stable company-level AI data views

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The `ai_data_etl_flow` currently publishes business-line data through `ai_bus_*` views, but the same names are also reused for company-level reporting data when the flow runs in `业报数据` mode. Add stable company-level views named `ai_revenue`, `ai_profit`, and `ai_expense` so AI consumers can query company data without depending on the selected flow mode. `ai_profit` must read from `fact_profit`; revenue and expense must retain the field transformations already used by the existing company-report configuration.

## Progress

- [x] (2026-07-15 03:42Z) Inspected the AI ETL flow, view creation task, company/business-line source tables, and generic view refresh exclusions.
- [x] (2026-07-15 03:44Z) Added the three company-level view configurations to every AI ETL run.
- [x] (2026-07-15 03:44Z) Protected the new AI views from generic mapped-view refresh behavior.
- [x] (2026-07-15 03:45Z) Ran Python compilation, focused pre-commit hooks, and whitespace/error diff validation successfully.
- [x] (2026-07-15 03:46Z) Recorded validation evidence and completion outcome; runtime database creation remains intentionally unexecuted because no database target was named.
- [x] (2026-07-15 04:17Z) After explicit user authorization, ran the view-only AI ETL against `mydb` at `10.18.8.191:5432` and verified the three new views by database readback.

## Surprises & Discoveries

- The `业报数据` branch currently writes company-level data into `ai_bus_revenue`, `ai_bus_profit`, and `ai_bus_expense`, adding `bus_line = '无'`. The new `ai_*` views can be added without removing that compatibility behavior.
- Company-level revenue is not a direct projection of `fact_revenue`: the existing `"7-4收入计算表"` dependency emits both `营业收入` and `营业成本` rows with a common `amt` field.

## Decision Log

- Decision: Create `ai_revenue`, `ai_profit`, and `ai_expense` on every `ai_data_etl_flow` run, independent of `data_type`.
  Rationale: Their names represent stable company-level datasets, while `ai_bus_*` remains the business-line namespace. This avoids mode-dependent availability and preserves existing consumers.
  Date/Author: 2026-07-15 / Codex
- Decision: Reuse the existing company-report SQL transformations, except do not synthesize a `bus_line` column for the new company-level views.
  Rationale: This keeps amount, expense dimension, customer dimension, and organization parsing behavior consistent while avoiding a misleading business-line field.
  Date/Author: 2026-07-15 / Codex

## Outcomes & Retrospective

Added stable `ai_revenue`, `ai_profit`, and `ai_expense` definitions to every AI ETL run while preserving the existing `ai_bus_*` behavior. `ai_profit` reads directly from `fact_profit`; revenue and expense reuse the established company-report transformations. The generic view refresh exclusions now include all three names. Static validation passed. After the user identified the current configured database as the intended target by authorizing the write, the view-only flow run completed successfully and database readback confirmed source/view row-count parity for all three new views.

## Context and Orientation

`modules/ai_data_etl/flows/ai_data_etl_flow.py` defines the main Prefect flow and builds a list of source-to-view SQL configurations. `modules/ai_data_etl/tasks/ai_data_etl_tasks.py` validates each query with `LIMIT 0`, drops the prior view, and recreates it. `modules/view_update/tasks/view_update_tasks.py` contains a separate generic mapped-view refresh; AI-managed view names belong in its `EXCLUDE_TABLES` set so that workflow does not treat them as ordinary source tables.

The company sources are `fact_profit`, `fact_expense`, and the derived view `"7-4收入计算表"`. The derived revenue view is recreated earlier in the same AI ETL flow from `fact_revenue`.

## Plan of Work

Define three company-level configurations before the existing `data_type` branch, append them to the selected branch's configurations, and update flow documentation/count descriptions. Add the new names to `EXCLUDE_TABLES`. Validate Python syntax and run focused pre-commit checks on the changed Python files. Do not run the Prefect flow or write database views because the target database/environment has not been explicitly identified for execution.

## Concrete Steps

From `/root/prefect`:

1. Edit `modules/ai_data_etl/flows/ai_data_etl_flow.py` and `modules/view_update/tasks/view_update_tasks.py` with `apply_patch`.
2. Run `venv/bin/python -m compileall modules/ai_data_etl modules/view_update`.
3. Run `venv/bin/pre-commit run --files modules/ai_data_etl/flows/ai_data_etl_flow.py modules/view_update/tasks/view_update_tasks.py` when the checked-in hook environment is usable.
4. Inspect the final diff and confirm each target view maps to the intended source.

## Validation and Acceptance

Acceptance requires static evidence that:

- every selected `data_type` configuration includes `ai_revenue`, `ai_profit`, and `ai_expense`;
- `ai_profit` selects from `fact_profit`;
- `ai_revenue` selects from `"7-4收入计算表"` and retains customer and organization fields;
- `ai_expense` selects from `fact_expense` and retains normalized amount, project, expense dimensions, and organization fields;
- generic view refresh excludes all three new views;
- changed Python files compile and pass focused formatting/lint checks.

Runtime/database acceptance, when authorized for a named non-production or production target, should run `ai_data_etl_flow` with explicit parameters, inspect Prefect flow/task states, query `information_schema.views`, compare source/view row counts, and confirm representative amount totals. That runtime action is outside this implementation pass unless separately authorized with a database target.

## Idempotence and Recovery

The existing `create_ai_view_task` drops and recreates each view, so rerunning the flow converges to the checked-in definition. Each view is committed independently; if a later view fails, earlier views remain updated. Recovery is to fix the failing SQL and rerun the flow. The change does not alter source tables or financial rows.

## Artifacts and Notes

- Existing company profit source confirmed at `fact_profit`.
- Existing company revenue transformation confirmed through `"7-4收入计算表"`.
- Existing company expense transformation confirmed from `fact_expense` joined to `dim_exp_item`.
- `venv/bin/python -m compileall -q modules/ai_data_etl modules/view_update` completed successfully.
- Focused `venv/bin/pre-commit run --files ...` passed trim-whitespace, EOF, AST, merge-conflict, debug-statement, Black, isort, and flake8 checks.
- `git diff --check` completed successfully.
- Prefect flow run `79fe82df-5411-4fd2-8fd8-87bf120c890b` (`auspicious-cuscus`) completed with `data_type="业务线数据"` and `calc_budget_profit=False`; all 9 configured AI views were created.
- Database readback: `ai_profit` 713443 rows = `fact_profit` 713443 rows; `ai_expense` 324243 rows = `fact_expense` 324243 rows; `ai_revenue` 313272 rows = `"7-4收入计算表"` 313272 rows.
- Database metadata confirmed `ai_profit` has 11 columns, `ai_expense` has 33 columns, and `ai_revenue` has 41 columns.

## Interfaces and Dependencies

- Prefect flow: `modules.ai_data_etl.flows.ai_data_etl_flow.ai_data_etl_flow`
- Prefect task: `modules.ai_data_etl.tasks.ai_data_etl_tasks.create_ai_view_task`
- Database sources: `fact_revenue`, `fact_profit`, `fact_expense`, `fone_cust_group`, `dim_exp_item`
- Derived dependency: `"7-4收入计算表"`
- New database views: `ai_revenue`, `ai_profit`, `ai_expense`
- Existing deployment registrations are unchanged because no flow is added, renamed, or removed.

## Revision Notes

- 2026-07-15: Created the plan after repository inspection and documented stable company-view mappings and the no-runtime-write boundary.
- 2026-07-15: Marked implementation and static validation complete; retained runtime database verification as an explicitly environment-bound follow-up.
- 2026-07-15: Recorded the authorized runtime creation in `mydb` and exact Prefect/database verification evidence.
