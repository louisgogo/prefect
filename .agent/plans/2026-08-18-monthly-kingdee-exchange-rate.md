# Add monthly Kingdee exchange-rate refresh

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The daily `业报基础数据更新` Prefect flow will gain an independent `exchange_rate` dataset. Each run reads USD/CNY and EUR/CNY rates from Kingdee `BD_Rate` and replaces only the selected calendar month's rows in `excel_exchange_rates`; historical months remain untouched. The existing daily 06:00 schedule includes the dataset, and business-report editors can retry it independently after the companion FastAPI contract and approved finance migration are applied.

## Progress

- [x] (2026-08-18 02:49Z) Confirmed all 323 stored business keys exist in Kingdee and all direct/indirect amounts match; Kingdee has two new August fixed-rate rows and two historical status-only differences.
- [x] (2026-08-18 02:49Z) Created feature worktree from `origin/session/prefect` because this repository has no remote `develop` branch.
- [x] (2026-08-18 03:07Z) Implemented month resolution, Kingdee normalization/validation, and transactional month replacement with a historical-row invariant.
- [x] (2026-08-18 03:07Z) Registered `exchange_rate` in the flow, exports, daily deployment parameters, documentation, and focused tests.
- [x] (2026-08-18 03:07Z) Disabled routine Excel exchange-rate writes by default while retaining an explicit emergency flag.
- [x] (2026-08-18 03:07Z) Ran 18 focused unit tests, flake8, compile checks, and a real read-only August Kingdee query returning the expected two rows.

## Surprises & Discoveries

- The Prefect remote has no `develop`; the active integration branch is `session/prefect`.
- Kingdee start date is `FBegDate`, while the target column is `effective_date`.
- The current target has 323 rows through 2026-07-31. All amounts match Kingdee; two 2020 rows differ only because Kingdee now reports `已审核`.
- The older `data_import_flow` remained a competing Excel writer. It is now disabled by default for exchange rates so a later last-month import cannot overwrite the authoritative Kingdee result.
- The checked-in Prefect virtual environment does not include pytest; the focused suite runs successfully through its unittest entry point. Black/isort expose pre-existing formatting drift in touched legacy files, while flake8 passes.

## Decision Log

- Decision: Replace only rows whose `effective_date` falls in the selected month.
  Rationale: The user explicitly requested monthly refresh without historical restatement. Re-running the same month remains idempotent and can pick up the month-end spot rate later.
  Date/Author: 2026-08-18 / Codex.
- Decision: Default scheduled runs to the current Asia/Shanghai calendar month while accepting explicit `exchange_rate_year` and `exchange_rate_month` parameters.
  Rationale: The daily deployment needs automatic month rollover, while explicit parameters make manual verification and recovery reproducible.
  Date/Author: 2026-08-18 / Codex.
- Decision: Preserve Kingdee status fields rather than silently filtering them.
  Rationale: The existing table includes status columns and historically contained non-final statuses; preserving source state avoids changing business semantics during source replacement.
  Date/Author: 2026-08-18 / Codex.
- Decision: Make `import_exchange_rates_from_excel` default to false and retain it only as an explicit fallback.
  Rationale: Two routine writers would allow the older monthly Excel import to overwrite a completed Kingdee month after the daily flow has moved to the next month.
  Date/Author: 2026-08-18 / Codex.

## Outcomes & Retrospective

Local implementation is complete. The daily flow includes a month-scoped Kingdee exchange-rate dataset, and the old Excel path is opt-in only. Publication, deployment registration, worker restart, and production data writes remain pending separate authorization.

## Context and Orientation

`modules/business_data_refresh/flows/business_data_refresh_flow.py` orchestrates independent datasets and writes run state to `sys_business_data_sync_run` and `sys_business_data_sync_item`. `modules/business_data_refresh/tasks/business_data_refresh_tasks.py` already contains the authenticated, paginated Kingdee `BillQuery` helper. `deploy_to_server.py` and `deploy_production.py` register the flow daily at 06:00 Asia/Shanghai. The target `excel_exchange_rates` contains the legacy Excel-shaped final schema and is intentionally retained for downstream compatibility.

The companion FastAPI worktree adds `exchange_rate` to the API dataset contract and expands the two approved check constraints on the finance sync-run tables. No target exchange-rate table structure changes are required.

## Plan of Work

Add constants and a normalized exchange-rate record representation. Resolve the period with strict year/month validation. Query `BD_Rate` for source currencies `PRE007` (USD) and `PRE003` (EUR), target `PRE001` (CNY), and the selected `FBegDate` month. Normalize status codes and dates into the existing target columns, reject empty/duplicate/invalid rows, then use a temporary table and one transaction to delete and replace only the selected month.

Expose a retrying Prefect task and add it as the final independent dataset in the canonical order. Add optional period parameters to the flow and deployment defaults, update operator documentation, and add unit tests for request filters, normalization, duplicate rejection, canonical ordering, and month-scoped SQL behavior.

## Concrete Steps

Run from `/root/worktrees/prefect/monthly-exchange-rate`:

    source /root/prefect/venv/bin/activate
    pytest -q tests/test_business_data_refresh.py
    pre-commit run --files <changed files>
    git diff --check

Use an explicit August 2026 read-only API/database comparison to confirm the calculation would insert two fixed-rate rows and leave all prior periods unchanged. Do not trigger the production flow or write production data without deployment authorization.

## Validation and Acceptance

The focused test suite must prove that `exchange_rate` is selectable, deployment defaults include it, explicit period parameters validate correctly, Kingdee pagination/filtering is correct, duplicate natural keys fail closed, and the month replacement SQL is bounded by the first and last day of the selected month. A read-only August comparison must report two source rows, zero existing August rows, and zero differences against overlapping stored amounts. Validation completed with 18 passing tests, successful flake8 and compile checks, and a live read-only Kingdee result of two normalized August rows.

## Idempotence and Recovery

The target write takes the `business-data:exchange_rate` advisory transaction lock, stages the complete selected-month source set, deletes only that month, inserts staged rows, verifies the month count, and commits. Any exception rolls back both deletion and insertion. Re-running the same month yields the same rows. Historical rows are outside the delete predicate. If Kingdee returns an empty or invalid month, the task fails before deletion.

## Artifacts and Notes

Initial evidence: database 323 rows; 323 keys found in Kingdee; zero amount differences; two historical status-only differences; two new August fixed-rate rows.

Validation evidence: 18 focused tests passed; the live `BD_Rate` query for 2026-08 returned EUR/CNY 7.7886 and USD/CNY 6.7894 with the expected dates and statuses.

## Interfaces and Dependencies

External form: Kingdee `BD_Rate` through `/api/proxy/erp/sdk/BillQuery`. Existing credentials `XGD_TOKEN` or `AIHUB_FONE_API_TOKEN` and optional `KINGDEE_VOUCHER_BASE_URL` are reused. Target: finance PostgreSQL `public.excel_exchange_rates`. Flow: `business_data_refresh_flow`; deployment: `子流程-业报基础数据更新`.

## Revision Notes

- 2026-08-18: Initial plan created after source-to-target reconciliation and database-constraint approval.
- 2026-08-18: Recorded completed local implementation, the single-writer Excel fallback decision, and validation evidence.
