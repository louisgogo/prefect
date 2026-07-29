# Deploy and verify FONE refresh-only execution

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The manually triggered `fone_income_expense_refresh_flow` currently executes FONE-owned scripts that both rebuild financial detail tables and then generate an Excel file, send a WeCom message, and write a delivery log. A delivery-side exception can occur after the database refresh has succeeded, causing Prefect to report the whole run as failed and skip the expense stage. After this work, the Prefect button will compile and execute only the data-refresh and lock-release sections of the authoritative FONE scripts, so file delivery failures cannot misclassify a successful database refresh. Operators will observe a completed Prefect run only after income and expense tables both pass explicit database postconditions.

## Progress

- [x] (2026-07-29 06:59Z) Read repository planning rules, inspected the current branch, flow, tasks, tests, deployment registrations, and operating documentation.
- [x] (2026-07-29 06:59Z) Identified pushed commit `b49d9d7` that implements refresh-only script compilation but is not yet loaded by the production worker, whose last served deployment used `4233aed8`.
- [x] (2026-07-29 07:02Z) Audited both live FONE definitions: each has exactly one ordered delivery, unlock, operation-log, and catch marker; compiled refresh-only scripts retain period/user substitution and both lock-release paths while removing delivery and operation-log sections.
- [x] (2026-07-29 07:07Z) Passed 20 focused FONE tests, all 72 repository tests, compile/import checks, `git diff --check`, and scoped Black/isort/flake8 hooks; updated the proxy-auth fixture to exercise refresh-only compilation.
- [ ] Restart `prefect-workers.service` from the pushed branch and verify the target deployment is READY at the intended commit.
- [ ] Trigger the explicit `2026/6` production verification run, inspect Prefect logs, and independently validate all three target tables.
- [ ] Update durable documentation and this plan with evidence, archive the plan, and send the WeCom completion notification.

## Surprises & Discoveries

- A user-triggered run `84aceb0d-6a77-495b-bbaf-7378a94ae4de` returned `script_status=-1` with two FONE errors after rebuilding the income table to 11,666 June 2026 rows. Because the execution task raised before database validation, Prefect marked the run failed and skipped expense even though the core income refresh succeeded.
- The branch advanced from deployed commit `4233aed8` to pushed commit `b49d9d7` while the worker remained active from 2026-07-29 14:37 CST. The current production deployment therefore does not yet use the committed refresh-only behavior.
- Both live content definitions matched the committed structural assumptions on 2026-07-29: income was reduced from 292 to 212 script lines and expense from 303 to 200, with delivery/log markers absent and unlock/catch markers retained after compilation.
- `pre-commit run --all-files` found and reformatted three unrelated historical files. Those mechanical changes were restored exactly from `HEAD`; scoped checks for all FONE paths pass. The repository-wide run therefore remains non-clean only because of unrelated pre-existing formatting, not this change.

## Decision Log

- Decision: Remove Excel generation, WeCom delivery, and delivery-operation logging from the dynamically loaded scripts instead of treating their failures as successful refreshes after the fact.
  Rationale: The button's business purpose is database refresh. Executing fewer side effects avoids ambiguous partial success and retains strict failure handling for the remaining data-refresh script.
  Date/Author: 2026-07-29 / Codex
- Decision: Preserve the FONE script's normal and exception-path lock release, and reject execution when expected source markers are absent, duplicated, or reordered.
  Rationale: Dynamic text slicing is safe only while the authoritative script structure is explicitly recognized; fail-closed behavior prevents accidental execution of an unknown script layout.
  Date/Author: 2026-07-29 / Codex
- Decision: Use `2026/6` for production verification and do not use a relative-month default.
  Rationale: The current requested operation is “获取FONE上月数据” on 2026-07-29, and the target tables are whole-table snapshots that must be validated against an explicit accounting period.
  Date/Author: 2026-07-29 / Codex

## Outcomes & Retrospective

Pending implementation audit, rollout, and production verification.

## Context and Orientation

The public flow is `modules/recon/flows/fone_income_expense_refresh_flow.py:fone_income_expense_refresh_flow`, deployed remotely as `子流程-FONE收入费用明细刷新`. Dynamic content loading, script compilation, execution, and aggregate database validation live in `modules/recon/tasks/fone_income_expense_tasks.py`. Registrations are defined in `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`. Operational behavior is documented in `docs/fone_income_expense_refresh_flow.md`.

The flow refreshes whole-table snapshots in `fone_db.FONE_MRPT_AC_OffLineFormat`, `fone_db.FONE_MRPT_FY_OffLineFormat`, and `fone_db.FONE_MRPT_FY_OffLineDetail`. It uses the AIHub proxy token from `AIHUB_FONE_API_TOKEN` and the permission-bearing FONE account from `FONE_DETAIL_PERMISSION_USER`. The production service is `prefect-workers.service`, which reads `/root/prefect/.env`, pulls the tracked Git branch before startup, and runs `deploy_to_server.py` inside the checked-in virtual environment.

## Plan of Work

First audit commit `b49d9d7` and verify that refresh-only compilation retains the data-refresh prefix, the success-path lock release, the exception cleanup, and all required variable definitions for both live income and expense content. Ensure it fails before execution if source markers have changed.

Next run the focused unit tests and repository checks appropriate to the changed task, documentation, and tests. If defects are found, patch narrowly, preserve unrelated untracked directories, and commit and push only the intended paths.

Then confirm the intended commit exists on `origin/session/prefect`, restart only `prefect-workers.service`, wait for systemd-managed shutdown/startup, verify the worker environment and deployment version, and trigger one explicit June 2026 run. Inspect Flow and Task Runs and independently query aggregate row counts, ID ranges, and periods from all three target tables.

Finally record the evidence, update durable documentation if needed, archive this plan, and send a concise WeCom completion notification.

## Concrete Steps

Run from `/root/prefect` and activate `venv` for Python, Prefect, and pre-commit commands.

1. Inspect `git show b49d9d7`, current source, tests, and live FONE content marker counts without printing script bodies or credentials.
2. Run `python -m unittest tests.test_fone_income_expense_refresh tests.test_fone_proxy_auth`, compile/import checks, `git diff --check`, and focused pre-commit hooks.
3. Confirm `git rev-list --left-right --count HEAD...@{upstream}` is `0 0` before the production restart.
4. Run `systemctl restart --no-block prefect-workers.service`, poll systemd without force-killing runners, inspect worker logs, and verify `子流程-FONE收入费用明细刷新` is READY at the intended version.
5. Run the deployment with `year=2026` and `month=6`. Save the flow-run ID, monitor it to a terminal state, and query aggregate target-table state.

## Validation and Acceptance

Acceptance requires all focused tests and checks to pass; both live FONE definitions to contain exactly one ordered set of recognized refresh/delivery/unlock/log/catch markers; the production worker to be active and serve deployment version `b49d9d7` or a later reviewed fix; and the explicit June 2026 run to complete with income before expense. The income table must be non-empty and contain only `2026-06-01`; expense format must contain only `2026-06-01`; expense detail must contain only `2026-M6`; and each table's ID signature must change from its pre-run state.

The verification run must not execute the removed Excel generation or WeCom delivery sections. Prefect logs must contain compact execution and table-validation summaries without credentials, script bodies, row-level financial data, or download URLs.

## Idempotence and Recovery

The source scripts delete and rebuild whole tables and are not transactional from Prefect's perspective. Tasks have no automatic retries. A timeout remains an unknown state and requires aggregate database inspection before any manual rerun. Marker validation occurs before the script execution request, so an unrecognized source structure fails without clearing tables. The FONE script's normal and exception cleanup retain lock release. Production verification uses one explicit period and no concurrent trigger.

If a run fails after execution begins, inspect the three aggregate table signatures before deciding whether to rerun. Do not infer failure solely from HTTP or script status, and do not trigger an automatic retry.

## Artifacts and Notes

- Previous successful run: `1d86e948-0e77-485e-acdd-1e44a17db720`, completed for June 2026.
- Misclassified user-triggered run: `84aceb0d-6a77-495b-bbaf-7378a94ae4de`; income refreshed to 11,666 rows but post-refresh delivery returned two errors and expense was skipped.
- Intended code commit at plan creation: `b49d9d723958b797c114249e8685368ae6ab0a30`, already present on `origin/session/prefect`.

## Interfaces and Dependencies

No new Python dependency is expected. The implementation depends on Prefect tasks and flows, `requests`, the AIHub FONE proxy, the authoritative income and expense FONE content IDs, the configured `songsong` FONE permission account, FONE MySQL aggregate read access, and the systemd-managed worker service. Public flow names, parameters, target tables, and deployment entry points remain unchanged.

## Revision Notes

- 2026-07-29: Created the plan after discovering that the exact delivery-decoupling fix is already committed and pushed but not yet deployed to the active production worker.
