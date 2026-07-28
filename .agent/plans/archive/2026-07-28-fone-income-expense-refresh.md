# Add a verified FONE income and expense detail refresh subflow

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Finance users currently refresh FONE income and expense detail through separate FONE report scripts. This work first executes and verifies the explicitly requested May 2026 expense refresh in production, then adds a manually triggered Prefect subflow that runs the existing FONE income and expense scripts sequentially for an explicit accounting period. The subflow must treat a successful HTTP response as insufficient: it verifies that the target FONE MySQL tables are non-empty and contain only the requested period, preventing the known failure mode where the API login user has no legal-entity permissions and a script clears a table without repopulating it.

## Progress

- [x] (2026-07-28 07:54Z) Reviewed repository planning rules, existing FONE tasks/flow, deployment entry points, and the dirty working tree.
- [x] (2026-07-28 07:54Z) Verified the May 2026 income refresh separately: `fone_db.FONE_MRPT_AC_OffLineFormat` contains 8,849 rows for `2026-05-01` after execution with the authorized FONE permission account.
- [x] (2026-07-28 07:58Z) Captured the expense baseline: format 3,444 rows with IDs 458534-461977; detail 2,500 rows with IDs 754760-757259; both contained only May 2026.
- [x] (2026-07-28 07:59Z) Executed the authorized May 2026 expense refresh and verified format 3,444 rows with IDs 461978-465421, detail 2,500 rows with IDs 757260-759759, one requested period in each table, non-zero amount aggregates, and a new operation-log timestamp.
- [x] (2026-07-28 08:07Z) Implemented dynamic FONE content loading, safe variable compilation, permission-account substitution, no-retry execution, explicit `fone_db` validation, and refresh-signature checks.
- [x] (2026-07-28 08:08Z) Added and exported the sequential income-and-expense Prefect subflow, registered it in local, remote, and production entry points, and documented its operation.
- [x] (2026-07-28 08:09Z) Passed 14 focused tests, all 66 repository tests, scoped pre-commit hooks, compile/import checks, flow-schema checks, real FONE-definition compilation, and `git diff --check`.
- [x] (2026-07-28 08:09Z) Sent the completion notification and archived this ExecPlan.

## Surprises & Discoveries

- The FONE `/api/login/prod` credential identifies the runtime user as `api`, which has no legal-entity permission for these report scripts. An income-script API call can therefore return success after clearing the target table but insert zero rows. Runtime substitution to the approved permission account is required, and a database postcondition is mandatory.
- The repository already contains unrelated uncommitted work in deployment scripts, `modules/__init__.py`, documentation, inventory modules, and tests. All edits in this plan must be narrow merges that preserve those changes.
- The expense refresh preserved the same row counts as the pre-run baseline while advancing both tables' auto-increment identifier ranges. This is evidence that the delete-and-reload completed rather than merely reusing the pre-existing May data.
- The shared `mypackage.utilities.connect_to_fone()` connection defaults to `fone_bi_db`. Income has a same-named table there, which can hide an incorrect schema assumption, while expense tables exist only in `fone_db`. Validation SQL must therefore qualify every target as `fone_db.<table>`.
- After the May expense refresh was verified at 2026-07-28 07:59Z, another external run replaced the whole expense tables with June 2026 data before implementation validation completed. The May evidence remains valid for the immediate post-run state, but the current production tables no longer represent May; this confirms that these whole-table refresh scripts require serialization and immediate post-run validation.

## Decision Log

- Decision: Load the current income and expense script definitions from FONE by content ID at runtime instead of storing their script text in Git.
  Rationale: The source scripts can change in FONE and contain credentials and environment-specific identifiers that must not be copied into the repository.
  Date/Author: 2026-07-28 / Codex
- Decision: Execute income first and expense second, with no automatic retry of either destructive delete-and-reload script.
  Rationale: Sequential execution makes the affected table and failure point clear. An HTTP timeout leaves execution state unknown, so automatic retry could race or repeat a destructive refresh.
  Date/Author: 2026-07-28 / Codex
- Decision: Require explicit `year` and `month` flow parameters and validate database period/row-count postconditions after each script.
  Rationale: Financial refreshes must not silently use a relative month, and API success alone does not prove the data reload succeeded.
  Date/Author: 2026-07-28 / Codex
- Decision: Qualify all validation reads with the `fone_db` schema even though the shared connection's default schema is `fone_bi_db`.
  Rationale: The operational scripts write `fone_db`; relying on the connection default validated the BI copy for income and failed to locate expense tables.
  Date/Author: 2026-07-28 / Codex

## Outcomes & Retrospective

The requested May 2026 income and expense refreshes were independently executed and verified immediately after their runs. The new `fone_income_expense_refresh_flow` now retrieves current script definitions from FONE, compiles explicit period variables, requires a permission-bearing user, runs income before expense without automatic retries, and verifies the `fone_db` tables after each stage. It is exported and registered in all three deployment entry points, with operational guidance under `docs/`.

No Prefect deployment, worker restart, combined-flow production trigger, commit, or push was performed. Two later external June expense runs replaced the whole expense tables after the verified May run, so the current expense tables are June rather than May. This did not invalidate the immediate May verification, but it demonstrates that the underlying FONE scripts are shared whole-table refreshes and that a later operator can supersede a completed run.

## Context and Orientation

Existing FONE API code lives in `modules/recon/tasks/fone_recon_tasks.py`; `modules/recon/flows/fone_recon_flow.py` is the current subflow for a different FONE reconciliation script. Public flow exports are in `modules/recon/__init__.py` and `modules/__init__.py`. Deployments are registered by `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`.

Operational usage and recovery guidance for the new subflow is in `docs/fone_income_expense_refresh_flow.md`.

The new flow targets explicit `year` and `month` parameters. Its source content IDs are the FONE income-detail and expense-detail report scripts already supplied and authorized by the user. The relevant output tables are `fone_db.FONE_MRPT_AC_OffLineFormat` for income and `fone_db.FONE_MRPT_FY_OffLineFormat` plus `fone_db.FONE_MRPT_FY_OffLineDetail` for expense. The scripts delete and reload whole target tables, not a single-period partition. FONE authentication, script retrieval, execution, and MySQL validation are external dependencies.

## Plan of Work

First, query aggregate baseline facts for both expense tables without printing row data or secrets. Retrieve the authoritative expense script definition from FONE, compile its variables for `2026` and `M5`, substitute the approved legal-entity permission account exactly once, execute it, and verify non-zero rows, period consistency, refreshed identifiers, and an operation-log entry.

Next, add reusable task helpers that retrieve a FONE content definition, parse its nested JSON, compile script variables, enforce the permission-account substitution, submit the execution request without logging console output or authentication material, and query aggregate table postconditions. Add a flow that validates an explicit period, logs in once, runs income then expense, validates each result, and reports compact stage summaries through the existing Hermes notification task.

Finally, export and register the flow in every deployment entry point, add unit tests for the pure compilation and validation helpers, run focused tests and pre-commit checks, inspect the diff for accidental credential or unrelated changes, then archive this completed plan and notify the user through the configured WeCom robot.

## Concrete Steps

Run commands from `/root/prefect`, activating `venv` when available.

1. Query aggregate baselines for `FONE_MRPT_FY_OffLineFormat` and `FONE_MRPT_FY_OffLineDetail`: row count, identifier range, and period distribution.
2. Run the FONE expense script for year `2026`, month `M5`, using the approved permission account. Do not print script console logs or access tokens.
3. Re-query both tables and the latest `FONE_WxInformation_Log` expense entry.
4. Edit the recon task/flow modules, exports, and three deployment scripts using `apply_patch`.
5. Add focused tests under `tests/`.
6. Run the focused test file, Black/isort/flake8 or relevant pre-commit hooks on changed files, import/compile checks, and inspect `git diff --check` plus the scoped diff.

## Validation and Acceptance

The production expense refresh is accepted when both expense tables are non-empty after execution, their accounting-period fields correspond to May 2026, the identifiers demonstrate a completed reload, and the operation log records the requested export. No row-level financial contents or credentials are retained in logs or the plan.

The code change is accepted when the new flow imports successfully, rejects invalid months, requires a configured permission account, fails if the account substitution is absent or ambiguous, compiles FONE variables deterministically, executes income before expense, does not automatically retry unknown-state calls, and validates non-zero requested-period data. `modules/recon/__init__.py`, `modules/__init__.py`, `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py` must all expose/register the new subflow. Focused tests and formatting checks must pass.

No deployment, service restart, Prefect flow trigger, commit, or push is part of this implementation unless separately authorized.

## Idempotence and Recovery

The FONE source scripts clear and rebuild entire target tables. They are operationally rerunnable for the same explicit period but are not transactional from Prefect's perspective. The new Prefect execution tasks therefore have no automatic retries. A timeout is reported as an unknown state and requires aggregate database inspection before any manual rerun. If validation fails after an API success, operators must inspect the relevant FONE script run and table aggregates; they must not assume an empty table is a valid result. The scripts' own FONE lock behavior serializes users, while the Prefect flow adds no independent distributed lock.

## Artifacts and Notes

- Income verification before this plan: 8,849 rows in `FONE_MRPT_AC_OffLineFormat`, all for `2026-05-01`, refreshed identifier range 380271 through 389119.
- Expense verification: 3,444 format rows for `2026-05-01` and 2,500 detail rows for `2026-M5`; both identifier ranges advanced from their pre-run baselines and both amount aggregates were non-zero.
- Later external state: two June expense exports completed at 2026-07-28 16:04:49 and 16:06:26 Asia/Shanghai, leaving 3,953 format rows and 2,924 detail rows for June 2026.
- Validation: 66 repository tests passed; all scoped pre-commit hooks, compile/import checks, required-flow-parameter checks, real content-definition compilation, deployment registration checks, and `git diff --check` passed.
- Sensitive FONE script text, tickets, API tokens, database passwords, and row samples must not appear in committed files, test fixtures, plan updates, or final output.

## Interfaces and Dependencies

The intended public flow is `fone_income_expense_refresh_flow`, deployed as `子流程-FONE收入费用明细刷新`. It depends on `requests`, Prefect tasks/flows, the existing FONE login endpoint, FONE content and script-execution endpoints, the two authoritative FONE content IDs, the configured permission account, and read access to aggregate metadata in `fone_db`. No new Python package is expected.

## Revision Notes

- 2026-07-28: Created the plan after the independently verified income refresh and before the authorized expense refresh, documenting the permission-account failure mode and mandatory database postconditions.
- 2026-07-28: Recorded the successful expense refresh, aggregate database evidence, and the advanced identifier ranges used to distinguish a completed reload from stale data.
- 2026-07-28: Corrected validation to explicitly read `fone_db` and recorded the later external June refresh that overwrote the verified May expense state.
- 2026-07-28: Recorded the completed implementation, registrations, focused tests, and operational documentation before final repository-wide validation.
- 2026-07-28: Completed all validation, recorded the later external June overwrite, sent the WeCom notification, and archived the plan.
