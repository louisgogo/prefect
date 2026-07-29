# Deploy the single-basis R&D profitability report

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Promote the already verified `rd_project_profitability_flow` update to the production Prefect workers. The production personal workbench currently fails because the long-running worker holds an old validation wrapper that still reads `power_bi_non_additive_expense_gap`, while the source calculation has moved to the single-basis result structure. Completion is observable when the intended revision is pushed, the systemd-managed workers restart from a clean production checkout, and an explicit read-only run for 2026-01-01 through 2026-06-30 completes and produces the four-sheet workbook.

## Progress

- [x] (2026-07-29 02:35Z) Confirmed production failure `d88bd3a0-8607-4c5b-9014-d2bf6bb6c976` is a stale-code `KeyError` in validation, not a user permission or source-data failure.
- [x] (2026-07-29 02:40Z) Identified the intended R&D profitability changes and separated unrelated inventory-impairment worktree changes.
- [x] (2026-07-29 02:42Z) Passed five focused unit tests, `git diff --check`, and focused pre-commit hooks.
- [ ] Commit and push only the intended R&D profitability revision.
- [ ] Preserve unrelated work outside the clean production checkout and restart `prefect-workers.service`.
- [ ] Run and verify an explicit production read-only flow execution.

## Surprises & Discoveries

- `prefect-workers.service` has been running since 2026-07-28 17:28 CST, before the R&D source files changed on 2026-07-29 around 09:40 CST.
- The production repository also contains unrelated uncommitted inventory-impairment files and deployment registrations. Restarting directly from that dirty checkout would unintentionally publish them.
- The current R&D source and tests no longer reference `power_bi_non_additive_expense_gap`; all focused tests pass.

## Decision Log

- Decision: Treat the user's request as authorization to deploy the verified R&D profitability workflow, not to copy test database business rows into production.
  Rationale: The request follows directly from the stale Prefect code diagnosis, and the relevant archived implementation plan explicitly awaited separate deployment authorization.
  Date/Author: 2026-07-29 / Codex.
- Decision: Publish only the R&D flow, task, test, and canonical R&D documentation changes.
  Rationale: Inventory-impairment and other worktree changes are unrelated and not production-ready within this rollout.
  Date/Author: 2026-07-29 / Codex.
- Decision: Verify with `write_to_db=false` and explicit dates.
  Rationale: The personal workbench report is an Excel delivery; production verification does not require overwriting a financial snapshot table.
  Date/Author: 2026-07-29 / Codex.

## Outcomes & Retrospective

Pending production rollout and verification.

## Context and Orientation

The flow is `modules/rd_project_profitability/flows/rd_project_profitability_flow.py`; calculation, validation, and Excel export are in `modules/rd_project_profitability/tasks/rd_project_profitability_tasks.py`. The deployment name is `主流程-研发项目收益分析`. Production workers are managed by `prefect-workers.service`, working from `/root/prefect` and running `deploy_to_server.py`. The service pulls the tracked branch on restart, so the exact intended commit must be pushed first. The production run must keep `write_to_db=false` and use 2026-01-01 through 2026-06-30.

## Plan of Work

Create one focused revision containing only the R&D profitability flow, tasks, tests, and canonical R&D documentation. Push that revision to the tracked `session/prefect` branch. Preserve all unrelated dirty work without deploying it, leave `/root/prefect` as a clean production checkout, and restart the systemd-managed workers. Confirm the deployment version changes, then trigger one explicit read-only production run, inspect its task states and worker logs, and verify that the generated workbook has four sheets including `收入成本备查`.

## Concrete Steps

From `/root/prefect`, activate `venv`. Run `python -m unittest tests.test_rd_project_profitability -v`, `git diff --check`, and focused pre-commit hooks. Commit only the intended paths and push `session/prefect`. Preserve unrelated files in a separate development worktree or recoverable Git stash before cleaning the production checkout. Restart with `systemctl restart prefect-workers`, inspect status and journal logs, then execute deployment `主流程-研发项目收益分析` with explicit dates and `write_to_db=false`.

## Validation and Acceptance

- The focused five-test suite passes.
- The pushed commit contains no inventory-impairment files or registrations.
- `/root/prefect` is clean at the pushed intended commit when workers start.
- `prefect-workers.service` is active after restart.
- Deployment `主流程-研发项目收益分析` reports the new revision.
- A production flow run for 2026-01-01 through 2026-06-30 completes with no database write.
- Its report contains four sheets and a nonzero income-cost backup row count.
- Worker logs contain no R&D profitability exception for the verification run.

## Idempotence and Recovery

Tests and the verification flow run are safe to repeat because `write_to_db=false`. Git stashes or a separate worktree preserve unrelated changes. If the worker restart fails, keep the production checkout at the pushed commit, inspect import and registration errors, and restart the same systemd service after correcting only the deployment issue. Do not restore unrelated source edits into the production checkout after workers start because that can recreate mixed in-memory/on-disk code.

## Artifacts and Notes

Failed personal-workbench run: `d88bd3a0-8607-4c5b-9014-d2bf6bb6c976`. Prior verified implementation run: `804c2916-7060-4621-9ba0-72827aa988a1`. Expected period totals remain revenue 736,636,078.99, cost 474,249,880.54, expense 115,636,794.82, and remaining profit 146,749,403.63.

## Interfaces and Dependencies

Public flow parameters remain `start_date`, `end_date`, `write_to_db`, `target_table`, `output_dir`, `download_base_url`, `notify_frontend`, `callback_url`, and `tolerance`. The flow uses the existing pandas/openpyxl environment and current production source systems. No schema migration or database row copy is part of this rollout.

## Revision Notes

- 2026-07-29: Created after the user authorized production promotion of the verified single-basis R&D report.
