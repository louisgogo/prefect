# Deploy the R&D report and quarterly inventory impairment flow

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Promote the already verified `rd_project_profitability_flow` update and the quarterly inventory impairment flow to the production Prefect workers. The production personal workbench currently fails because the long-running worker holds an old validation wrapper that still reads `power_bi_non_additive_expense_gap`. The quarterly inventory impairment deployment is grey because its runner was not restored after the prior service restart. Completion is observable when both revisions are pushed, the systemd-managed workers restart from the intended commit, both deployments report ready, and explicit read-only runs complete.

## Progress

- [x] (2026-07-29 02:35Z) Confirmed production failure `d88bd3a0-8607-4c5b-9014-d2bf6bb6c976` is a stale-code `KeyError` in validation, not a user permission or source-data failure.
- [x] (2026-07-29 02:40Z) Identified the intended R&D profitability changes and separated unrelated inventory-impairment worktree changes.
- [x] (2026-07-29 02:42Z) Passed five focused unit tests, `git diff --check`, and focused pre-commit hooks.
- [x] (2026-07-29 03:06Z) Passed the combined 22-test inventory impairment and R&D profitability suite.
- [x] (2026-07-29 02:33Z) Completed a local read-only Q2 2026 inventory impairment smoke test: 6 calculated rows totaling 3,444,244.79, with no writeback.
- [x] (2026-07-29 02:35Z) Pushed R&D commit `0dcafcb` and inventory impairment commit `9d3ab7f` to `origin/session/prefect`.
- [x] (2026-07-29 02:35Z) Restarted `prefect-workers.service` through systemd from commit `9d3ab7f`.
- [x] (2026-07-29 02:37Z) Verified both deployments are unpaused and READY and both production read-only runs completed.

## Surprises & Discoveries

- `prefect-workers.service` has been running since 2026-07-28 17:28 CST, before the R&D source files changed on 2026-07-29 around 09:40 CST.
- The quarterly inventory impairment deployment is paused and `NOT_READY`; its independent runner process is absent from the current systemd service process tree.
- The current R&D source and tests no longer reference `power_bi_non_additive_expense_gap`; all focused tests pass.

## Decision Log

- Decision: Treat the user's request as authorization to deploy the verified R&D profitability workflow, not to copy test database business rows into production.
  Rationale: The request follows directly from the stale Prefect code diagnosis, and the relevant archived implementation plan explicitly awaited separate deployment authorization.
  Date/Author: 2026-07-29 / Codex.
- Decision: Publish both the R&D report and quarterly inventory impairment changes in separate focused commits.
  Rationale: The user explicitly authorized submission and restart after asking about the grey inventory deployment; both groups have focused tests and can be verified without database writes.
  Date/Author: 2026-07-29 / Codex.
- Decision: Verify with `write_to_db=false` and explicit dates.
  Rationale: The personal workbench report is an Excel delivery; production verification does not require overwriting a financial snapshot table.
  Date/Author: 2026-07-29 / Codex.

## Outcomes & Retrospective

The rollout completed successfully. `prefect-workers.service` is active and both `子流程-季度存货跌价计算` and `主流程-研发项目收益分析` are polling as READY deployments. Inventory verification run `9e4c6d0c-d9d3-4ae3-8da4-eab7865f9073` completed for 2026 Q2 with writeback disabled. R&D verification run `6c4074f0-1d4a-4faf-bb1a-4ea9d66e765c` completed for 2026 H1 with database writes and frontend notification disabled, producing 176 result rows, 1,149 backup rows, and the expected four-sheet workbook. The inventory comparison still contains one 327,117.64 historical difference caused by the documented delivery-date-first ageing rule; this was not written back.

## Context and Orientation

The R&D flow is `modules/rd_project_profitability/flows/rd_project_profitability_flow.py`; calculation, validation, and Excel export are in its task package. Its deployment is `主流程-研发项目收益分析`. The inventory flow lives under `modules/inventory_impairment/`; its deployment is `子流程-季度存货跌价计算`. Production workers are managed by `prefect-workers.service`, working from `/root/prefect` and running `deploy_to_server.py`. The service pulls the tracked branch on restart, so the exact intended commit must be pushed first. Verification runs must use explicit periods and disable database writes.

## Plan of Work

Create two focused revisions: first the quarterly inventory impairment flow, deployment registration, tests, and documentation; then the R&D profitability flow, tests, and documentation. Push both to `session/prefect`, restart the systemd-managed workers, and confirm both deployment versions and readiness. Trigger explicit read-only verification runs, inspect states and worker logs, verify the R&D workbook has four sheets including `收入成本备查`, and confirm the inventory run performs no fact-table writeback.

## Concrete Steps

From `/root/prefect`, activate `venv`. Run `python -m unittest tests.test_inventory_impairment tests.test_rd_project_profitability`, `git diff --check`, focused pre-commit hooks, and a local Q2 2026 inventory run with writeback disabled. Commit only the intended paths in two commits and push `session/prefect`. Restart with `systemctl restart prefect-workers.service`, inspect status and journal logs, then execute deployments `子流程-季度存货跌价计算` and `主流程-研发项目收益分析` with explicit periods and database writes disabled.

## Validation and Acceptance

- The combined 22-test suite and focused pre-commit hooks pass.
- The pushed commits contain only the two intended workflow groups and their shared registrations/docs.
- `prefect-workers.service` is active after restart.
- Both target deployments are unpaused and report ready.
- A Q2 2026 inventory run completes with writeback disabled.
- A production flow run for 2026-01-01 through 2026-06-30 completes with no database write.
- Its report contains four sheets and a nonzero income-cost backup row count.
- Worker logs contain no R&D profitability exception for the verification run.

## Idempotence and Recovery

Tests and verification runs are safe to repeat because inventory `write_to_fact_profit_bd=false` and R&D `write_to_db=false`. If the worker restart fails, keep the checkout at the pushed commit, inspect import and registration errors, and restart the same systemd service after correcting only the deployment issue.

## Artifacts and Notes

Failed personal-workbench run: `d88bd3a0-8607-4c5b-9014-d2bf6bb6c976`. Prior verified implementation run: `804c2916-7060-4621-9ba0-72827aa988a1`. Expected period totals remain revenue 736,636,078.99, cost 474,249,880.54, expense 115,636,794.82, and remaining profit 146,749,403.63.

Production inventory verification run: `9e4c6d0c-d9d3-4ae3-8da4-eab7865f9073`; calculated total 3,444,244.79, status counts 5 matched and 1 different, no writeback. Production R&D verification run: `6c4074f0-1d4a-4faf-bb1a-4ea9d66e765c`; workbook sheets are `研发项目收益`, `汇总与校验`, `计算口径`, and `收入成本备查`.

## Interfaces and Dependencies

Public flow parameters remain `start_date`, `end_date`, `write_to_db`, `target_table`, `output_dir`, `download_base_url`, `notify_frontend`, `callback_url`, and `tolerance`. The flow uses the existing pandas/openpyxl environment and current production source systems. No schema migration or database row copy is part of this rollout.

## Revision Notes

- 2026-07-29: Created after the user authorized production promotion of the verified single-basis R&D report.
- 2026-07-29: Expanded to include the quarterly inventory impairment flow after the user authorized submission and service restart in the context of its grey deployment.
- 2026-07-29: Recorded successful commits, systemd restart, deployment readiness, and read-only production verification; plan is complete and ready for archive.
