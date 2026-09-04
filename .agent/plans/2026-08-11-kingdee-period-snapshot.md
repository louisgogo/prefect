# Make Kingdee period synchronization authoritative

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The Kingdee voucher workflow currently reads a complete accounting period but only upserts returned rows. Entries deleted or removed from a voucher in Kingdee remain in `fact_gl_voucher_journal` and distort reconciliation. After this change, a successful period synchronization removes local rows for that exact year and period that were not returned by the completed source scan. Failed or interrupted scans do not prune rows.

## Progress

- [x] (2026-08-11 06:04Z) Confirmed the current flow performs a full source scan with page-level upserts and no absent-row pruning.
- [x] (2026-08-11 06:15Z) Added authoritative period pruning during successful finalization and deleted-row reporting.
- [x] (2026-08-11 06:32Z) Added focused prune predicate and task/flow result tests, including the empty-source-compatible finalization path.
- [x] (2026-08-11 06:32Z) Passed 13 focused tests, all changed-file pre-commit hooks, and `git diff --check`.
- [ ] Commit and push the Prefect topic branch and open a pull request to the running integration branch; do not deploy or restart production workers without separate authorization.

## Surprises & Discoveries

- The active Kingdee workflow is on `origin/session/prefect`, not the repository default `origin/main`.
- For 2026 period 7, the latest source scan returned 78,365 rows while the table contained 78,546 rows; all 181 extra rows had `last_synced_at` before the latest run start.

## Decision Log

- Decision: Use the synchronization run start timestamp as the generation marker and delete only rows for the requested year/period whose `last_synced_at` is older than that marker, after the final source page succeeds.
  Rationale: Every returned row is inserted or updated during the run and receives a new `last_synced_at`. This avoids collecting tens of thousands of IDs in memory and requires no schema change.
  Date/Author: 2026-08-11 / Codex.
- Decision: Perform pruning and marking the run completed in one final transaction.
  Rationale: A run must never be recorded as completed while stale rows remain, and a failed prune must preserve the previous snapshot and leave the run failed.
  Date/Author: 2026-08-11 / Codex.
- Decision: Permit an empty authoritative source result to delete all rows for that explicit period only after the request loop completed successfully.
  Rationale: A legitimately empty period must converge to an empty snapshot; transport or API errors already raise before finalization.
  Date/Author: 2026-08-11 / Codex.

## Outcomes & Retrospective

The implementation and local validation are complete. Integration and any worker rollout remain pending.

## Context and Orientation

`modules/kingdee_voucher/tasks/kingdee_voucher_tasks.py` requests `GL_VOUCHER` rows ordered by `FEntity_FEntryID`, normalizes them, and upserts each page into finance table `fact_gl_voucher_journal`. `last_synced_at` is updated by the `ON CONFLICT` clause. Run state is tracked in `kingdee_gl_voucher_sync_runs`. The public Prefect flow and deployment registrations do not need interface changes.

## Plan of Work

Change `_start_run` to return both the run UUID and its database `started_at` timestamp. Add a finalization helper that deletes rows matching the explicit fiscal year and period with `last_synced_at < run_started_at`, updates the run with `status='completed'`, and commits once. Return `deleted_rows` from the task and aggregate it in the flow result and completion logs. Keep page commits so long scans retain existing progress behavior; because pruning only happens at finalization, any failed scan leaves prior rows available and is not treated as a completed authoritative snapshot.

Add unit tests that inspect the delete predicate and transaction ordering, verify no finalization after a page/request failure, and update existing result assertions for `deleted_rows`.

## Concrete Steps

Work in `/root/worktrees/prefect/kingdee-voucher-snapshot`.

1. Patch task helpers and result payloads.
2. Extend `tests/test_kingdee_voucher_journal.py`.
3. Run the focused test file in the repository `venv` when available.
4. Run pre-commit on changed files or the closest available Black/isort/flake8 checks, then `git diff --check`.
5. Commit, push, and open a pull request to `session/prefect`. Do not trigger a production flow or restart a worker in this task.

## Validation and Acceptance

- Returned rows have `last_synced_at` at or after the run start.
- Only older rows for the exact requested year and period are deleted.
- Rows from other periods are untouched.
- The completed run and prune commit atomically.
- A request, normalization, page-order, upsert, or prune failure marks the run failed and never completes it.
- The task and flow report `deleted_rows`.
- Focused tests and `git diff --check` pass.

## Idempotence and Recovery

Rerunning a successful period is idempotent with respect to source IDs: all current rows are refreshed and no absent rows remain. If finalization fails, the transaction rolls back the delete and completion update; the run is marked failed by the existing failure path. A subsequent explicit rerun safely converges the period. No schema migration is required.

## Artifacts and Notes

Production data evidence must not be committed. Tests use synthetic IDs and periods.

## Interfaces and Dependencies

The task result gains `deleted_rows`; the Prefect flow aggregate and logs gain the same field. Existing flow parameters, deployment names, database tables, and external API fields remain unchanged.

## Revision Notes

- 2026-08-11: Initial plan created from the confirmed stale-row reconciliation incident.
- 2026-08-11: Updated after implementation and successful focused tests/pre-commit validation.
