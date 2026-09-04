# Roll out monthly business-line staging batches

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Apply the reviewed single-table monthly batch model to the configured PostgreSQL database, prove that every existing staging row and filled ratio survives migration unchanged, publish the exact source revision used by the systemd-managed Prefect workers, and restart the workers safely. `fact_bus_line` remains the final unversioned approved ratio table and must not be modified by the schema migration.

## Progress

- [x] (2026-07-16 03:15Z) Confirmed `prefect-server.service` and `prefect-workers.service` are active and workers run `/root/prefect/deploy_to_server.py` after a mandatory `git pull`.
- [x] (2026-07-16 03:20Z) Confirmed the repository is on `session/prefect` at `7a30491` with unrelated dirty work that must not be included in the rollout commit.
- [x] (2026-07-16 04:05Z) Captured read-only metrics for 176,079 staging rows across 6 populated months; all logical keys and required identifiers are valid.
- [x] (2026-07-16 04:20Z) Added and dry-ran full-history baseline migration: 5 `PUBLISHED`, 1 `FILLING`, all protected checksums unchanged, transaction rolled back.
- [x] (2026-07-16 04:42Z) Applied the migration to the configured database and verified 176,079 staging rows, protected checksums, audit states, ratio values, and the unchanged 1,814,912-row `fact_bus_line` guard.
- [x] (2026-07-16 04:55Z) Committed the batch rollout, company AI views, and durable business documentation as `7892f43`, `4363ef6`, and `2ceb5a6`; pushed and confirmed `origin/session/prefect` at `2ceb5a6e798f7c9a1afe9398847b8fec298ca9a7`.
- [x] (2026-07-16 05:02Z) Restarted `prefect-workers.service`, verified API health and 14 registered deployments, and confirmed the unpaused batch-aware staging deployment `0efda22a-afaf-4eb7-b16d-6878544fa7a3`.
- [x] (2026-07-16 05:04Z) Reviewed remaining untracked paths, excluded local indexes/check outputs and a document containing a plaintext credential, and prepared this completed plan for archival.

## Surprises & Discoveries

- The workers unit has `ExecStartPre=/usr/bin/git -C /root/prefect pull` without a leading `-`; a restart will fail if the current branch cannot pull cleanly. The intended code must therefore be committed and pushed before restart.
- The working tree contains unrelated changes in AI ETL, view updates, and other untracked paths. Deployment staging must exclude them.
- Existing source implementation intentionally did not apply the database migration; production staging tables still lack `batch_id`.
- Staging dates span 2025-04 through 2026-06, but only 6 distinct months currently contain rows: 2025-04 and 2026-02 through 2026-06.
- The requested expense source-level change is already committed and pushed on `feature/expense-source-level` at `fbd9dca`; its code and tests are included in the combined rollout.
- Several old `flow.serve()` child processes did not exit within systemd's 90-second graceful-stop window. systemd terminated the residual children and started the replacement worker process successfully; no live Flow Run subprocess was found, and six months-old `RUNNING` states were stale orchestration records.
- The remaining untracked `docs/caiwu-data-pipeline-api.md` contains a plaintext API credential. It was deliberately excluded from Git and the credential should be rotated before any sanitized version of the document is considered for publication.

## Decision Log

- Decision: Use one `bus_line_staging_batch` table because one batch always represents exactly one accounting month.
  Rationale: Confirmed by the user; separate batch-period and pointer tables add no value.
  Date/Author: 2026-07-16 / Codex
- Decision: Create a version-zero baseline `BLS-YYYYMM-000` for every historical month before restarting workers.
  Rationale: New extractions can inherit existing ratios only when historical rows already have a batch identity.
  Date/Author: 2026-07-16 / Codex
- Decision: Mark a baseline `PUBLISHED` when `fact_bus_line` contains that month; otherwise mark it `FILLING`.
  Rationale: `fact_bus_line` is the final approved upload result and is the available evidence of publication.
  Date/Author: 2026-07-16 / Codex

## Outcomes & Retrospective

The configured database now uses one monthly staging-batch table with human-readable baseline batch numbers while preserving all historical filled ratios and audit status. All 176,079 existing staging rows were assigned a non-null baseline `batch_id`; five baselines are `PUBLISHED` and June 2026 is `FILLING`. `fact_bus_line` remained the unversioned final reporting result and its 1,814,912 rows and aggregate ratio guard were unchanged.

The batch-aware Prefect code and the previously requested expense source metadata behavior are pushed to `origin/session/prefect`. The systemd-managed workers restarted successfully, the Prefect API is healthy, and the staging deployment is registered and unpaused. No financial calculation, staging extraction, or final upload flow was triggered during rollout, so verification relied on migration invariants, current-view reconciliation, focused tests, deployment registration, and worker health.

## Context and Orientation

The six editable runtime tables are `staging_bus_expense`, `staging_bus_revenue`, `staging_bus_profit_bd`, `staging_bus_inventory`, `staging_bus_receivable`, and `staging_bus_in_transit_inventory`. Runtime columns are Chinese and business-line ratios are stored as dynamic decimal columns. The migration adds only `batch_id` to those rows and creates `bus_line_staging_batch`; it does not update source financial fields, ratios, audit status, or `fact_bus_line`.

The systemd workers use `/root/prefect`, source `.env`, connect to `http://127.0.0.1:4200/api`, run a mandatory `git pull`, then start `deploy_to_server.py`. Only `prefect-workers.service` requires restart for flow-code registration; the Prefect server should remain running unless health checks prove otherwise.

## Plan of Work

First collect deterministic per-table/per-month snapshots containing row counts, non-null business-line ratio counts, audit-state counts, and numeric totals for stable amount columns. Verify `(source_no, unique_lvl)` is unique per table and month. Any duplicate key, missing source identifier, or unexplained table/schema difference stops the migration.

Add a migration command that supports `--dry-run` and `--apply`. It creates the schema, discovers every historical period, creates `BLS-YYYYMM-000`, updates only null `batch_id` values, and verifies all snapshot metrics inside one database transaction. Dry-run must roll back. Apply commits only after every assertion passes.

After post-commit readback, selectively commit and push only the staging-batch feature, migration, deployment-description, tests, and operational documentation required by the workers. Confirm `origin/session/prefect` resolves to the new commit before restarting `prefect-workers.service`. Inspect systemd status and journal output and verify the staging deployment is registered. Do not trigger the financial calculation flow or upload `fact_bus_line` during rollout.

## Concrete Steps

Run from `/root/prefect` using `venv/bin/python`:

1. Read-only preflight and migration dry-run with explicit output captured in this plan.
2. Run focused unit tests and pre-commit checks.
3. Apply the migration once and immediately run readback verification.
4. Create a Conventional Commit containing only intended files, push `session/prefect`, and verify the remote SHA.
5. Run `systemctl restart prefect-workers.service`, then inspect `systemctl status`, `journalctl -u prefect-workers.service`, Prefect health, and deployment listings.

## Validation and Acceptance

- Every pre-migration staging row has a non-null `batch_id` after migration.
- Each historical month has exactly one `BLS-YYYYMM-000` baseline.
- Per table and month, row counts, stable numeric totals, all business-line ratio values, and audit-state counts match before and after.
- No `fact_bus_line` row or schema is changed.
- The intended commit is present on `origin/session/prefect` before service restart.
- `prefect-workers.service` is active after restart and its journal shows the business-line staging deployment registered without import/schema errors.
- Prefect API health succeeds and deployment listings include `主流程-业务线Staging抽取`.

## Idempotence and Recovery

The migration inserts baseline batches with unique `(acct_period, version_no)` and updates only rows where `batch_id IS NULL`. A repeated dry-run or apply must produce no new baselines and no row changes. All schema creation, backfill, and validation occur in one transaction; any assertion failure rolls back. The worker restart happens only after database verification and remote-push confirmation. If workers fail to start, keep the database migration (it is backward-compatible for existing reads), inspect logs, and restore service using a forward fix or the last pushed commit without destructive Git cleanup.

## Artifacts and Notes

Populate with pre/post metrics, migration output, commit SHA, service status, and Prefect registration evidence. Do not record row-level financial data.

Preflight evidence:

- Staging row counts: expense 56,982; revenue 8,295; profit detail 221; inventory 66,628; receivable 5,599; in-transit 38,354.
- Blank source numbers: 0; blank unique levels: 0; duplicate `(period, source_no, unique_lvl)` keys: 0 across every table.
- Expense source metadata dry-run covered 56,982 rows across 4 expense months; no source-level backfill remained and `数据来源` was already normalized to `费用`.
- Batch migration dry-run covered 176,079 rows and left all row-content checksums plus the 1,814,912-row `fact_bus_line` guard unchanged.

Completion evidence:

- Applied migration created six baselines: 2025-04 and 2026-02 through 2026-06; five are `PUBLISHED`, and `BLS-202606-000` is `FILLING`.
- All six staging tables have non-null `batch_id`. Current June views reconcile exactly to their active-batch rows: expense 13,728; revenue 1,781; profit detail 47; inventory 11,522; receivable 978; in-transit 7,278.
- Expense metadata guards pass: blank `来源层级` count 0 and `数据来源 != 费用` count 0.
- Twelve focused unit tests passed; focused pre-commit and compile checks passed.
- Published commits: `7892f43` (monthly staging batches and source metadata), `4363ef6` (company AI views), and `2ceb5a6` (business-line documentation).
- `prefect-workers.service` is active with main PID 4136828 since 2026-07-16 13:01:13 CST. Prefect API health succeeded and 14 deployments are registered.

## Interfaces and Dependencies

- Migration: `modules/bus_line_staging/sql/add_batch_versioning.sql` plus the rollout migration command.
- Flow: `modules.bus_line_staging.bus_line_staging_flow`.
- Service: `prefect-workers.service`; Prefect API at `http://127.0.0.1:4200/api`.
- Database connectivity: `mypackage.utilities.connect_to_db` through the configured `.env`/package configuration.
- Remote branch: `origin/session/prefect` unless pre-push inspection proves the service uses another branch.

## Revision Notes

- 2026-07-16: Created for authorized database migration and systemd worker rollout, with historical-fill preservation as a hard gate.
- 2026-07-16: Recorded the applied migration, pushed commits, database reconciliation, worker restart behavior, deployment health, credential exclusion, and final outcome before archival.
