# Orchestrate the Kingdee voucher journal with Prefect

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The Kingdee `GL_VOUCHER` journal prototype currently runs as a local FastAPI-repository command. After this work, Prefect will own the operational synchronization. A finance operator can use Quick Run to synchronize the previous completed calendar month, or customize the accounting year and one month or a list of months. Each month appears as an independently retryable task and writes idempotently into `mydb.public.fact_gl_voucher_journal`; `kingdee_gl_voucher_sync_runs` retains source counts, insert/update counts, timestamps, and errors.

FastAPI continues to own the manifest-managed table migrations. The unshipped FastAPI command and service are removed as a clean replacement so there is one production synchronization implementation.

## Progress

- [x] (2026-08-03 05:15Z) Confirmed the local prototype, database schema, pagination, unique source key, and 2026 test import results.
- [x] (2026-08-03 05:15Z) Created isolated Prefect worktree `/root/prefect-kingdee-voucher-sync` on `feature/kingdee-voucher-journal-sync` from `origin/session/prefect`.
- [x] (2026-08-03 05:22Z) Implemented the Kingdee voucher task package and parameterized Prefect flow with sequential month tasks and Hermes lifecycle callbacks.
- [x] (2026-08-03 05:22Z) Exported the flow and registered it in local, server, and production deployment scripts.
- [x] (2026-08-03 05:22Z) Added 10 focused task and flow tests; unittest and focused flake8 checks pass.
- [x] (2026-08-03 05:23Z) Ran the real Prefect flow for 2026 period 8 against `test_mydb`: 30 source rows, 0 inserts, 30 updates, and 30 distinct stored entry IDs.
- [x] (2026-08-03 05:25Z) Focused pre-commit hooks and final whitespace checks pass for every changed Prefect file.
- [x] (2026-08-03 05:41Z) Committed and pushed the Prefect feature, merged PR #3 into `session/prefect`, configured the ignored worker credential, restarted `prefect-workers.service`, and verified the deployment is READY.
- [x] (2026-08-03 05:53Z) Added previous-month Quick Run defaults and empty-page pagination, merged PR #5, restarted the worker, and verified deployment version `9ecef7e1` is READY with defaults `year=2026`, `month=7`, `page_size=5000`.
- [x] (2026-08-03 06:38Z) Released FastAPI schema and tool-panel PR #23 to production, applied the two finance migrations, and backported PR #25 to `develop`.
- [x] (2026-08-03 06:38Z) Synchronized production periods 1 through 7: seven completed month records, 457,120 unique rows, zero null source IDs, and exact per-period source/stored counts.
- [x] (2026-08-03 06:40Z) Converted the monthly task result timestamp to ISO 8601 text and added a regression assertion; all 12 focused tests and changed-file pre-commit hooks pass.
- [ ] Publish a new deployment version and verify a safe idempotent rerun reaches Prefect `Completed`.

## Surprises & Discoveries

- The live Kingdee API returns several documented one-character flags as JSON booleans. The finance schema therefore uses a follow-up migration converting five columns to PostgreSQL `BOOLEAN`.
- The Prefect `.env` currently exposes an FONE proxy token but no dedicated `XGD_TOKEN` or voucher database URL. The flow must prefer `XGD_TOKEN` and `KINGDEE_VOUCHER_DATABASE_URL`, with a compatibility fallback to `AIHUB_FONE_API_TOKEN` and the established `mypackage.utilities.connect_to_db()` production connection.
- The production Prefect worker is systemd-managed and pulls the checked-out `/root/prefect` branch before starting `deploy_to_server.py`. Registration or restart must wait for an authorized pushed commit.
- The first live Prefect run exposed that psycopg2 does not adapt Python `uuid.UUID` values automatically in this environment. Passing the run ID as canonical UUID text fixes the insert without changing the PostgreSQL UUID column, and a regression test now covers this boundary.
- Treating any page shorter than `page_size` as the final page depends on the external API always honoring the requested limit. Continuing until an empty page costs one final request but prevents silent data loss if Kingdee applies a smaller server-side page cap.
- The authorized production backfill committed all seven months successfully, but the completion notification received `max_source_modified_at` as a Python `datetime`. `httpx` could not JSON-encode it, so flow run `7e340323-cbee-4151-912d-47d46f9ba65a` was marked `Failed` after 457,120 rows and all seven month records were already completed.
- Production reconciliation found one July voucher in document status `A` whose raw Kingdee entry amounts are unbalanced; all typed stored amounts match the raw payload. This is a source-side unapproved voucher condition, not a paging, duplication, or normalization defect. Completed/approved voucher reconciliation remains clean when the source draft is excluded.

## Decision Log

- Decision: Put orchestration and data synchronization in a new `modules/kingdee_voucher/` Prefect package and retain only migrations in FastAPI.
  Rationale: ERP extraction is a monitored ETL workflow. A single implementation avoids drift between a local script and Prefect.
  Date/Author: 2026-08-03 / Codex.

- Decision: Flow parameters are `year`, optional `month`, optional `months`, and `page_size`; exactly one of `month` or `months` is required.
  Rationale: A single month is simple in Prefect UI, while a month list supports annual backfills. The deployment supplies the previous calendar month as explicit Quick Run defaults; customized runs must still keep `month` and `months` mutually exclusive.
  Date/Author: 2026-08-03 / Codex.

- Decision: One Prefect task synchronizes one accounting month and is retried independently.
  Rationale: A failed month should not rerun already completed months. Database upsert by `FEntity_FEntryID` makes task retries safe.
  Date/Author: 2026-08-03 / Codex.

- Decision: Serialize `max_source_modified_at` to ISO 8601 text in the monthly task result while retaining the database run-record column as a timestamp.
  Rationale: Prefect/Hermes summaries must be JSON-safe, while PostgreSQL should keep a typed timestamp for querying. Converting at the task-result boundary fixes notifications and flow state without changing stored financial data.
  Date/Author: 2026-08-03 / Codex.

## Outcomes & Retrospective

The implementation, test rollout, Git release, worker configuration, deployment registration, FastAPI schema/tool release, and production January-through-July data load are complete. Seven month records completed with 457,120 unique rows and exact source/stored counts. The remaining defect is confined to the post-load Hermes completion notification: a non-JSON-safe timestamp caused Prefect to mark the otherwise successful aggregate flow as failed. The hotfix converts that result field to ISO text and will be verified through an idempotent closed-month rerun.

## Context and Orientation

Prefect flows live under `modules/<package>/flows/`, task logic under `modules/<package>/tasks/`, and public flows are exported through package `__init__.py` files and root `modules/__init__.py`. New flows must be registered by `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`.

The source endpoint is `POST https://aihub.xgd.com/api/proxy/erp/sdk/BillQuery` with `FormId=GL_VOUCHER`, explicit `FYEAR`/`FPERIOD`, `StartRow`, `Limit`, and stable `FEntity_FEntryID ASC` ordering. The target table is one row per voucher entry, keyed by `source_entry_id`.

Production workers run `/root/prefect/deploy_to_server.py` under `prefect-workers.service`, loading `/root/prefect/.env`. The target database is resolved from `KINGDEE_VOUCHER_DATABASE_URL` when set; otherwise production compatibility uses `mypackage.utilities.connect_to_db()`.

## Plan of Work

Create `modules/kingdee_voucher/tasks/kingdee_voucher_tasks.py` with parameter validation, credential resolution, Kingdee paging, typed normalization, page-level transactions, run-record updates, and reconciliation summaries. Use Prefect task retries for month-level recovery and bounded HTTP retries inside each page request. Avoid per-row logging.

Create `modules/kingdee_voucher/flows/kingdee_voucher_journal_flow.py`. Resolve explicit month selection, notify Hermes at start/completion/failure, and call the monthly task sequentially so the external API and finance database are not overloaded.

Export and register the flow in every deployment script. The deployment is manual-trigger only, with registration-time defaults for the previous completed calendar month. Add tests covering the previous-month and year-boundary defaults, parameter exclusivity, page progression through a short page to an empty page, boolean normalization, idempotent upsert decisions, token redaction, and month task ordering.

Run focused tests and formatting. For integration verification, set `XGD_TOKEN` and a temporary `KINGDEE_VOUCHER_DATABASE_URL` derived from the test environment only for the command, run period 8, and confirm it updates the existing 30 test rows without inserting duplicates.

## Concrete Steps

All commands run in `/root/prefect-kingdee-voucher-sync` using `/root/prefect/venv/bin/python`.

1. Add the new package, flow, tasks, exports, deployment registrations, tests, and this plan.
2. Run focused tests for `tests/test_kingdee_voucher_journal.py`.
3. Run Black/isort/flake8 or focused pre-commit hooks on changed files, then `pre-commit run --all-files` when practical.
4. Run an ephemeral Prefect flow for explicit year 2026, month 8 against `test_mydb`.
5. Verify the latest test run reports 30 source rows, zero inserts, 30 updates, and no duplicate source IDs.
6. After authorized Git release and FastAPI production migration, configure worker secrets, register the deployment, restart the systemd worker, trigger the requested production periods, and validate production counts and balances.
7. Convert the task-result timestamp to ISO 8601 text, add a regression assertion, release the Prefect hotfix, and rerun one already-loaded closed month to prove idempotence and a terminal `Completed` state.

## Validation and Acceptance

Unit tests must pass. The flow schema must expose `year`, `month`, `months`, and `page_size`; the registered deployment must prefill the previous calendar year/month and page size 5000. Package exports and all three deployment scripts must reference the new flow.

The test flow is accepted when period 8 completes through Prefect task orchestration and remains idempotent. Production acceptance requires the FastAPI migrations to be present in `main`, both target tables to exist, the worker to run the intended pushed Prefect commit, every requested month to complete, row uniqueness/reconciliation checks to pass, and the aggregate flow to reach Prefect `Completed` after its Hermes completion notification.

## Idempotence and Recovery

Each page uses `INSERT ... ON CONFLICT (source_entry_id) DO UPDATE` and commits only after the page upsert and run-progress update succeed. Retrying a month begins at page zero but updates existing source IDs instead of duplicating them. Completed months remain committed when a later month fails.

The partial unique index on running year/month prevents concurrent duplicate jobs. Stale running records older than six hours are marked failed before a replacement starts. The flow never logs or stores bearer tokens. It does not physically delete target rows absent from a later source query; hard-deletion reconciliation remains separate from safe upsert.

## Artifacts and Notes

Existing test evidence from the prototype: 456,153 unique 2026 entries, 50,320 vouchers, zero null source IDs, zero voucher header-total mismatches, and zero unbalanced vouchers.

Prefect evidence: flow run `vagabond-lyrebird` completed 2026 period 8 with 30 source rows, zero inserts, 30 updates, one completed page, and 30 distinct stored source IDs. Ten focused unittest cases pass, including parameter exclusivity, ordered month orchestration, page advancement, boolean conversion, UUID adaptation, idempotent upsert counting, and token redaction.

Registration evidence: Prefect PR #3 merged as `67f6e0a2746758725481d8ce56f0cd1ec897896d`; `prefect-workers.service` is active; deployment ID is `d5ec32dc-f45c-4edb-82c5-18802e0db198`, status READY, not paused, with no schedules.

Default-period evidence: Prefect PR #5 merged as `9ecef7e12eb949c1af7ff3d137458d4abd7b57ee`; deployment defaults are `year=2026`, `month=7`, `page_size=5000` on 2026-08-03. Test flow `thistle-shrew` completed period 8 with a 30-row short page followed by an empty page, reporting zero inserts and 30 updates.

Production evidence: run `7e340323-cbee-4151-912d-47d46f9ba65a` completed month tasks for periods 1 through 7 with 457,120 inserted rows and exact per-period stored counts. The flow then failed only in `notify_hermes_task` because `datetime` was not JSON serializable. Identity checks found 457,120 distinct source IDs and zero null IDs. One unapproved July source voucher is unbalanced in the raw Kingdee payload; no typed/raw normalization discrepancy was found.

## Interfaces and Dependencies

The implementation uses existing Prefect, requests, psycopg2, `mypackage`, and Hermes notification dependencies. Required worker configuration is a valid `XGD_TOKEN` and finance database connection through `KINGDEE_VOUCHER_DATABASE_URL` or the established `mypackage` configuration.

The public flow is `kingdee_voucher_journal_flow`, registered as `子流程-金蝶凭证序时簿同步` and tagged for Kingdee, vouchers, monthly tasks, manual trigger, and financial writes.

## Revision Notes

- 2026-08-03: Initial plan created after the user selected Prefect orchestration and explicit month parameters.
- 2026-08-03: Recorded completed implementation, deployment registration, focused test results, the psycopg2 UUID adaptation fix, and the successful period-8 Prefect test run.
- 2026-08-03: Recorded the merged Prefect release, worker credential configuration, systemd restart, and READY manual-trigger deployment evidence.
- 2026-08-03: Added the user-requested previous-month Quick Run behavior and documented empty-page termination as the safer pagination contract.
- 2026-08-03: Recorded merged PR #5, deployment version `9ecef7e1`, verified previous-month defaults, and the successful short-page integration run.
- 2026-08-03: Recorded the production January-through-July load, the post-completion notification serialization defect, the source-side unapproved-voucher reconciliation exception, and the ISO timestamp hotfix plan.
