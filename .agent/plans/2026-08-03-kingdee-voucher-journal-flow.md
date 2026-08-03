# Orchestrate the Kingdee voucher journal with Prefect

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The Kingdee `GL_VOUCHER` journal prototype currently runs as a local FastAPI-repository command. After this work, Prefect will own the operational synchronization. A finance operator can open Prefect UI, enter an explicit accounting year and either one month or a list of months, and run a monitored, retryable workflow. Each month appears as an independently retryable task and writes idempotently into `mydb.public.fact_gl_voucher_journal`; `kingdee_gl_voucher_sync_runs` retains source counts, insert/update counts, timestamps, and errors.

FastAPI continues to own the manifest-managed table migrations. The unshipped FastAPI command and service are removed as a clean replacement so there is one production synchronization implementation.

## Progress

- [x] (2026-08-03 05:15Z) Confirmed the local prototype, database schema, pagination, unique source key, and 2026 test import results.
- [x] (2026-08-03 05:15Z) Created isolated Prefect worktree `/root/prefect-kingdee-voucher-sync` on `feature/kingdee-voucher-journal-sync` from `origin/session/prefect`.
- [x] (2026-08-03 05:22Z) Implemented the Kingdee voucher task package and parameterized Prefect flow with sequential month tasks and Hermes lifecycle callbacks.
- [x] (2026-08-03 05:22Z) Exported the flow and registered it in local, server, and production deployment scripts.
- [x] (2026-08-03 05:22Z) Added 10 focused task and flow tests; unittest and focused flake8 checks pass.
- [x] (2026-08-03 05:23Z) Ran the real Prefect flow for 2026 period 8 against `test_mydb`: 30 source rows, 0 inserts, 30 updates, and 30 distinct stored entry IDs.
- [x] (2026-08-03 05:25Z) Focused pre-commit hooks and final whitespace checks pass for every changed Prefect file.
- [ ] Coordinate Git release, worker environment variables, deployment registration, and production trigger with the FastAPI migration release.

## Surprises & Discoveries

- The live Kingdee API returns several documented one-character flags as JSON booleans. The finance schema therefore uses a follow-up migration converting five columns to PostgreSQL `BOOLEAN`.
- The Prefect `.env` currently exposes an FONE proxy token but no dedicated `XGD_TOKEN` or voucher database URL. The flow must prefer `XGD_TOKEN` and `KINGDEE_VOUCHER_DATABASE_URL`, with a compatibility fallback to `AIHUB_FONE_API_TOKEN` and the established `mypackage.utilities.connect_to_db()` production connection.
- The production Prefect worker is systemd-managed and pulls the checked-out `/root/prefect` branch before starting `deploy_to_server.py`. Registration or restart must wait for an authorized pushed commit.
- The first live Prefect run exposed that psycopg2 does not adapt Python `uuid.UUID` values automatically in this environment. Passing the run ID as canonical UUID text fixes the insert without changing the PostgreSQL UUID column, and a regression test now covers this boundary.

## Decision Log

- Decision: Put orchestration and data synchronization in a new `modules/kingdee_voucher/` Prefect package and retain only migrations in FastAPI.
  Rationale: ERP extraction is a monitored ETL workflow. A single implementation avoids drift between a local script and Prefect.
  Date/Author: 2026-08-03 / Codex.

- Decision: Flow parameters are `year`, optional `month`, optional `months`, and `page_size`; exactly one of `month` or `months` is required.
  Rationale: A single month is simple in Prefect UI, while a month list supports annual backfills. Explicit periods prevent accidental implicit financial writes.
  Date/Author: 2026-08-03 / Codex.

- Decision: One Prefect task synchronizes one accounting month and is retried independently.
  Rationale: A failed month should not rerun already completed months. Database upsert by `FEntity_FEntryID` makes task retries safe.
  Date/Author: 2026-08-03 / Codex.

## Outcomes & Retrospective

The Prefect implementation and test rollout are complete. Flow run `vagabond-lyrebird` synchronized 2026 period 8 through real Prefect orchestration and proved idempotence with 30 updates and no inserts or duplicates. The FastAPI worktree now retains only manifest-managed schema migrations. Git release, production worker configuration, deployment registration, and production data synchronization remain intentionally pending authorization and migration sequencing.

## Context and Orientation

Prefect flows live under `modules/<package>/flows/`, task logic under `modules/<package>/tasks/`, and public flows are exported through package `__init__.py` files and root `modules/__init__.py`. New flows must be registered by `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`.

The source endpoint is `POST https://aihub.xgd.com/api/proxy/erp/sdk/BillQuery` with `FormId=GL_VOUCHER`, explicit `FYEAR`/`FPERIOD`, `StartRow`, `Limit`, and stable `FEntity_FEntryID ASC` ordering. The target table is one row per voucher entry, keyed by `source_entry_id`.

Production workers run `/root/prefect/deploy_to_server.py` under `prefect-workers.service`, loading `/root/prefect/.env`. The target database is resolved from `KINGDEE_VOUCHER_DATABASE_URL` when set; otherwise production compatibility uses `mypackage.utilities.connect_to_db()`.

## Plan of Work

Create `modules/kingdee_voucher/tasks/kingdee_voucher_tasks.py` with parameter validation, credential resolution, Kingdee paging, typed normalization, page-level transactions, run-record updates, and reconciliation summaries. Use Prefect task retries for month-level recovery and bounded HTTP retries inside each page request. Avoid per-row logging.

Create `modules/kingdee_voucher/flows/kingdee_voucher_journal_flow.py`. Resolve explicit month selection, notify Hermes at start/completion/failure, and call the monthly task sequentially so the external API and finance database are not overloaded.

Export and register the flow in every deployment script. The deployment is manual-trigger only and has no hidden last-month default. Add tests covering parameter exclusivity, page progression, boolean normalization, idempotent upsert decisions, token redaction, and month task ordering.

Run focused tests and formatting. For integration verification, set `XGD_TOKEN` and a temporary `KINGDEE_VOUCHER_DATABASE_URL` derived from the test environment only for the command, run period 8, and confirm it updates the existing 30 test rows without inserting duplicates.

## Concrete Steps

All commands run in `/root/prefect-kingdee-voucher-sync` using `/root/prefect/venv/bin/python`.

1. Add the new package, flow, tasks, exports, deployment registrations, tests, and this plan.
2. Run focused tests for `tests/test_kingdee_voucher_journal.py`.
3. Run Black/isort/flake8 or focused pre-commit hooks on changed files, then `pre-commit run --all-files` when practical.
4. Run an ephemeral Prefect flow for explicit year 2026, month 8 against `test_mydb`.
5. Verify the latest test run reports 30 source rows, zero inserts, 30 updates, and no duplicate source IDs.
6. After authorized Git release and FastAPI production migration, configure worker secrets, register the deployment, restart the systemd worker, trigger the requested production periods, and validate production counts and balances.

## Validation and Acceptance

Unit tests must pass. The flow schema must expose explicit `year`, `month`, `months`, and `page_size` parameters. Package exports and all three deployment scripts must reference the new flow.

The test flow is accepted when period 8 completes through Prefect task orchestration and remains idempotent. Production acceptance requires the FastAPI migrations to be present in `main`, both target tables to exist, the worker to run the intended pushed Prefect commit, every requested month to complete, and row uniqueness/reconciliation checks to pass.

## Idempotence and Recovery

Each page uses `INSERT ... ON CONFLICT (source_entry_id) DO UPDATE` and commits only after the page upsert and run-progress update succeed. Retrying a month begins at page zero but updates existing source IDs instead of duplicating them. Completed months remain committed when a later month fails.

The partial unique index on running year/month prevents concurrent duplicate jobs. Stale running records older than six hours are marked failed before a replacement starts. The flow never logs or stores bearer tokens. It does not physically delete target rows absent from a later source query; hard-deletion reconciliation remains separate from safe upsert.

## Artifacts and Notes

Existing test evidence from the prototype: 456,153 unique 2026 entries, 50,320 vouchers, zero null source IDs, zero voucher header-total mismatches, and zero unbalanced vouchers.

Prefect evidence: flow run `vagabond-lyrebird` completed 2026 period 8 with 30 source rows, zero inserts, 30 updates, one completed page, and 30 distinct stored source IDs. Ten focused unittest cases pass, including parameter exclusivity, ordered month orchestration, page advancement, boolean conversion, UUID adaptation, idempotent upsert counting, and token redaction.

## Interfaces and Dependencies

The implementation uses existing Prefect, requests, psycopg2, `mypackage`, and Hermes notification dependencies. Required worker configuration is a valid `XGD_TOKEN` and finance database connection through `KINGDEE_VOUCHER_DATABASE_URL` or the established `mypackage` configuration.

The public flow is `kingdee_voucher_journal_flow`, registered as `子流程-金蝶凭证序时簿同步` and tagged for Kingdee, vouchers, monthly tasks, manual trigger, and financial writes.

## Revision Notes

- 2026-08-03: Initial plan created after the user selected Prefect orchestration and explicit month parameters.
- 2026-08-03: Recorded completed implementation, deployment registration, focused test results, the psycopg2 UUID adaptation fix, and the successful period-8 Prefect test run.
