# Make business-line ratio inheritance resilient to source renumbering

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

When selected business-line Staging modules are refreshed, draft ratios should survive if the underlying business rows are unchanged. Current inheritance recognizes rows only by period, `source_no`, and organization level. This hotfix will add a strict business-payload fallback for legacy or already-renumbered rows, with ambiguity checks, so source renumbering cannot silently erase a draft.

## Progress

- [x] (2026-08-17 02:29Z) Confirmed the production failure and created a hotfix worktree from `origin/session/prefect`.
- [x] (2026-08-17 02:34Z) Restored 6,364 affected production draft ratios transactionally; current batch now has ratios on all 6,457 rows and total rate 6,457.
- [x] (2026-08-17 02:49Z) Added regression tests covering renumbered sources and conflicting duplicate payload signatures.
- [x] (2026-08-17 02:49Z) Implemented strict payload fallback after exact source matching, with ambiguity rejection.
- [x] (2026-08-17 02:52Z) Passed 23 focused unit tests, 123 broader unittest cases, 10 pytest-only Fact assignment cases, and all changed-file pre-commit hooks; production SQL validation returned zero remaining fallbacks after repair and planned the DML successfully.
- [x] (2026-08-17 02:53Z) Committed, pushed, and merged production hotfix PR #25 to `session/prefect` as `ca60e35106eb9782de0159b6aab4934acecc5605`.
- [x] (2026-08-17 02:53Z) Updated `/root/prefect`, restarted `prefect-workers`, and verified the worker is active and the business-line Staging deployment is unpaused and `READY`.

## Surprises & Discoveries

- July batch `BLS-202607-001` contained 6,364 filled rows for the affected organization; `BLS-202607-002` contained the same business payload but no ratios because every source number changed.
- The 71 rows in 23 duplicate payload groups all share the same ratio signature, so the current production repair is unambiguous.

## Decision Log

- Decision: Keep exact source-number matching as the primary path and add a full business-payload fingerprint fallback only when the old payload maps to one ratio signature.
  Rationale: This repairs already-renumbered data without a schema change, avoids guessing when identical payloads have conflicting ratios, and remains compatible with non-business-report sources.
  Date/Author: 2026-08-17 / Codex

## Outcomes & Retrospective

The affected production draft is repaired: 6,364 prior ratios were restored, and all 6,457 current rows now have ratios. The defensive inheritance fallback is merged and running in production, with exact source matching still primary and ambiguous payload matches rejected. The Prefect worker is active and the business-line Staging deployment is unpaused and `READY`. No schema change or new dependency was required.

## Context and Orientation

`modules/bus_line_staging/batch.py::inherit_previous_values` copies audit status and normalized rows from `staging_bus_line_ratio` into a new batch. The primary join uses accounting period, `来源编号`, and `唯一层级`. The base Staging tables do not contain `business_report_staging_id`. The companion FastAPI hotfix lives at `/root/worktrees/fastapi/business-report-fact-identity` and prevents future Fact identity changes at their source.

## Plan of Work

First restore the affected current production batch using the previous batch as the durable recovery source, within one transaction and with exact pre/post counts. Then add testable helpers that define identity-excluded payload columns, derive old ratio signatures, reject conflicting duplicate signatures, and insert ratios/status for unmatched new rows. Keep exact matching first. Add batch-level summaries so a future unexpected renumbering is visible rather than silently producing zero inherited rows.

## Concrete Steps

Work in `/root/worktrees/prefect/bus-line-ratio-inheritance` using `venv`. Run targeted tests for batch inheritance and module refresh, then relevant pre-commit checks and `git diff --check`. Publish through a ready PR from `hotfix/bus-line-ratio-inheritance` to `session/prefect`, update `/root/prefect`, restart `prefect-workers`, and verify the Prefect deployment and service status.

## Validation and Acceptance

Exact source matches continue to inherit unchanged. Rows with changed source numbers but identical payloads inherit when the old payload has one ratio signature. Conflicting duplicate payloads cause a clear failure instead of arbitrary assignment. The production target organization must show 6,364 rows with ratios in `BLS-202607-002`, with the prior 6,185/104/66/9 distribution.

## Idempotence and Recovery

The production repair inserts only missing ratio rows for the exact class, period, batch, and organization and checks the expected count before commit. Re-running it is a no-op after success. The previous batch remains untouched and is the recovery source. The code fallback uses `ON CONFLICT` and must be safe to retry within a failed flow transaction.

## Artifacts and Notes

Do not store production row data or credentials. Record aggregate counts, test summaries, PRs, commits, and service verification only.

## Interfaces and Dependencies

The change uses existing `bus_line_staging_batch`, six `staging_bus_*` tables, and `staging_bus_line_ratio`. No schema change or new dependency is required.

## Revision Notes

- 2026-08-17: Created for the production hotfix after confirming source renumbering caused zero inheritance for the affected organization.
- 2026-08-17: Marked complete after PR #25 merged to `session/prefect`, the production worker restarted successfully, and deployment health verification passed; moved to the plan archive.
