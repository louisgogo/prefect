# Restore fact business-line assignments into Staging

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The monthly deployment `主流程-业务线Staging抽取` must treat an explicit business-line assignment stored on a source fact row as authoritative. After this change, a fact row carrying `business_line_ratios` will create its normal Staging base row and matching `staging_bus_line_ratio` rows, so the collection page immediately displays the fact assignment. `fact_expense.dist_bus_line` remains a legacy single-line source and becomes a 100% ratio when JSON is absent. Conflicting JSON and single-line values stop the batch with the fact table and source number in the error.

## Progress

- [x] (2026-08-13 10:30Z) Inspected the Staging flow, normalized ratio storage, batch inheritance, six source tasks, downstream calculation, and the FastAPI fact-ratio persistence branch.
- [x] (2026-08-13 11:15Z) Implemented common fact-assignment parsing, active-line validation, conflict messages, and preflight checks.
- [x] (2026-08-13 11:45Z) Routed explicit fact rows through a dedicated six-table restore task and prevented legacy extraction, income auto-fill, and prior-batch inheritance from overwriting them.
- [x] (2026-08-13 12:05Z) Added focused tests and completed local compilation, diff, and targeted regression validation.
- [x] (2026-08-13 07:30Z) Confirmed the paired FastAPI fact columns are present in test and production, both environments have zero pending manifest migrations, schema drift is zero, and map translation coverage is complete.
- [ ] Publish the Prefect topic branch through a merge-commit PR to `session/prefect`, restart the systemd-managed workers, and confirm the deployment is registered from the merged commit.
- [ ] Run the deployment for an explicit accounting period only after confirming the period has no active filling batch that would be disrupted; record the flow-run and database verification evidence.

## Surprises & Discoveries

- The page does not read business-line columns from the Staging base tables. It hydrates display columns from `staging_bus_line_ratio`, keyed by the generated Staging `record_id`.
- The current income auto-fill task still updates legacy wide-table columns, so it must be moved to the normalized ratio table.
- In production June 2026 data, 24 `fact_expense` rows with `dist_bus_line` also match the administrative/human-resources allocation rules. Explicit rows must therefore be removed before those rules run or they can be allocated twice.
- Current-month `source_no + unique_lvl` keys are unique in all six fact tables inspected, but implementation still reports the source number in every validation failure instead of depending silently on that observation.
- A generic Staging-to-fact JSON writeback is unsafe: administrative and human-resources allocations can create multiple second-level organization rows for one fact `source_no`, while fact JSON has no organization key. The existing business-report publication/change paths remain the authoritative fact JSON writers; `sync_staging_data` continues to write only `fact_bus_line`.

## Decision Log

- Decision: Use precedence `business_line_ratios`, then legacy single-line assignment, then existing automatic rules, then prior-batch inheritance.
  Rationale: Fact data is the newest authoritative declaration; system inference and historical values are fallbacks only.
  Date/Author: 2026-08-13 / Codex.
- Decision: When JSON and the single-line value both exist, accept only the exact equivalent `{single_line: 1}`; otherwise fail the entire batch.
  Rationale: Silently choosing either representation can misstate financial attribution.
  Date/Author: 2026-08-13 / Codex.
- Decision: Persist restored values only in `staging_bus_line_ratio`; do not add another Staging JSON column.
  Rationale: This is the normalized store already consumed by the FastAPI collection page and downstream upload.
  Date/Author: 2026-08-13 / Codex.
- Decision: Only currently active dimension business lines are valid for a new fact assignment; `无` and `抵销数` are not explicit assignments.
  Rationale: Inactive lines are hidden from the current filling interface and would otherwise create ratios users cannot see or correct.
  Date/Author: 2026-08-13 / Codex.

## Outcomes & Retrospective

The local implementation is complete. Explicit fact assignments are preflighted before batch creation, restored into normalized Staging ratios, and isolated from legacy inference. The paired FastAPI schema and application are already live in test and production. Prefect publication and representative runtime verification remain in progress.

## Context and Orientation

`modules/bus_line_staging/flows/bus_line_staging_flow.py` orchestrates expense, revenue/other, unassigned, income auto-fill, and asset extraction. Each source task creates a Staging base row through `modules/bus_line_staging/utils.py::insert_to_staging_table`, which converts temporary in-memory business-line columns into `staging_bus_line_ratio`. `modules/bus_line_staging/batch.py::inherit_previous_values` then copies ratios from the preceding batch. The six source facts are `fact_revenue`, `fact_expense`, `fact_profit_bd`, `fact_receivable`, `fact_inventory`, and `fact_inventory_on_way`.

The paired FastAPI feature branch stores business-report ratios as `business_line_ratios JSONB` on those six fact tables. Expense additionally has legacy `dist_bus_line`. The Staging workflow must support both representations until all sources use JSON.

## Plan of Work

First add a shared parser that accepts PostgreSQL JSON objects or serialized JSON, validates active dimension keys and positive ratios summing to one, validates the legacy single-line value, and annotates a DataFrame with normalized ratio columns plus an internal authoritative-assignment marker. Add an early Prefect task that checks all six source tables before any Staging rows are written, so conflicts fail with one actionable message.

Then update each extraction task. A dedicated restore task inserts explicit rows before legacy extraction; each legacy query excludes non-empty fact JSON, and expense also excludes legacy `dist_bus_line`. The unassigned fallback task therefore cannot duplicate explicit rows. Income auto-fill and previous-batch inheritance write only records without any current ratio.

Finally add unit tests for JSON parsing, direct assignment, conflict messages, invalid or inactive lines, ratio totals, and direct-or-legacy row selection. Run the focused tests, compilation, formatting checks, and `git diff --check` without connecting to or writing any runtime database.

## Concrete Steps

Work in `/root/worktrees/prefect/fact-business-line-staging` using `venv/bin/python`.

1. Patch common configuration and fact-assignment helpers.
2. Patch the flow and six extraction paths.
3. Patch normalized income auto-fill and batch inheritance guards.
4. Add tests under `tests/` and run `venv/bin/python -m pytest` for the new tests.
5. Run compile checks, focused tests with the shared FastAPI pytest plus Prefect site packages, and `git diff --check`.

## Validation and Acceptance

Acceptance requires tests showing that `{"国际业务": 1}` produces an authoritative ratio, `dist_bus_line="国际业务"` produces the same result when JSON is empty, and the combination `dist_bus_line="国际业务"` plus `{"国内硬件": 1}` raises an error naming the fact table and source number. Multi-line JSON must be preserved after the existing two-decimal Staging normalization. Invalid JSON, inactive/unknown lines, non-positive ratios, and totals other than one must fail.

Validation passed: 30 focused Prefect tests cover the new parser plus existing batch, ratio-storage, expense-source, and in-transit behavior. Core modules compile, and `git diff --check` passes. Code inspection shows direct expense rows are excluded from administrative/headcount and wage-rate allocation, direct asset rows bypass legacy organization-line eligibility filters, income auto-fill inserts into `staging_bus_line_ratio`, and prior-batch inheritance uses `NOT EXISTS` against current ratios.

## Idempotence and Recovery

The code change is additive and safe to rerun locally. Runtime extraction remains batch-versioned: a failed preflight marks no completed Staging batch, and no prior batch is overwritten. The income auto-fill uses conflict-safe inserts and selects only ratio-empty records. Database migration and representative Prefect execution remain future test-delivery steps requiring the repository's deployment authorization and table backups.

## Artifacts and Notes

The implementation depends on FastAPI feature commit `a0bd343`, merged as `824be72`. On 2026-08-13 both test and production were verified to contain all six JSONB columns with no pending manifest migration; schema drift and map-translation coverage checks passed.

## Interfaces and Dependencies

No new package dependency is required. The parser uses Python `json`, `math`, pandas, PostgreSQL JSONB values returned by psycopg2, `dim_bus_line.status`, and the existing normalized `staging_bus_line_ratio` schema.

## Revision Notes

- 2026-08-13: Created after the user approved authoritative fact assignments and explicit conflict failures.
- 2026-08-13: Completed the local implementation and recorded the reason generic Staging-to-fact JSON writeback was intentionally excluded.
- 2026-08-13: Updated the rollout state after production authorization and confirmed the FastAPI prerequisite is already deployed in both environments.
