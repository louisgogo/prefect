# Add selectable modules to the existing business-line Staging flow

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

The existing deployment `主流程-业务线Staging抽取` always rebuilds all six business-line Staging categories in one serial run. Finance operators need to rerun one or several categories when only a subset of source facts changed or one extraction task needs repair, without producing an incomplete batch or overwriting the batch currently being filled in. After this change, the same existing flow and deployment accept an explicit `modules` parameter containing `expense`, `revenue`, `profit_other`, `inventory`, `receivable`, or `in_transit_inventory`; omitting it or passing an empty list runs all modules.

A module refresh creates a new versioned batch, clones the preceding complete batch including normalized ratios and audit state, removes only the selected category rows from the clone, rebuilds those categories from source facts, inherits prior values for unchanged rebuilt records, and leaves the successful result `READY` when a `FILLING` batch already exists. It never silently changes the batch users are editing. The source implementation and local tests are in scope; registering or triggering the flow against the live Prefect server, writing financial data, activating a batch, and restarting workers are not.

## Progress

- [x] (2026-08-14 03:33Z) Located the historical fact-assignment rollout, current batch lifecycle, six extraction categories, and all deployment entry points.
- [x] (2026-08-14 03:33Z) Chose a clone-and-rebuild batch design that preserves untouched categories and the existing no-silent-activation rule.
- [x] (2026-08-14 03:36Z) Corrected the interface after user clarification: extend the existing main flow instead of adding a separate subflow/deployment.
- [x] (2026-08-14 03:43Z) Added module definitions, selection validation, previous-batch cloning, and selected-category reset helpers.
- [x] (2026-08-14 03:45Z) Refactored combined extraction tasks so selected categories control writes and the unassigned task does not query unselected source facts.
- [x] (2026-08-14 03:46Z) Added the parameter to the existing flow, deployment defaults/descriptions, and durable workflow documentation.
- [x] (2026-08-14 03:48Z) Added focused tests; 40 relevant tests, compilation, focused pre-commit, and `git diff --check` pass.
- [x] (2026-08-14 03:51Z) Ran the final focused pre-commit, compilation, diff, parameter-schema, and 41-test validation set; an all-files hook attempt was reverted for unrelated historical formatting changes and is intentionally not part of this feature.

## Surprises & Discoveries

- The normalized `staging_bus_line_ratio` table has category-specific foreign keys with `ON DELETE CASCADE`, so removing cloned base rows safely removes their cloned ratio rows.
- The existing task boundaries do not match the six user-facing categories: revenue and other profit share one task, unassigned processing spans three categories, and inventory/receivable/in-transit share one task. Those tasks need allowlisted category selectors rather than new duplicate implementations.
- Batch rules intentionally keep a successful rerun `READY` when an editable `FILLING` batch exists. The module refresh must preserve this rule rather than automatically activating its output.
- Prefect converts `list[Literal[...]]` into an array schema whose item enum is exactly `费用、收入、其他损益、存货、应收、在途存货`; this gives the UI a fixed multi-select input instead of a free-text array.
- The Prefect virtual environment has no pytest package. Pytest-only fact-assignment tests run successfully by using the FastAPI pytest interpreter with the Prefect site-packages added to `PYTHONPATH`; unittest-only checks also pass in the native Prefect environment.

## Decision Log

- Decision: Add the `modules` parameter directly to the existing `bus_line_staging_flow` and keep the existing deployment name.
  Rationale: The user explicitly wants configuration on the current main flow, with all modules as the default, rather than another flow entry.
  Date/Author: 2026-08-14 / Codex.
- Decision: Clone the preceding batch before rebuilding selected categories.
  Rationale: A batch visible to the collection application must remain complete across all six categories; a selected-only batch would hide untouched categories if activated.
  Date/Author: 2026-08-14 / Codex.
- Decision: Keep the existing full-flow path additive and unchanged in behavior, and use cloning only for module refreshes.
  Rationale: The monthly main flow already has tested failure cleanup and inheritance behavior; limiting the new clone path reduces regression risk.
  Date/Author: 2026-08-14 / Codex.
- Decision: Do not automatically activate a completed module-refresh batch.
  Rationale: Existing batch architecture requires administrator review before replacing a live filling batch, preventing edits from silently switching versions.
  Date/Author: 2026-08-14 / Codex.

## Outcomes & Retrospective

The existing flow now exposes a six-option multi-select module parameter and defaults to all modules through each deployment registration. A partial selection creates a complete cloned batch, clears only the selected category tables, restores selected fact assignments, and routes only the matching write paths. No live Prefect deployment, database write, batch activation, or worker restart has been performed.

## Context and Orientation

`modules/bus_line_staging/flows/bus_line_staging_flow.py` owns the full monthly extraction. It validates and restores fact-level assignments, creates a `bus_line_staging_batch`, runs expense, revenue/other, unassigned, revenue auto-fill, and asset tasks, inherits prior ratios/audit state, compares the batch, and marks it `READY` or `FILLING`.

`modules/bus_line_staging/batch.py` owns batch states and the six table-to-category mappings. `start_batch()` records `previous_batch_id`; `fail_batch()` deletes only the failed batch; `complete_batch()` refuses to replace an existing `FILLING` batch. `staging_bus_line_ratio` stores ratios by category and `record_id` and cascades when a base Staging row is deleted.

The user-facing modules are: expense -> `staging_bus_expense` / `fact_expense`; revenue -> `staging_bus_revenue` / `fact_revenue`; profit_other -> `staging_bus_profit_bd` / `fact_profit_bd`; inventory -> `staging_bus_inventory` / `fact_inventory`; receivable -> `staging_bus_receivable` / `fact_receivable`; in_transit_inventory -> `staging_bus_in_transit_inventory` / `fact_inventory_on_way`.

Public flow exports live in `modules/bus_line_staging/__init__.py`. Registration must remain aligned in `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`. The canonical operator documentation is `docs/business_line_accounting_process.md`.

## Plan of Work

First introduce an allowlisted module registry and normalization helper. Add a transactional batch clone helper that discovers real table columns, generates new `record_id` values, copies every previous base row and normalized ratio into the new batch, and fails before extraction if no previous batch exists. Add a reset helper that deletes only selected category rows from the cloned batch.

Next make fact validation/restoration accept selected fact tables. Add category selectors to the shared revenue/other, unassigned, and asset tasks; keep their default behavior equivalent to the current full run. Extend the existing flow with `modules`. The partial path will start a batch, clone it, clear selected categories, restore selected direct fact assignments, run only the selected extraction blocks, inherit unchanged ratios/status, compare, and complete the batch. Failure cleanup will still remove the entire new batch.

Then update the existing local/server/production registration parameters and descriptions so Prefect shows all six modules by default under `主流程-业务线Staging抽取`. Update the workflow guide with module names, clone behavior, `READY` status, and the administrator activation boundary.

Finally add unit tests for module validation, clone/reset SQL, selected task routing, full-flow compatibility, failure cleanup, and deployment registration. Run the focused test suite, compile changed Python, run focused pre-commit hooks, and inspect `git diff --check` and the final diff. Do not connect to or mutate the runtime database.

## Concrete Steps

Work in `/root/worktrees/prefect/staging-module-refresh` on branch `feature/staging-module-refresh`, based on `origin/session/prefect` at `66201a1`.

1. Edit only with `apply_patch` and keep the plan current.
2. Run unittest-compatible checks with `/root/prefect/venv/bin/python -m unittest ...`. Run pytest-only checks with `PYTHONPATH=<worktree>:/root/prefect/venv/lib/python3.11/site-packages /root/fastapi/AIPlatform/.venv/bin/python -m pytest ...`.
3. Compile with `/root/prefect/venv/bin/python -m compileall modules/bus_line_staging deploy_local.py deploy_to_server.py deploy_production.py`.
4. Run focused pre-commit hooks on changed files, followed by `/root/prefect/venv/bin/pre-commit run --all-files` when practical.
5. Run `git diff --check`, inspect `git status --short`, and review the final diff.

## Validation and Acceptance

- The existing `bus_line_staging_flow(start_date, end_date, modules)` executes all six categories in the same order when `modules` is omitted or empty and returns the existing summary fields.
- The flow rejects unknown module codes and a partial run with no preceding batch before source rows are written; duplicates are normalized without rerunning a module twice.
- A module refresh clones every unselected table row, `record_id`-linked ratio, and audit state into a new batch, then deletes and rebuilds only selected categories.
- Selecting revenue runs direct fact restoration for `fact_revenue`, normal revenue extraction, revenue unassigned extraction, and revenue ratio auto-fill, but no expense, other-profit, or asset extraction.
- Selecting profit_other runs only the two other-profit extraction paths; selecting each asset module runs only its corresponding table block.
- Failure after cloning marks the new batch failed and removes only its rows; the preceding batch remains unchanged.
- A successful partial refresh completes as `READY` when a monthly `FILLING` batch exists and requires the existing administrator activation operation.
- The existing deployment remains named `主流程-业务线Staging抽取` and exposes all six modules as its default parameter value in applicable deployment scripts.
- Focused tests, compilation, pre-commit, and `git diff --check` pass without a live database or Prefect write.

## Idempotence and Recovery

Each refresh creates a new versioned batch; it never deletes the preceding batch. Clone and selected-category reset occur only inside the new batch. Base-row deletion cascades to normalized ratios. If any task fails, `fail_batch()` removes every row belonging to the new batch and records `FAILED`; rerunning creates another version and starts from the still-intact preceding editable/published batch.

The flow-run identifier keeps `start_batch()` retry-aware. Module codes and table names come only from repository allowlists. The local implementation is safe to repeat. A later live verification must use an explicit accounting month, back up the six Staging tables plus batch and ratio tables, compare pre/post row counts and ratios, and activate the output only after review.

## Artifacts and Notes

The base revision is `66201a157a22f22c54e89e4748af9c9f159c396e`. Prefect parameter-schema inspection reports `modules` as an array whose item enum is `['费用', '收入', '其他损益', '存货', '应收', '在途存货']`. The focused regression set passes 41 tests in 2.12 seconds. Changed-file pre-commit and Python compilation pass. A full-tree pre-commit attempt reformatted unrelated legacy modules; those changes were restored immediately with `apply_patch`, and the feature-only hook set passes.

## Interfaces and Dependencies

- Existing and only flow: `modules.bus_line_staging.bus_line_staging_flow` / `主流程-业务线Staging抽取`.
- New parameter: `modules: list[Literal['费用', '收入', '其他损益', '存货', '应收', '在途存货']] | None`, plus existing `start_date` and `end_date`; deployment defaults select all six, and omitted or empty means all modules.
- Database objects: `bus_line_staging_batch`, six `staging_bus_*` tables, `staging_bus_line_ratio`, six source `fact_*` tables, `dim_org_struc`, `dim_bus_line`, and `fact_bus_wage_rate` for expense prerequisites.
- No database schema change and no new Python dependency are required.

## Revision Notes

- 2026-08-14: Created after locating the existing batch architecture and choosing a clone-and-selective-rebuild design for independently runnable modules.
- 2026-08-14: Revised the public interface after user clarification; the existing main flow now owns the optional module selector and remains the only deployment.
