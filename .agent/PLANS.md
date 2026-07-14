# Codex Execution Plans (ExecPlans)

This document defines how Codex should write and maintain execution plans for complex work in this Prefect repository. It is adapted from OpenAI's official guide, [Using PLANS.md for multi-hour problem solving](https://developers.openai.com/cookbook/articles/codex_exec_plans), with repository-specific controls for workflow registration, financial data changes, and production workers.

An ExecPlan is a self-contained, living implementation document. Use it when a task is too large, uncertain, or operationally risky to manage reliably as a short conversational plan. A developer with only the current working tree and the plan must be able to understand the goal, continue the work, and verify the outcome.

## When an ExecPlan is required

Create an ExecPlan for work such as:

- a feature spanning multiple Prefect flows, tasks, shared utilities, and deployment scripts;
- a significant orchestration, scheduling, retry, concurrency, or dependency refactor;
- a database schema change, migration, backfill, or financial-data recomputation;
- a reconciliation or accounting-rule redesign that needs evidence across several periods;
- a production worker, service, or deployment-model change;
- a performance or reliability investigation with multiple experiments;
- any long-running task that must survive a new Codex session.

Do not create an ExecPlan for a focused task edit, a small bug fix, a documentation-only change, or routine maintenance that can be implemented and verified in one short work cycle. Use a normal prompt or Codex Plan mode instead.

## Location and naming

Store each active plan at:

    .agent/plans/YYYY-MM-DD-<short-slug>.md

Use a stable descriptive slug such as `recon-retry-redesign`, `budget-version-backfill`, or `worker-deployment-migration`. One plan should cover one coherent observable outcome.

Never store an ExecPlan in `docs/`. If a completed plan is worth retaining as historical implementation evidence, move it to `.agent/plans/archive/`. Do not create a parallel Spec Kit bundle of `spec.md`, `tasks.md`, `research.md`, and checklists. If the work produces durable business, architecture, database, or operating knowledge, update the appropriate canonical document under `docs/`.

## Core requirements

Every ExecPlan must be:

- **Self-contained:** include repository context, assumptions, paths, commands, data periods, dependencies, and decisions required to continue without chat history.
- **Living:** update it whenever progress, discoveries, validation results, decisions, or remaining work change.
- **Outcome-focused:** explain the workflow, operational, or financial behavior that will be observable after completion.
- **Safe and repeatable:** explain idempotence, transaction boundaries, retry behavior, partial-failure handling, and recovery.
- **Verifiable:** define exact commands, representative flow runs, and database or file assertions that demonstrate success.
- **Plain-language:** define Prefect, accounting, and project-specific terms when first used.

An ExecPlan may document future commits, pushes, deployments, worker restarts, flow triggers, or database writes, but it does not authorize them. Execute those actions only when they are in scope for the user's request and allowed by `AGENTS.md`.

Preserve unrelated user changes in a dirty working tree. Never use destructive Git cleanup to make the tree match the plan.

## Working with the plan

Before implementation, read the relevant modules, deployment scripts, operational documentation, and applicable `AGENTS.md` files. Put enough context into the plan that another contributor can resume from the document alone.

During implementation:

- proceed through safe, in-scope milestones without repeatedly asking for the next step;
- stop for direction when a material business rule, accounting period, database target, production action, destructive operation, or scope expansion requires user choice;
- update `Progress` at every meaningful stopping point;
- record unexpected behavior and evidence in `Surprises & Discoveries`;
- record decisions and rejected alternatives in `Decision Log`;
- keep commands, paths, expected results, deployment registrations, and remaining work accurate;
- revise earlier assumptions when source code or runtime evidence contradicts them.

The plan records intent and evidence. The working tree, Prefect runtime, and verified data outputs remain the sources of truth for implementation state.

## Required structure

Every ExecPlan must contain the following sections. Add other sections only when they improve execution or auditability.

### Title and maintenance statement

Start with a short action-oriented title. State that the document is a living ExecPlan maintained according to `.agent/PLANS.md`.

### Purpose / Big Picture

Explain why the work matters, which flows or users are affected, what becomes possible after completion, and how the result can be observed.

### Progress

Maintain UTC-timestamped checkboxes reflecting the actual state:

    - [x] (2026-07-14 09:00Z) Completed example step.
    - [ ] Remaining example step.
    - [ ] Partially completed step (completed: X; remaining: Y).

Split partially completed work rather than marking an entire milestone complete.

### Surprises & Discoveries

Record unexpected data conditions, Prefect behavior, dependency constraints, performance results, bugs, or environment differences. Include concise evidence such as a flow-run ID, task state, log excerpt, row count, reconciliation result, or command output.

### Decision Log

Record consequential decisions in this form:

    - Decision: What was chosen.
      Rationale: Why this option was selected over the alternatives.
      Date/Author: 2026-07-14 / Codex or contributor name.

### Outcomes & Retrospective

At major milestones and completion, summarize what was achieved, what remains, deviations from the original intent, and lessons that should influence later workflow changes.

### Context and Orientation

Describe the relevant flows, tasks, deployment entry points, databases, tables, schedules, parameters, and external systems as if the reader is new to the repository. Use repository-relative paths and do not rely on chat history.

### Plan of Work

Describe the implementation sequence in prose. For each milestone, identify the files and behavior that change, why the order matters, and what independently verifiable result will exist afterward.

Use prototypes or shadow/parallel calculations when they reduce financial or operational risk. State the evidence required to promote or discard a prototype.

### Concrete Steps

List exact commands, working directories, environment assumptions, accounting periods, and short expected outputs. Keep this section current as commands or paths change.

### Validation and Acceptance

Express acceptance as observable workflow and data behavior. Use the following checks as applicable:

- Activate the project environment with `source venv/bin/activate`.
- Run focused pre-commit checks on changed files, then `pre-commit run --all-files` before completion when practical.
- Add focused tests under `tests/` when task logic can be isolated; otherwise document a safe representative Prefect run.
- For an added, renamed, or removed flow, verify package exports and every applicable deployment script.
- Validate registration with `prefect flow ls` and `prefect deployment ls` in the intended environment.
- Trigger a representative flow with explicit non-production or approved parameters; inspect Flow Runs, Task Runs, and worker logs.
- For database writes, compare pre/post row counts, totals, affected periods, uniqueness, reconciliation balances, and rerun behavior.
- For a production rollout, use systemd-managed services, inspect `systemctl status`, and review `journalctl` output after the authorized restart.

Registration or a successful deployment command alone is not acceptance. Verify the flow's actual result and its data side effects.

### Idempotence and Recovery

Explain which operations are safe to repeat. For delete-and-reload flows, backfills, imports, budget updates, or reconciliation writes, document transaction boundaries, backups or snapshots, concurrency protection, retry rules, partial-failure detection, and rollback or forward-recovery steps.

### Artifacts and Notes

Keep concise evidence needed to verify or continue the work: relevant diffs, flow-run identifiers, test summaries, row-count comparisons, reconciliation totals, schema notes, or log excerpts. Do not copy large generated outputs or sensitive business data into the plan.

### Interfaces and Dependencies

Name the Prefect flows and tasks, deployment names, schedules, parameters, shared utilities, database tables, external systems, configuration, and package dependencies that must exist at completion. Explain new dependencies and compatibility constraints.

### Revision Notes

End the plan with a dated note for each material revision, explaining what changed and why.

## Project-specific constraints

- Run Python and Prefect commands in the repository `venv` when available.
- Keep flow definitions under `modules/<name>/flows/`, task logic under `modules/<name>/tasks/`, and update package exports when public flows change.
- Keep `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py` aligned with the flows appropriate to each environment.
- Production workers are managed by systemd. Do not substitute `pkill`, `nohup`, or an unmanaged long-running deployment script.
- The workers service may pull from Git on restart. An authorized rollout must identify the branch and commit expected on the server before restart.
- Treat accounting period, budget version, database/schema target, and overwrite scope as required plan inputs for financial-data changes.
- Prefer explicit periods over implicit "last month" defaults in high-impact verification and production steps.
- Avoid high-volume per-record logging in Prefect tasks; log stage summaries and actionable exceptions.
- Keep `.env`, credentials, personal data, production row samples, and sensitive financial details out of plans and Git.

## Minimal skeleton

Use this skeleton when starting a plan:

    # <Short, action-oriented title>

    This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

    ## Purpose / Big Picture

    ## Progress

    ## Surprises & Discoveries

    ## Decision Log

    ## Outcomes & Retrospective

    ## Context and Orientation

    ## Plan of Work

    ## Concrete Steps

    ## Validation and Acceptance

    ## Idempotence and Recovery

    ## Artifacts and Notes

    ## Interfaces and Dependencies

    ## Revision Notes

The standard is a plan that a new contributor can execute safely and verify from the current repository plus the plan file alone.
