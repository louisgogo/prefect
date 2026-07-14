# Repository Guidelines

## Project Structure & Module Organization

This repository is a Python Prefect 2.x workflow project for business-line accounting, ETL, budget updates, reporting, and reconciliation. Workflow packages live under `modules/`, usually split into `flows/` and `tasks/` subpackages, for example `modules/ai_data_etl/flows/ai_data_etl_flow.py` and `modules/recon/tasks/`. Shared helpers are in `utils/` and `modules/common/`. Deployment entry points are `deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`. Operational scripts are in `scripts/`, service examples are at the repository root, and deployment/process documentation is in `docs/`. Complex-task planning rules live in `.agent/PLANS.md`; durable business process and database documentation belongs in `docs/`, not in agent instruction files.

## Build, Test, and Development Commands

Use the checked-in virtual environment when available:

```bash
source venv/bin/activate
pip install -r requirements.txt
pre-commit install
```

Start a local Prefect API/UI with `prefect server start`. Register flows for local testing with `python deploy_local.py`; deploy to the configured remote/server environment with `python deploy_to_server.py`; use `python deploy_production.py` only for production-style defaults and schedules. Check Prefect state with `prefect config view`, `prefect flow ls`, and `prefect deployment ls`.

## Coding Style & Naming Conventions

Python formatting is enforced by pre-commit: Black with `--line-length=100`, isort using the Black profile, and flake8. Use 4-space indentation, snake_case for modules, functions, variables, and Prefect task helpers, and keep flow files named after their exported flow, such as `budget_update_flow.py`. New flows should be exported through the relevant package `__init__.py` and registered in deployment scripts so they appear in Prefect UI.

## Testing Guidelines

There is no committed pytest/unittest suite in the current tree. For changes, run `pre-commit run --all-files` and perform a targeted Prefect run or deployment check for the affected flow. When adding tests, place them under `tests/`, name files `test_*.py`, and prefer small unit tests for task logic plus one integration-style smoke test for flow orchestration where feasible.

## Commit & Pull Request Guidelines

Recent history uses Conventional Commit prefixes such as `fix(ai_data_etl): ...`, `feat(contract_ocr): ...`, and `chore(contract_ocr): ...`; follow `type(scope): summary` with a concise imperative summary. PRs should describe the workflow impact, list affected modules and deployment scripts, mention required environment variables or database assumptions, link the issue/task, and include Prefect UI screenshots or command output when deployment behavior changes.

## Security & Configuration Tips

Keep secrets and environment-specific settings out of git. Use `.env`, `PREFECT_API_URL`, and documented systemd environment files for server configuration. Be careful with database writes, budget version parameters, and production worker restarts; validate on local or staging Prefect before updating production services.

## Flow Registration & Operational Safety

- Run project commands inside the checked-in `venv` when it is available.
- When adding, renaming, or deleting a flow, update its package exports and every applicable deployment entry point (`deploy_local.py`, `deploy_to_server.py`, and `deploy_production.py`) so Prefect UI registrations match the codebase.
- Local development may use `python deploy_local.py`. Production workers must be managed through the configured systemd services; do not use `pkill`, `nohup`, or an ad hoc long-running `python deploy_to_server.py` process in production.
- The workers service may pull from Git before startup. Before an authorized production restart, confirm the intended commit is pushed to the branch checked out by the service. A restart, deployment, flow trigger, or database write is not authorized merely because it appears in a plan.
- Deployment registration is not sufficient verification. For behavior changes, trigger a representative flow with safe parameters, inspect Prefect Flow/Task Runs and worker logs, and validate the expected database or file outputs.
- Avoid per-row or per-record `print()` calls in large loops. Prefect Server log batching can exceed SQLite parameter limits and make task logs disappear; log exceptions and stage summaries instead.
- Record explicit accounting periods, budget versions, database targets, and retry behavior for any operation that can overwrite or recompute financial data. Do not rely silently on "last month" defaults for high-impact runs.

## Execution Plans

- For complex multi-flow features, significant orchestration refactors, database migrations or backfills, reconciliation redesigns, production rollout changes, or investigations that need multiple independently verifiable milestones, create and maintain an ExecPlan following `.agent/PLANS.md`.
- Store active plans at `.agent/plans/YYYY-MM-DD-<short-slug>.md`; retain completed plans only under `.agent/plans/archive/`. Never store ExecPlan files in `docs/`.
- Use a normal prompt or Codex Plan mode for small, well-scoped fixes, documentation-only edits, and routine maintenance; do not create an ExecPlan for those tasks.
- ExecPlans are living implementation documents, not a second requirements system. The plan files themselves remain under `.agent/`; durable workflow and business documentation belongs in `docs/`. Do not introduce `.specify/`, `specs/`, or mandatory `spec.md`/`tasks.md` bundles unless the user explicitly requests that workflow.
- An ExecPlan does not expand authority: commits, pushes, merges, service restarts, production deployments, Prefect flow triggers, destructive database operations, and external writes still require the authorization implied by the user's request.
