# Roll out AIHub proxy authentication for FONE flows

This ExecPlan is a living document maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

Replace the FONE ticket-login path used by the reconciliation and income/expense refresh flows with the approved AIHub Bearer-token proxy, commit only the intended FONE changes, push the exact commit to the worker branch, and restart the systemd-managed Prefect workers so the deployed flows load the new code and token environment.

## Progress

- [x] (2026-07-28 09:21Z) Verified the new token against the exact AIHub proxy path using real content reads and a no-data-write script: HTTP 200, script status 0, no errors.
- [x] (2026-07-28 09:21Z) Updated both FONE flows to remove ticket login, configured the token only in the ignored `.env`, and passed 70 repository tests plus scoped pre-commit checks.
- [x] (2026-07-28 09:24Z) Selectively staged only FONE changes, passed pre-commit, and created commit `853575a` without secrets or unrelated inventory/R&D work.
- [x] (2026-07-28 09:25Z) Pushed `853575a` to `origin/session/prefect` and confirmed the remote branch resolved to the exact commit.
- [x] (2026-07-28 09:29Z) Restarted `prefect-workers.service` through systemd and verified the exact committed deployment set after temporarily stashing and restoring two mixed working-tree files.
- [x] (2026-07-28 09:29Z) Confirmed the service remained active and both FONE deployments were polling without missing-token, import, or startup errors; no financial flow was triggered.
- [x] (2026-07-28 09:30Z) Archived this plan and prepared the completion notification.

## Surprises & Discoveries

- `prefect-workers.service` runs `git pull` before startup, so the intended restart commit must be pushed before restart.
- The worktree contains unrelated inventory, R&D, documentation, and generated-file changes. The FONE commit must use selective staging.
- Full destructive income, expense, and reconciliation flows were not re-triggered after the auth change; validation used the real proxy, real content definitions, and a no-write script.
- The first restart loaded an unrelated uncommitted inventory deployment because `git pull` preserves a dirty worktree. The two mixed startup files were temporarily stashed, the service was restarted from exact commit `853575a`, and the stash was then restored without conflicts.

## Decision Log

- Decision: Store the AIHub token only in `/root/prefect/.env` as `AIHUB_FONE_API_TOKEN` and resolve it inside tasks.
  Rationale: Prefect parameters and committed source would expose the secret in schemas, run metadata, or Git.
  Date/Author: 2026-07-28 / Codex
- Decision: Restart only `prefect-workers.service`, not the Prefect server.
  Rationale: The change affects flow-serving worker processes and their environment; the API server configuration is unchanged.
  Date/Author: 2026-07-28 / Codex

## Outcomes & Retrospective

Completed. Commit `853575a` routes both FONE workflows through the AIHub Bearer-token proxy and is present on `origin/session/prefect`. The systemd-managed worker was restarted from that exact committed startup configuration, remained active, and served both `子流程-从FONE获取往来数据` and `子流程-FONE收入费用明细刷新`. Existing unrelated working-tree changes were preserved. Validation deliberately avoided triggering financial refreshes; it covered 70 automated tests, real proxy content reads, a no-write FONE script, and production deployment startup/readback.

## Context and Orientation

The shared proxy helpers and reconciliation execution task are in `modules/recon/tasks/fone_recon_tasks.py`. Income/expense content loading and execution are in `modules/recon/tasks/fone_income_expense_tasks.py`. Flow orchestration is in `modules/recon/flows/fone_recon_flow.py` and `modules/recon/flows/fone_income_expense_refresh_flow.py`. The systemd service loads `/root/prefect/.env`, runs `git pull`, and starts `python deploy_to_server.py`.

## Plan of Work

Selectively stage the FONE task, flow, export, deployment, documentation, test, service-example, and completed implementation-plan changes, excluding unrelated hunks and files. Review the staged diff for secrets and scope, create a Conventional Commit, push it to the checked-out remote branch, restart the worker service, then inspect systemd status and recent logs. Confirm the new deployment name is visible without triggering the financial scripts.

## Concrete Steps

Completed from `/root/prefect` using the checked-in virtual environment. Selective index patches excluded unrelated deployment hunks. `git diff --cached --check` and pre-commit passed, commit `853575a` was pushed, and systemd restarted the worker. Recent `journalctl` output confirmed both FONE deployments were served from the exact committed registration set.

## Validation and Acceptance

Acceptance requires the exact commit to exist on `origin/session/prefect`, `prefect-workers.service` to be active after restart, no immediate traceback or missing-token errors in worker logs, and both FONE deployments to be served. No financial refresh flow is triggered as part of rollout verification.

## Idempotence and Recovery

Git staging and commit inspection are repeatable. Pushing is an external write and should occur once for the final reviewed commit. Restarting the workers is safe to repeat but interrupts flow-serving processes; do not restart the Prefect server. If startup fails, inspect `journalctl`, correct the committed code or `.env`, push any required fix, and restart the same systemd service. Do not fall back to `nohup`, `pkill`, or unmanaged workers.

## Artifacts and Notes

- Proxy verification: content reads for both income and expense returned HTTP 200; a no-write script returned FONE status 0.
- Automated validation: 70 tests passed; scoped pre-commit, compile, import, and secret-location checks passed.
- The production token exists only in ignored `.env` and must not appear in the staged diff or commit.
- Production rollout: `prefect-workers.service` active since 2026-07-28 17:28:55 CST; both FONE deployment runners reported `being served and polling for scheduled runs`.

## Interfaces and Dependencies

Required environment: `AIHUB_FONE_API_TOKEN` in `/root/prefect/.env`. Proxy base URL defaults to `https://aihub.xgd.com/api/proxy/fone`. Required service: `prefect-workers.service`. Required branch: `session/prefect` tracking `origin/session/prefect`.

## Revision Notes

- 2026-07-28: Created before selective commit and production worker restart, recording the push-before-restart requirement and dirty-worktree scope controls.
- 2026-07-28: Completed and archived after exact-commit push, clean systemd restart, deployment log verification, and restoration of unrelated working-tree changes.
