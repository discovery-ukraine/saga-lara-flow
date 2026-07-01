---
id: artisan-commands
title: Artisan commands
sidebar_position: 16
---

# Artisan commands

The package ships a set of CLI commands for inspecting and operating flows. There are **no HTTP
routes** — everything is done through Artisan or the `SagaFlow` facade.

| Command | Purpose |
| --- | --- |
| `saga-flow:list {--status=} {--tag=} {--workflow=} {--limit=50}` | List runs, newest first, with filters. |
| `saga-flow:show {run} {--compact}` | Inspect a run: header, actions, signals, compensations, history. |
| `saga-flow:signal {run} {name} {--payload=}` | Deliver a JSON-payload signal and wake the run. |
| `saga-flow:cancel {run} {--compensate}` | Cancel a non-terminal run; `--compensate` rolls back first. |
| `saga-flow:kick {run}` | Manually re-drive a stuck run. |
| `saga-flow:monitor` | Expire overdue runs/actions and time out waits. |
| `saga-flow:repair` | Recover runs whose progress was lost to a dropped job. |
| `saga-flow:prune {--days=} {--before=} {--dry-run}` | Delete old terminal runs and related rows. |
| `make:workflow {name}` | Generate a workflow class in `App\Workflows`. |
| `make:action {name}` | Generate an action class in `App\Actions`. |

## Examples

```bash
# List waiting runs for one workflow, tagged for a tenant
php artisan saga-flow:list --status=waiting --workflow="App\\Workflows\\CheckoutWorkflow" --tag=tenant=acme

# Inspect a run
php artisan saga-flow:show 01JABCDEF...

# Approve a run waiting on a signal
php artisan saga-flow:signal 01JABCDEF... approval --payload='{"approved":true}'

# Cancel and roll back
php artisan saga-flow:cancel 01JABCDEF... --compensate

# Re-drive a stuck run
php artisan saga-flow:kick 01JABCDEF...
```

Schedule `saga-flow:monitor` and `saga-flow:repair` for background maintenance — see
[Expiration & monitoring](./expiration-and-monitoring.md).
