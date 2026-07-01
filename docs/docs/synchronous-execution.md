---
id: synchronous-execution
title: Synchronous execution
sidebar_position: 15
---

# Synchronous execution

`runSync()` drives the entire workflow in-process, using the **same single replay loop** as the
queued path. It is handy for tests, tinkering, and short workflows that don't need to suspend:

```php
$run = SagaFlow::create(CheckoutWorkflow::class)
    ->withArguments('order-42')
    ->runSync();

$run->status;   // FlowStatus::Completed
$run->result;   // the array handle() returned
```

The queued and synchronous paths are guaranteed to reach the **same** final database state — the only
difference is *who* drives the steps (your worker vs. the current process).

## When to use which

| | `run()` (queued) | `runSync()` |
| --- | --- | --- |
| Returns | a `Pending` `FlowRun` immediately | a settled `FlowRun` |
| Drives steps | on your queue workers | in the calling process |
| Suspend/resume | yes (signals, queued actions) | resolved inline where possible |
| Good for | production, long-running flows | tests, short flows, exploration |

:::note
A workflow that awaits a signal cannot make progress under `runSync()` past the wait unless the
signal was already delivered — synchronous execution has no worker to resume it later. Model
long-running, human-in-the-loop flows with the queued path.
:::
