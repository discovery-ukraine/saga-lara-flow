---
id: synchronous-execution
title: Synchronous execution
sidebar_position: 16
---

# Synchronous execution

`runSync()` drives the entire workflow in-process, using the **same single replay loop** as the
queued path. It is handy for tests, tinkering, and short workflows that don't need to suspend:

```php
$run = SagaFlow::create(CheckoutWorkflow::class)
    ->withArguments('order-42')
    ->runSync();

$run->status;   // FlowStatus::Completed
$run->result;   // the value handle() returned
```

The queued and synchronous paths are guaranteed to reach the **same** final database state — the only
difference is *who* drives the steps (your worker vs. the current process).

:::warning Do not call `runSync()` inside a transaction of your own
A step's body runs while your transaction is still open, so a rollback afterwards — yours, or one a
failed statement forced on you — discards every row the run recorded while the work those rows
describe is already done. A charge stays charged with nothing left to attribute it to, on every
driver. The queued path has no such problem: `queue.after_commit` holds the jobs until your
transaction commits.
:::

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
