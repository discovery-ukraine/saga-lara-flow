---
id: child-workflows
title: Child workflows
sidebar_position: 12
---

# Child workflows

A workflow can start another workflow and await its result. The child inherits the parent's
**tenant context**; everything else comes from the [child's own class](#a-childs-own-class):

```php
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;

public function handle(): array
{
    $shipment = $this->child(ShipmentWorkflow::class, ['order-42'])
        ->closePolicy(ChildClosePolicy::Cancel)
        ->run();

    return ['shipment' => $shipment];
}
```

`child(string $workflowClass, array $arguments = [])` takes the child's `handle()` arguments as an
array. `run()` awaits the child and returns whatever the child's `handle()` returned, scalars
included.

It is also the only seam that runs another workflow from inside `handle()`. `SagaFlow::create(...)`
there takes no ordinal, so the run it starts is recognized by nothing and the next replay starts
another one. A step is free to start a run of its own — an action's body is recorded, so it executes
once however the workflow is replayed — but `handle()` itself reaches another workflow through
`child()`.

## A child's own class

A child is created from its class the way a root run is. Its `#[Tag]`s are written with it, in the
same transaction as the run and its link. `#[Flow]` names and versions it. Its deadline is
`#[FlowTimeout]`, else `monitor.expiration.defaults.run`. None of these come from the parent: a
child with no `#[Flow]` has no name or version, and the parent's deadline is not the child's.

`->expiresAt()` on the child builder sets a deadline over both the class and the default:

```php
$this->child(ShipmentWorkflow::class, ['order-42'])
    ->expiresAt(now()->addHour())
    ->run();
```

`#[FlowQueue]` resolves one field at a time, with the parent where config stands for a root run: the
class's connection or queue, else the parent's, else the configured one. `#[FlowQueue(queue:
'heavy')]` under a parent on `redis` sends the child to `heavy` on `redis`, and a worker has to
listen there. The job that closes a child under the parent's [close policy](#close-policies) runs on
the parent's connection and queue, and so does the child's rollback, which that job runs inline.

All of it is read once, when the child starts. A replay of the parent that reaches the child again
resolves the run on record and does not consult the builder or the class.

## Close policies

`ChildClosePolicy` decides what happens to the child when the **parent** closes:

- `Abandon` (default) — leave the child running independently.
- `Cancel` — cancel the child.
- `Fail` — fail the child.

A child closed with `Cancel` rolls back its own completed steps first, whatever ended the parent —
a failure, an expiry, or a rollback — unless the parent was cancelled with `cancel()`, which skips
compensation and so closes its children without it too. `Fail` always rolls the child back.

The default comes from `children.default_close_policy`, or per class via `#[ChildPolicy]`.

## Handling child failure

A failing child throws `ChildWorkflowFailedException`, one that ran out of time throws
`ChildWorkflowExpiredException`, and a cancelled one throws `ChildWorkflowCancelledException`:

```php
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowFailedException;

try {
    $this->child(ShipmentWorkflow::class, ['order-42'])->run();
} catch (ChildWorkflowFailedException $e) {
    // compensate or branch
}
```

Like an action failure, this surfaces on the parent's **replay** pass (the child runs as its own
run), not the instant the child fails — a `try/catch` around `->run()` catches it when the parent is
re-driven. It is for **local** branching; for cross-cutting failure reporting prefer the
[`FlowFailed`](./events.md) event, and if you report from inside `handle()`, re-throw so the parent
still fails and compensates.

To let the parent proceed past a child that failed or expired, call `->continueParentOnFailure()` —
`run()` then returns `null` rather than throwing. A cancelled child still throws: a cancellation is
an explicit act, and the parent has no result to carry on with.

A parent that carries on this way keeps collecting compensations, and a later rollback runs them: a
child that has already finished is history the plan reads, not a frontier it stops at. A child still
in flight is a frontier, so a rollback planned while one runs covers only the steps before it — the
child rolls itself back through its own [close policy](#close-policies) instead.

## A parent that is rolling back

No child starts under a parent that is rolling back. The seam reads the parent's status from the
connection that wrote it and ends the pass instead: a rollback plans the stack it will undo once, so
a child started afterwards would run to completion outside that plan, and under the default
`Abandon` policy nothing closes it. The child's run and its link are written together, so an ordinal
that does not finish leaves behind no run without an owner. The `ChildWorkflowStarted` event and the
child's own job both follow that commit and only once it is read back, so a listener never sees a
child the write did not keep.
