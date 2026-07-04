---
id: child-workflows
title: Child workflows
sidebar_position: 11
---

# Child workflows

A workflow can start another workflow and await its result. The child inherits the parent's
connection, queue, and **tenant context**:

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
array. `run()` awaits the child and returns its result.

## Close policies

`ChildClosePolicy` decides what happens to the child when the **parent** closes:

- `Abandon` (default) — leave the child running independently.
- `Cancel` — cancel the child.
- `Fail` — fail the child.

The default comes from `children.default_close_policy`, or per class via `#[ChildPolicy]`.

## Handling child failure

A failing child throws `ChildWorkflowFailedException`; a cancelled one throws
`ChildWorkflowCancelledException`:

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

To let the parent proceed regardless of the child's outcome, call `->continueParentOnFailure()` — the
child's failure is then swallowed rather than thrown.
