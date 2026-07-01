---
id: versioning
title: Versioning long-running workflows
sidebar_position: 17
---

# Versioning long-running workflows

A workflow may be suspended for days — waiting on a signal or a slow downstream service — while your
code keeps shipping. Because `handle()` is replayed from the start on resume, changing its logic can
break in-flight runs whose recorded history no longer matches the new code.

## Keep versions in separate classes

The simplest, most explicit strategy is to keep each version in its own class/directory and pin a
version at creation:

```php
namespace App\Workflows\V1;
class CheckoutWorkflow extends Workflow { /* original logic */ }

namespace App\Workflows\V2;
class CheckoutWorkflow extends Workflow { /* new logic */ }
```

```php
SagaFlow::create(\App\Workflows\V2\CheckoutWorkflow::class)
    ->version('v2')
    ->run();
```

Existing runs keep replaying against the exact class they were created with, so they finish on the
logic they started on. New runs use the new version.

## Reading the version inside `handle()`

```php
public function handle(): void
{
    if ($this->version() === 'v2') {
        // v2-only branch
    }
}
```

You can also declare identity with the attribute:

```php
#[Flow(name: 'orders.checkout', version: 'v2')]
class CheckoutWorkflow extends Workflow { /* ... */ }
```

## Guidance

- Prefer additive changes; avoid reordering or removing already-recorded steps in a version that has
  in-flight runs.
- When a change is not replay-compatible, cut a new version class rather than editing the old one.
- Let old runs drain before retiring an old version class.
