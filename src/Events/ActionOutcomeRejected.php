<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use Throwable;

/**
 * Dispatched when a step finished but its row had moved on, so the outcome was
 * refused and nothing was stored. The work happened; this carries the only copy of
 * what it produced. `$outcome` names which outcome it was: ActionCompleted fills
 * `$result` in the form the row would have stored, so a queued listener can carry
 * it; ActionFailed fills `$exception`, which is not rethrown either, making this
 * its only notice. The model reads as the row did before the refused write, so
 * nothing here is safe to save back.
 */
final readonly class ActionOutcomeRejected
{
    public function __construct(
        public ActionRun $actionRun,
        public FlowEventType $outcome,
        public mixed $result = null,
        public ?Throwable $exception = null,
    ) {}
}
