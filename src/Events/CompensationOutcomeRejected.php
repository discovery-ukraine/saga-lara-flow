<?php

namespace DiscoveryUkraine\SagaLaraFlow\Events;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use Throwable;

/**
 * The compensation counterpart of ActionOutcomeRejected: the undo ran, its row had
 * moved on, and what it produced was refused. `$outcome` names which outcome it was
 * — CompensationCompleted fills `$result` in the form the row would have stored,
 * CompensationFailed fills `$exception`. The model reads as the row did before the
 * refused write.
 */
final readonly class CompensationOutcomeRejected
{
    public function __construct(
        public CompensationRun $compensationRun,
        public FlowEventType $outcome,
        public mixed $result = null,
        public ?Throwable $exception = null,
    ) {}
}
