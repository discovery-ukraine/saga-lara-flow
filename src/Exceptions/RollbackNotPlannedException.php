<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;

/**
 * A stalled rollback taken over but not planned again; the run stays in Cancelling under its new
 * owner.
 */
class RollbackNotPlannedException extends FlowException
{
    public static function for(FlowRun $flowRun, Throwable $cause): self
    {
        return new self(
            sprintf(
                'Stalled rollback of flow %s [%s] could not be planned: %s',
                $flowRun->workflow_class,
                $flowRun->id,
                $cause->getMessage(),
            ),
            previous: $cause,
        );
    }
}
