<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;

/**
 * A stalled rollback that was taken over but could not be planned again. The run stays
 * in Cancelling under its new owner token, and the monitor or the doctor that took it
 * steps over it and tries again on a later pass.
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
