<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;

/**
 * The rollback of an overdue run could not be planned; the run keeps the status it was found in.
 */
class ExpirationNotPlannedException extends FlowException
{
    public static function for(FlowRun $flowRun, Throwable $cause): self
    {
        return new self(
            sprintf(
                'Rollback for expiring flow %s [%s] could not be planned: %s',
                $flowRun->workflow_class,
                $flowRun->id,
                $cause->getMessage(),
            ),
            previous: $cause,
        );
    }
}
