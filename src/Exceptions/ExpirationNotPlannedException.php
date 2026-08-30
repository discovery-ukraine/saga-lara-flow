<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;

/**
 * An overdue run whose rollback could not be planned: the compensation-only replay
 * threw before anything was written, so the run is exactly where it was found.
 *
 * It exists so the monitor's sweep can tell that failure from every other one by its
 * type rather than by guessing from the run's status afterwards — Cancelling is
 * written by a manual compensate() too, and a run finished by somebody else in the
 * meantime looks like neither. A sweep may step over this one and carry on; anything
 * else happened after the run had already been moved, and belongs to its caller.
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
