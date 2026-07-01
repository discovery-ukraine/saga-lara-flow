<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Marks an expiration enforced by the monitor. Carried as the primary cause
 * when an expired run rolls back (its normalized array lands in flow_runs.exception),
 * and surfaced to the workflow on replay when an action step is found Expired — a
 * business error that, left uncaught, fails the flow and triggers compensation.
 */
class FlowExpiredException extends FlowException
{
    public static function forFlowRun(FlowRun $flowRun): self
    {
        return new self(sprintf(
            'Flow %s [%s] expired.',
            $flowRun->workflow_class,
            $flowRun->id,
        ));
    }

    public static function forAction(string $actionClass, int $sequence): self
    {
        return new self(sprintf(
            'Action %s at sequence %d expired.',
            $actionClass,
            $sequence,
        ));
    }
}
