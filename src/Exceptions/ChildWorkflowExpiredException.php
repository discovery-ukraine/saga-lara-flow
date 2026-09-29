<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Business failure surfaced to the parent when an awaited child workflow is found
 * Expired during replay. The child ran out of time rather than being stopped, so the
 * parent survives it under ->continueParentOnFailure(), as it does a failed child.
 */
class ChildWorkflowExpiredException extends FlowException
{
    public static function for(FlowRun $child, int $sequence): self
    {
        return new self(sprintf(
            'Child workflow %s [%s] at sequence %d expired.',
            $child->workflow_class,
            $child->id,
            $sequence,
        ));
    }
}
