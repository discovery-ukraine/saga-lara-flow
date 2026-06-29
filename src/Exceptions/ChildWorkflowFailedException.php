<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Business failure surfaced to the parent when an awaited child workflow is found
 * Failed during replay. Like ActionFailedException it is a legitimate business
 * error (not an internal control signal): by default it triggers the parent's
 * compensation, unless the child was launched with ->continueParentOnFailure().
 */
class ChildWorkflowFailedException extends FlowException
{
    public static function for(FlowRun $child, int $sequence): self
    {
        return new self(sprintf(
            'Child workflow %s [%s] at sequence %d failed.',
            $child->workflow_class,
            $child->id,
            $sequence,
        ));
    }
}
