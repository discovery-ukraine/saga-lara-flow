<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Business failure surfaced to the parent when an awaited child workflow is found
 * Cancelled during replay (typically an external cancellation, not one driven by
 * the parent's own close policy). The parent cannot obtain a result, so the wait
 * resolves as an error regardless of ->continueParentOnFailure().
 */
class ChildWorkflowCancelledException extends FlowException
{
    public static function for(FlowRun $child, int $sequence): self
    {
        return new self(sprintf(
            'Child workflow %s [%s] at sequence %d was cancelled.',
            $child->workflow_class,
            $child->id,
            $sequence,
        ));
    }
}
