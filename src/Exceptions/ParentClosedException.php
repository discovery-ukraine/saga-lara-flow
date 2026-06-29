<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Recorded as the cause when a child workflow is forcibly failed because its
 * parent became terminal under the Fail close policy. Distinguishes a parent-driven
 * shutdown from the child's own business failure.
 */
class ParentClosedException extends FlowException
{
    public static function for(FlowRun $parent): self
    {
        return new self(sprintf(
            'Child workflow failed because its parent %s [%s] closed under the Fail policy.',
            $parent->workflow_class,
            $parent->id,
        ));
    }
}
