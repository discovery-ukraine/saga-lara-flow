<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * The cause of a child failed by its parent's close under the Fail policy.
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

    public static function forPruned(string $parentId): self
    {
        return new self(sprintf(
            'Child workflow failed because its parent [%s] closed under the Fail policy and has since been pruned.',
            $parentId,
        ));
    }
}
