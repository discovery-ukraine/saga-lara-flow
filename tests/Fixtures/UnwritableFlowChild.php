<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use RuntimeException;

/**
 * A FlowChild that reads normally and refuses to be written, so a link write can fail
 * for real without a mock while the history lookup that precedes it still answers.
 * Swapped in through config('saga-lara-flow.models.flow_child').
 */
final class UnwritableFlowChild extends FlowChild
{
    /**
     * @param  array<string, mixed>  $options
     */
    public function save(array $options = []): bool
    {
        throw new RuntimeException('flow child link could not be written');
    }
}
