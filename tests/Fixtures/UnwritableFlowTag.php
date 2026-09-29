<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowTag;
use RuntimeException;

/**
 * A FlowTag that refuses to be written, so a tag write can fail for real without a mock.
 * Swapped in through config('saga-lara-flow.models.flow_tag').
 */
final class UnwritableFlowTag extends FlowTag
{
    /**
     * @param  array<string, mixed>  $options
     */
    public function save(array $options = []): bool
    {
        throw new RuntimeException('flow tag could not be written');
    }
}
