<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use Illuminate\Database\Eloquent\Builder;

/**
 * Read through LaggingReplicaBuilder. Swapped in through config('saga-lara-flow.models.*').
 */
final class LaggingReplicaFlowChild extends FlowChild
{
    /**
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return Builder<self>
     */
    public function newEloquentBuilder($query): Builder
    {
        return new LaggingReplicaBuilder($query);
    }
}
