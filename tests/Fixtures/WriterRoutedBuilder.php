<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use Illuminate\Database\Eloquent\Builder;

/**
 * Eloquent forwards useWritePdo() to the query builder rather than declaring it, so
 * counting it means declaring it here and delegating by hand.
 *
 * @template TModel of \Illuminate\Database\Eloquent\Model
 *
 * @extends Builder<TModel>
 */
final class WriterRoutedBuilder extends Builder
{
    public function useWritePdo(): static
    {
        WriterRoutedFlowRun::$writerReads++;

        $this->getQuery()->useWritePdo();

        return $this;
    }
}
