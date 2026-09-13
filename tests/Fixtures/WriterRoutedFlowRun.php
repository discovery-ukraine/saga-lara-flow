<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Builder;

/**
 * Counts the reads against it that asked for the write connection. The suite runs a
 * single PDO, so nothing else can tell a writer-bound read from an ordinary one —
 * without this, dropping useWritePdo() would leave every assertion green. Swapped in
 * through config('saga-lara-flow.models.flow_run').
 */
final class WriterRoutedFlowRun extends FlowRun
{
    public static int $writerReads = 0;

    public static function reset(): void
    {
        self::$writerReads = 0;
    }

    /**
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return Builder<self>
     */
    public function newEloquentBuilder($query): Builder
    {
        return new WriterRoutedBuilder($query);
    }
}
