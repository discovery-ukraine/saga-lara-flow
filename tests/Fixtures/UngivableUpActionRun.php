<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use Illuminate\Database\Eloquent\Builder;
use RuntimeException;

/**
 * An ActionRun whose give-up to OptionalFailed cannot be written, so the queue's failure
 * hook stops after it has recorded the attempts as spent. Swapped in through
 * config('saga-lara-flow.models.action_run').
 */
final class UngivableUpActionRun extends ActionRun
{
    /**
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return Builder<self>
     */
    public function newEloquentBuilder($query): Builder
    {
        return new class($query) extends Builder
        {
            /**
             * @param  array<string, mixed>  $values
             */
            public function update(array $values): int
            {
                $status = $values['status'] ?? null;

                if ($status === ActionStatus::OptionalFailed || $status === ActionStatus::OptionalFailed->value) {
                    throw new RuntimeException('the give-up could not be written');
                }

                return parent::update($values);
            }
        };
    }
}
