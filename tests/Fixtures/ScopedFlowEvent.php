<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use Illuminate\Database\Eloquent\Builder;

/**
 * An event model whose global scope hides every row of type $hiddenType, as a host's own scope
 * might. Swapped in through config('saga-lara-flow.models.flow_event').
 */
final class ScopedFlowEvent extends FlowEvent
{
    public static ?FlowEventType $hiddenType = null;

    protected static function booted(): void
    {
        self::addGlobalScope('hidden', function (Builder $query): void {
            if (self::$hiddenType !== null) {
                $query->where('type', '!=', self::$hiddenType->value);
            }
        });
    }
}
