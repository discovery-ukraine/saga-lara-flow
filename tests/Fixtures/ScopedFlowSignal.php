<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use Illuminate\Database\Eloquent\Builder;

/**
 * A signal model whose global scope hides every row named $hiddenName, as a host's own scope
 * might. Swapped in through config('saga-lara-flow.models.flow_signal').
 */
final class ScopedFlowSignal extends FlowSignal
{
    public static ?string $hiddenName = null;

    protected static function booted(): void
    {
        self::addGlobalScope('hidden', function (Builder $query): void {
            if (self::$hiddenName !== null) {
                $query->where('name', '!=', self::$hiddenName);
            }
        });
    }
}
