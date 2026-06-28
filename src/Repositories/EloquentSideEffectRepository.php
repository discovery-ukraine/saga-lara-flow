<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\SideEffectRepository;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;

class EloquentSideEffectRepository implements SideEffectRepository
{
    public function find(string $flowRunId, int $sequence): ?SideEffect
    {
        return $this->model()::query()
            ->where('flow_run_id', $flowRunId)
            ->where('sequence', $sequence)
            ->first();
    }

    /**
     * @return class-string<SideEffect>
     */
    private function model(): string
    {
        return config('saga-lara-flow.models.side_effect');
    }
}
