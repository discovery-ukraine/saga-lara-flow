<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;

class EloquentActionRunRepository implements ActionRunRepository
{
    public function find(string $flowRunId, int $sequence): ?ActionRun
    {
        return $this->model()::query()
            ->where('flow_run_id', $flowRunId)
            ->where('sequence', $sequence)
            ->first();
    }

    /**
     * @return class-string<ActionRun>
     */
    private function model(): string
    {
        return config('saga-lara-flow.models.action_run');
    }
}
