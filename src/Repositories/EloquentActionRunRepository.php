<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use Illuminate\Support\Carbon;

class EloquentActionRunRepository implements ActionRunRepository
{
    public function find(string $flowRunId, int $sequence): ?ActionRun
    {
        return $this->model()::query()
            ->where('flow_run_id', $flowRunId)
            ->where('sequence', $sequence)
            ->first();
    }

    public function dueForExpiration(int $limit): iterable
    {
        return $this->model()::query()
            ->whereIn('status', [ActionStatus::Pending, ActionStatus::Running])
            ->whereNotNull('expires_at')
            ->where('expires_at', '<=', Carbon::now())
            ->orderBy('expires_at')
            ->limit($limit)
            ->get();
    }

    public function dueForRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable
    {
        $now = Carbon::now();

        return $this->model()::query()
            ->where('status', ActionStatus::Pending)
            ->whereNull('parallel_group')
            ->where('created_at', '<=', $now->copy()->subSeconds($graceSeconds))
            ->where('repair_attempts', '<', $maxAttempts)
            ->where(function ($query) use ($now): void {
                $query
                    ->whereNull('repair_available_at')
                    ->orWhere('repair_available_at', '<=', $now);
            })
            ->orderBy('created_at')
            ->limit($limit)
            ->get();
    }

    /**
     * @return class-string<ActionRun>
     */
    private function model(): string
    {
        return config('saga-lara-flow.models.action_run');
    }
}
