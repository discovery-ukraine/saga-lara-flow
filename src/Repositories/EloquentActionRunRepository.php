<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
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
            ->whereHas('flowRun', FlowRun::live(...))
            ->orderBy('expires_at')
            ->limit($limit)
            ->get();
    }

    /**
     * Repair sends another job, so the run has to be one that may still start work, not
     * merely one that has not finished. A run rolling back would have the rule refuse
     * the row anyway — after it had taken a slot in the batch and before anything held
     * it off, so ordered oldest-first and never changing it would sit at the head of
     * every pass and starve the runs behind it.
     */
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
            ->whereHas('flowRun', FlowRun::mayStartWork(...))
            ->orderBy('created_at')
            ->limit($limit)
            ->get();
    }

    /**
     * The claim resolves each row's reclaim window into an absolute reclaim_stale_at,
     * so staleness is one indexed comparison — the same shape as dueForExpiration().
     * Ordering by the filtered column is what makes the limit safe: a pass can only be
     * filled with rows that are actually due, so a backlog of not-yet-stale rows can
     * never crowd out stale ones. It sends a job too, so it asks dueForRepair()'s
     * narrower question about the run.
     */
    public function dueForStaleRunningRepair(int $limit, int $maxAttempts): iterable
    {
        $now = Carbon::now();

        return $this->model()::query()
            ->where('status', ActionStatus::Running)
            ->whereNull('parallel_group')
            ->whereNotNull('reclaim_stale_at')
            ->where('reclaim_stale_at', '<=', $now)
            ->where('repair_attempts', '<', $maxAttempts)
            ->where(function ($query) use ($now): void {
                $query
                    ->whereNull('repair_available_at')
                    ->orWhere('repair_available_at', '<=', $now);
            })
            ->whereHas('flowRun', FlowRun::mayStartWork(...))
            ->orderBy('reclaim_stale_at')
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
