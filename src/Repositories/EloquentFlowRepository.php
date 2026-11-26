<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Contracts\Database\Eloquent\Builder;
use Illuminate\Support\Carbon;

class EloquentFlowRepository implements FlowRepository
{
    public function create(array $attributes, array $tags = []): FlowRun
    {
        $model = $this->flowRunClass();

        $run = new $model;
        $run->fill($attributes);
        $run->save();

        foreach ($tags as $tag) {
            $run->tags()->create([
                'key' => $tag['key'],
                'value' => $tag['value'] ?? null,
            ]);
        }

        return $run;
    }

    public function find(string $id): ?FlowRun
    {
        return $this->flowRunClass()::query()->find($id);
    }

    public function findOnWriter(string $id): ?FlowRun
    {
        return $this->flowRunClass()::query()->useWritePdo()->find($id);
    }

    public function findOrFail(string $id): FlowRun
    {
        return $this->find($id) ?? throw FlowNotFoundException::for($id);
    }

    public function dueForExpiration(int $limit): iterable
    {
        $now = Carbon::now();

        return $this->flowRunClass()::query()
            ->whereIn('status', [FlowStatus::Running, FlowStatus::Waiting])
            ->whereNotNull('expires_at')
            ->where('expires_at', '<=', $now)
            ->where(function (Builder $query) use ($now): void {
                $query->whereNull('expiry_available_at')
                    ->orWhere('expiry_available_at', '<=', $now);
            })
            // A held-off run sorts by its retry time, so retries cannot queue ahead of older runs.
            ->orderByRaw('coalesce(expiry_available_at, expires_at) asc')
            ->limit($limit)
            ->get();
    }

    public function dueForRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable
    {
        $now = Carbon::now();

        return $this->flowRunClass()::query()
            ->where('status', FlowStatus::Waiting)
            ->where('updated_at', '<=', $now->copy()->subSeconds($graceSeconds))
            ->where('repair_attempts', '<', $maxAttempts)
            ->where(function ($query) use ($now): void {
                $query->whereNull('repair_available_at')
                    ->orWhere('repair_available_at', '<=', $now);
            })
            ->whereDoesntHave('actions', function (Builder $query): void {
                $query->whereIn('status', [ActionStatus::Pending, ActionStatus::Running]);
            })
            ->whereDoesntHave('signals', function (Builder $query): void {
                $query->where('status', SignalStatus::Waiting);
            })
            ->orderBy('updated_at')
            ->limit($limit)
            ->get();
    }

    public function dueForStartRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable
    {
        $now = Carbon::now();

        return $this->flowRunClass()::query()
            ->where('status', FlowStatus::Pending)
            ->where('updated_at', '<=', $now->copy()->subSeconds($graceSeconds))
            ->where('repair_attempts', '<', $maxAttempts)
            ->where(function ($query) use ($now): void {
                $query->whereNull('repair_available_at')
                    ->orWhere('repair_available_at', '<=', $now);
            })
            ->orderBy('updated_at')
            ->limit($limit)
            ->get();
    }

    public function dueForRollbackRecovery(int $limit, int $graceSeconds): iterable
    {
        $now = Carbon::now();
        $model = $this->flowRunClass();

        // From the writer: the takeover does not look at compensations again.
        return $model::query()
            ->useWritePdo()
            ->where(fn ($query) => $model::rollbackStalled($query, $now->copy()->subSeconds($graceSeconds)))
            ->where('rollback_target', FlowStatus::Expired)
            ->where(function (Builder $query) use ($now): void {
                $query->whereNull('expiry_available_at')
                    ->orWhere('expiry_available_at', '<=', $now);
            })
            ->orderBy('updated_at')
            ->limit($limit)
            ->get();
    }

    public function dueForRollbackRepair(int $limit, int $graceSeconds, int $maxAttempts): iterable
    {
        $now = Carbon::now();
        $model = $this->flowRunClass();

        return $model::query()
            ->useWritePdo()
            ->where(fn ($query) => $model::rollbackStalled($query, $now->copy()->subSeconds($graceSeconds)))
            ->whereIn('rollback_target', [FlowStatus::Failed, FlowStatus::Cancelled])
            ->where('repair_attempts', '<', $maxAttempts)
            ->where(function (Builder $query) use ($now): void {
                $query->whereNull('repair_available_at')
                    ->orWhere('repair_available_at', '<=', $now);
            })
            ->orderBy('updated_at')
            ->limit($limit)
            ->get();
    }

    /**
     * @return class-string<FlowRun>
     */
    private function flowRunClass(): string
    {
        return config('saga-lara-flow.models.flow_run');
    }
}
