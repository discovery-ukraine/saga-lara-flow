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

    public function findOrFail(string $id): FlowRun
    {
        return $this->find($id) ?? throw FlowNotFoundException::for($id);
    }

    public function dueForExpiration(int $limit): iterable
    {
        return $this->flowRunClass()::query()
            ->whereIn('status', [FlowStatus::Running, FlowStatus::Waiting])
            ->whereNotNull('expires_at')
            ->where('expires_at', '<=', Carbon::now())
            ->orderBy('expires_at')
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
            // Positive evidence the run is stuck rather than legitimately parked:
            // nothing is in flight to wake it (no Pending/Running action, no open
            // signal wait), yet it is still Waiting — a lost resume.
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

    /**
     * @return class-string<FlowRun>
     */
    private function flowRunClass(): string
    {
        return config('saga-lara-flow.models.flow_run');
    }
}
