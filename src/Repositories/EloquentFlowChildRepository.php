<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowChildRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use Illuminate\Database\Eloquent\Collection;

class EloquentFlowChildRepository implements FlowChildRepository
{
    public function find(string $parentFlowRunId, int $sequence): ?FlowChild
    {
        return $this->model()::query()
            ->where('parent_flow_run_id', $parentFlowRunId)
            ->where('sequence', $sequence)
            ->first();
    }

    public function active(string $parentFlowRunId): Collection
    {
        $terminal = array_filter(ChildStatus::cases(), fn (ChildStatus $status) => $status->isTerminal())
                |> (fn ($x) => array_map(fn (ChildStatus $status) => $status->value, $x))
                |> array_values(...);

        return $this->model()::query()
            ->useWritePdo()
            ->where('parent_flow_run_id', $parentFlowRunId)
            ->whereNotIn('status', $terminal)
            ->get();
    }

    /**
     * @return class-string<FlowChild>
     */
    private function model(): string
    {
        return config('saga-lara-flow.models.flow_child');
    }
}
