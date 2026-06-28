<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;

class EloquentSignalRepository implements SignalRepository
{
    public function find(string $flowRunId, int $sequence): ?FlowSignal
    {
        return $this->model()::query()
            ->where('flow_run_id', $flowRunId)
            ->where('wait_sequence', $sequence)
            ->first();
    }

    public function earliestPending(string $flowRunId, string $name): ?FlowSignal
    {
        return $this->model()::query()
            ->where('flow_run_id', $flowRunId)
            ->where('name', $name)
            ->where('status', SignalStatus::Received)
            ->whereNull('wait_sequence')
            ->orderBy('id')
            ->first();
    }

    public function earliestWaiting(string $flowRunId, string $name): ?FlowSignal
    {
        return $this->model()::query()
            ->where('flow_run_id', $flowRunId)
            ->where('name', $name)
            ->where('status', SignalStatus::Waiting)
            ->whereNotNull('wait_sequence')
            ->orderBy('id')
            ->first();
    }

    /**
     * @return class-string<FlowSignal>
     */
    private function model(): string
    {
        return config('saga-lara-flow.models.flow_signal');
    }
}
