<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotCancelTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\History;
use Illuminate\Database\Eloquent\Collection;

/**
 * Operations over a single flow run. Read methods are available now;
 * signal()/compensate() are introduced in later phases.
 */
readonly class FlowHandle
{
    public function __construct(
        private FlowRun $flowRun,
    ) {}

    public function id(): string
    {
        return $this->flowRun->id;
    }

    public function run(): FlowRun
    {
        return $this->flowRun;
    }

    public function status(): FlowStatus
    {
        return $this->flowRun->status;
    }

    public function history(): Collection
    {
        return new History($this->flowRun)->events();
    }

    public function actions(): Collection
    {
        return new History($this->flowRun)->actions();
    }

    public function signals(): Collection
    {
        return $this->flowRun->signals;
    }

    public function tags(): Collection
    {
        return $this->flowRun->tags;
    }

    public function cancel(?string $reason = null): FlowRun
    {
        if ($this->flowRun->isTerminal()) {
            throw CannotCancelTerminalFlowException::for($this->flowRun);
        }

        // Phase 1: direct cancellation. Compensation-aware cancel arrives with sagas.
        return $this->flowRun->markCancelled();
    }
}
