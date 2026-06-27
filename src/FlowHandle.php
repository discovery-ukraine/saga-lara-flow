<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotCancelTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Collection;

/**
 * Operations over a single flow run. Read methods are available now;
 * signal()/compensate() are introduced in later phases.
 */
readonly class FlowHandle
{
    public function __construct(
        private FlowRun $run,
    ) {}

    public function id(): string
    {
        return $this->run->id;
    }

    public function run(): FlowRun
    {
        return $this->run;
    }

    public function status(): FlowStatus
    {
        return $this->run->status;
    }

    public function history(): Collection
    {
        return $this->run->events;
    }

    public function actions(): Collection
    {
        return $this->run->actions;
    }

    public function signals(): Collection
    {
        return $this->run->signals;
    }

    public function tags(): Collection
    {
        return $this->run->tags;
    }

    public function cancel(?string $reason = null): FlowRun
    {
        if ($this->run->isTerminal()) {
            throw CannotCancelTerminalFlowException::for($this->run);
        }

        // Phase 1: direct cancellation. Compensation-aware cancel arrives with sagas.
        return $this->run->markCancelled();
    }
}
