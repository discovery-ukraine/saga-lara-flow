<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotCancelTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildWorkflowManager;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\History;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SagaRunner;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalDispatcher;
use Illuminate\Database\Eloquent\Collection;
use Throwable;

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

    /**
     * Deliver an external signal to this run and wake it. Throws on a terminal run.
     *
     * @param  array<int|string, mixed>  $payload
     *
     * @throws CannotSignalTerminalFlowException
     */
    public function signal(string $name, array $payload = []): FlowRun
    {
        app(SignalDispatcher::class)->deliver($this->flowRun, $name, $payload);

        return $this->flowRun;
    }

    /**
     * Safe variant of signal(): swallows the terminal-run rejection and reports
     * whether the signal was delivered. (A missing run cannot reach here — loadFlow()
     * throws FlowNotFoundException before a handle is created.)
     *
     * @param  array<int|string, mixed>  $payload
     */
    public function signalIfRunning(string $name, array $payload = []): bool
    {
        try {
            $this->signal($name, $payload);

            return true;
        } catch (CannotSignalTerminalFlowException) {
            return false;
        }
    }

    public function cancel(?string $reason = null): FlowRun
    {
        if ($this->flowRun->isTerminal()) {
            throw CannotCancelTerminalFlowException::for($this->flowRun);
        }

        // Direct cancellation (no compensation). Compensation-aware cancel is compensate().
        $this->flowRun->markCancelled();

        app(ChildWorkflowManager::class)->onFlowFinalized($this->flowRun, false);

        return $this->flowRun;
    }

    /**
     * Manually roll back this run's completed compensatable steps and cancel it.
     * The compensation stack is reconstructed by a compensation-only replay (no
     * business logic re-runs); the rollback executes synchronously (sync mode) and
     * the run lands in Cancelled. Only valid for a non-terminal run.
     *
     * @throws CannotCancelTerminalFlowException
     * @throws Throwable
     */
    public function compensate(): FlowRun
    {
        if ($this->flowRun->isTerminal()) {
            throw CannotCancelTerminalFlowException::for($this->flowRun);
        }

        $entries = app(FlowExecutor::class)->collectCompensations($this->flowRun);

        app(StateMachine::class)->transition($this->flowRun, FlowStatus::Cancelling);

        app(SagaRunner::class)->rollback(
            $this->flowRun,
            $entries,
            null,
            RunMode::Sync,
            FlowStatus::Cancelled
        );

        return $this->flowRun;
    }
}
