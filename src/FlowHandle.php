<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotCancelTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildWorkflowManager;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowLifecycleRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\History;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SagaRunner;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalDispatcher;
use Illuminate\Database\Eloquent\Collection;
use Throwable;

/**
 * Operations over a single flow run: read its state and history, deliver signals,
 * cancel it, or manually compensate it.
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

    /**
     * Refuse a write to the run whose own retryOnSignal() predicate is running.
     *
     * @throws RetryPolicyReentryException
     */
    private function rejectWhileDeciding(string $operation): void
    {
        // Asked of the executor, not of the container: FlowExecutor is a singleton
        // holding a scoped runtime, and a queue worker forgets scoped instances
        // between jobs — so a fresh resolve would answer for the wrong instance.
        if (app(FlowExecutor::class)->isDecidingRun($this->flowRun->id)) {
            throw RetryPolicyReentryException::for($operation);
        }
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
     * Attach a queryable tag to this run, overwriting the value a previous write
     * left under that key. A workflow that tags the same key in handle() rewrites
     * it on every replay, so prefer keys the workflow does not write itself.
     */
    public function tag(string $key, string|int|null $value = null): static
    {
        return $this->withTags([$key => $value]);
    }

    /**
     * Attach several queryable tags at once, keyed by tag name.
     *
     * @param  array<string, string|int|null>  $tags
     */
    public function withTags(array $tags): static
    {
        $this->rejectWhileDeciding('withTags()');

        foreach ($tags as $key => $value) {
            $this->flowRun->tags()->updateOrCreate(['key' => $key], ['value' => $value]);
        }

        // tags() reads a lazily loaded relation, which would otherwise keep serving
        // the collection it held before these writes.
        $this->flowRun->unsetRelation('tags');

        return $this;
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
        $this->rejectWhileDeciding('signal()');

        app(SignalDispatcher::class)->deliver($this->flowRun, $name, $payload);

        return $this->flowRun;
    }

    /**
     * Safe variant of signal(): swallows the terminal-run rejection and reports
     * whether the signal was delivered. "IfRunning" means "unless the run has
     * already finished" — a signal reaches any non-terminal run (Pending, Running,
     * or Waiting, e.g. one parked on awaitSignal()), not only a Running one. (A
     * missing run cannot reach here — loadFlow() throws FlowNotFoundException before
     * a handle is created.)
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

    /**
     * Cancel this run directly, without compensation (compensation-aware cancel is
     * compensate()). The optional $reason is recorded on the flow.cancelled event
     * and carried on the FlowCancelled event. Throws on a terminal run.
     *
     * @throws CannotCancelTerminalFlowException
     * @throws ConcurrentFlowTransitionException
     */
    public function cancel(?string $reason = null): FlowRun
    {
        $this->rejectWhileDeciding('cancel()');

        if ($this->flowRun->isTerminal()) {
            throw CannotCancelTerminalFlowException::for($this->flowRun);
        }

        $this->flowRun->markCancelled();

        app(FlowLifecycleRecorder::class)->flowCancelled($this->flowRun, $reason);

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
     * @throws ConcurrentFlowTransitionException
     * @throws Throwable
     */
    public function compensate(): FlowRun
    {
        $this->rejectWhileDeciding('compensate()');

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
