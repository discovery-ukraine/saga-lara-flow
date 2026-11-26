<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotCancelTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
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
     * @throws RetryPolicyReentryException
     */
    private function rejectWhileDeciding(string $operation): void
    {
        // The executor owns the runtime of every pass; one resolved from the container is bound to
        // nothing.
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
     * A key the workflow also tags in handle() is rewritten on every replay.
     */
    public function tag(string $key, string|int|null $value = null): static
    {
        return $this->withTags([$key => $value]);
    }

    /**
     * @param  array<array-key, string|int|null>  $tags
     */
    public function withTags(array $tags): static
    {
        $this->rejectWhileDeciding('withTags()');

        foreach ($tags as $key => $value) {
            $this->flowRun->tags()->updateOrCreate(['key' => $key], ['value' => $value]);
        }

        // A loaded relation would keep serving the tags it held before these writes.
        $this->flowRun->unsetRelation('tags');

        return $this;
    }

    /**
     * @param  array<int|string, mixed>  $payload
     *
     * @throws CannotSignalFlowException
     * @throws FlowNotFoundException
     */
    public function signal(string $name, array $payload = []): FlowRun
    {
        $this->rejectWhileDeciding('signal()');

        app(SignalDispatcher::class)->deliver($this->flowRun, $name, $payload);

        return $this->flowRun;
    }

    /**
     * "IfRunning" means any run still taking signals: Pending, Running or Waiting.
     *
     * @param  array<int|string, mixed>  $payload
     */
    public function signalIfRunning(string $name, array $payload = []): bool
    {
        try {
            $this->signal($name, $payload);

            return true;
        } catch (CannotSignalFlowException|FlowNotFoundException) {
            return false;
        }
    }

    /**
     * @param  array<int|string, mixed>  $payload
     *
     * @throws CannotSignalFlowException
     * @throws FlowNotFoundException
     */
    public function signalRetry(?string $name = null, array $payload = []): FlowRun
    {
        $this->rejectWhileDeciding('signalRetry()');

        $dispatcher = app(SignalDispatcher::class);

        if ($name === null) {
            $dispatcher->deliverRetry($this->flowRun, $payload);
        } else {
            $dispatcher->deliver($this->flowRun, $name, $payload);
        }

        return $this->flowRun;
    }

    /**
     * A call from the run's own retry predicate still throws.
     *
     * @param  array<int|string, mixed>  $payload
     */
    public function signalRetryIfRunning(?string $name = null, array $payload = []): bool
    {
        try {
            $this->signalRetry($name, $payload);

            return true;
        } catch (CannotSignalFlowException|FlowNotFoundException) {
            return false;
        }
    }

    /**
     * @throws CannotCancelTerminalFlowException
     * @throws ConcurrentFlowTransitionException
     */
    public function cancel(?string $reason = null): FlowRun
    {
        $this->rejectWhileDeciding('cancel()');

        if ($this->flowRun->isTerminal()) {
            throw CannotCancelTerminalFlowException::for($this->flowRun);
        }

        if ($this->flowRun->status === FlowStatus::Cancelling) {
            app(SagaRunner::class)->abandon($this->flowRun);
        } else {
            try {
                $this->flowRun->markCancelled();
            } catch (ConcurrentFlowTransitionException $lost) {
                $current = $this->onWriter();

                if ($current?->status !== FlowStatus::Cancelling) {
                    throw $lost;
                }

                $this->flowRun->setRawAttributes($current->getAttributes(), sync: true);

                app(SagaRunner::class)->abandon($this->flowRun);
            }
        }

        app(FlowLifecycleRecorder::class)->flowCancelled($this->flowRun, $reason);

        app(ChildWorkflowManager::class)->onFlowFinalized($this->flowRun, false);

        return $this->flowRun;
    }

    /**
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

        if ($this->flowRun->status === FlowStatus::Cancelling) {
            return $this->leftToOwner($this->onWriter());
        }

        $entries = app(FlowExecutor::class)->collectCompensations($this->flowRun);

        try {
            app(StateMachine::class)->transition(
                $this->flowRun->intendRollback(FlowStatus::Cancelled, null),
                FlowStatus::Cancelling,
            );
        } catch (ConcurrentFlowTransitionException $lost) {
            $current = $this->onWriter();

            if ($current?->status !== FlowStatus::Cancelling) {
                throw $lost;
            }

            return $this->leftToOwner($current);
        }

        $entries = app(FlowExecutor::class)->replanCompensations($this->flowRun, $entries);

        app(SagaRunner::class)->rollback(
            $this->flowRun,
            $entries,
            null,
            RunMode::Sync,
            FlowStatus::Cancelled
        );

        return $this->flowRun;
    }

    /**
     * @throws CannotCancelTerminalFlowException
     */
    private function leftToOwner(?FlowRun $current): FlowRun
    {
        if ($current !== null) {
            $this->flowRun->setRawAttributes($current->getAttributes(), sync: true);
        }

        if ($this->flowRun->isTerminal()) {
            throw CannotCancelTerminalFlowException::for($this->flowRun);
        }

        return $this->flowRun;
    }

    private function onWriter(): ?FlowRun
    {
        return $this->flowRun->newQuery()->useWritePdo()->find($this->flowRun->getKey());
    }
}
