<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Concerns\ResolvesMethodDependencies;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ExpirationNotPlannedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Throwable;

/**
 * The drive loop — the heart of the engine. It runs the workflow's handle()
 * method and interprets the control-flow exceptions it throws: replay on inline
 * resolution (sync), suspend on a genuine wait (queued), fail on a business
 * error. Completed steps are skipped on replay because they resolve from stored
 * history by their (flow_run_id, sequence) identity.
 *
 * Invariant: catch (Throwable) sits AFTER catch (InternalFlowControl), so an
 * internal control signal is never mistaken for a business failure.
 */
class FlowExecutor
{
    use NormalizesExceptions;
    use ResolvesMethodDependencies;

    public function __construct(
        private readonly StateMachine $stateMachine,
        private readonly FlowLifecycleRecorder $recorder,
        private readonly FlowRuntime $runtime,
        private readonly TenancyManager $tenancy,
        private readonly Serializer $serializer,
        private readonly CompensationRecorder $compensationRecorder,
        private readonly SagaRunner $sagaRunner,
    ) {}

    /**
     * Whether the run currently deciding a retryOnSignal() predicate is this one.
     * The executor answers because it owns the runtime a pass is driven with.
     */
    public function isDecidingRun(string $flowRunId): bool
    {
        return $this->runtime->isDecidingRun($flowRunId);
    }

    /**
     * A retryOnSignal() predicate may read any run it likes, but it may not drive
     * one — not even somebody else's. There is a single runtime behind this
     * singleton, and a nested pass rebinds and resets the one the deciding pass is
     * suspended inside: the outer run loses its saga stack, its ordinal counter and,
     * once the nested pass clears up after itself, the run it was bound to.
     *
     * @throws RetryPolicyReentryException
     */
    private function rejectWhileDeciding(): void
    {
        if ($this->runtime->isDeciding()) {
            throw RetryPolicyReentryException::for('a nested flow execution');
        }
    }

    /**
     * @throws Throwable
     */
    public function drive(FlowRun $flowRun, RunMode $mode): FlowRun
    {
        $this->rejectWhileDeciding();

        try {
            return $this->tenancy->for(
                $flowRun,
                $flowRun->workflow_class,
                fn (): FlowRun => $this->driveInner($flowRun, $mode),
            );
        } catch (ConcurrentFlowTransitionException) {
            // Somebody else owns this run now. Returning it rather than throwing lets
            // every caller do the right thing without knowing about the race: a job ends
            // cleanly, runSync() returns the run as the winner left it, and a parent
            // awaiting it as a child reads its status and resolves accordingly.
            return $this->reread($flowRun);
        }
    }

    /**
     * @throws Throwable
     */
    private function driveInner(FlowRun $flowRun, RunMode $mode): FlowRun
    {
        if ($this->isExpired($flowRun)) {
            return $this->expireRun($flowRun);
        }

        $resuming = $flowRun->status !== FlowStatus::Pending;

        $this->stateMachine->transition($flowRun, FlowStatus::Running);

        $resuming ? $this->recorder->flowResumed($flowRun) : $this->recorder->flowStarted($flowRun);

        while (true) {
            $this->runtime->bind($flowRun, $mode);
            $this->runtime->reset();

            try {
                try {
                    $workflow = app()->make($flowRun->workflow_class, ['runtime' => $this->runtime]);

                    /** @var array<int, mixed> $arguments */
                    $arguments = (array) $this->serializer->deserialize($flowRun->arguments ?? []);

                    $result = $this->callWithDependencies($workflow, 'handle', $arguments);
                } finally {
                    $this->runtime->clear();
                }
            } catch (FlowSuspended $suspended) {
                if ($suspended->inlineResolved) {
                    continue; // Sync: the step ran inline; replay from the top.
                }

                return $this->suspend($flowRun);
            } catch (InternalFlowControl) {
                return $this->suspend($flowRun);
            } catch (ConcurrentFlowTransitionException $lost) {
                // Must precede the catch below: compensating over a refused transition
                // would roll back a run this pass no longer owns.
                throw $lost;
            } catch (Throwable $exception) {
                return $this->failAndCompensate($flowRun, $exception, $mode);
            }

            return $this->completeFlow($flowRun, $result);
        }
    }

    /**
     * Rebuild the compensation stack for a run: replay handle() in collecting mode
     * so completed steps register their compensations and the replay stops at the
     * live frontier. Every seam is guarded, so the pass starts no work and settles
     * no step; a workflow's own tag() calls still rewrite their rows, as they do on
     * every replay. Used by FlowHandle::compensate(). A throw the
     * replay did not expect is a fault, not a frontier, and leaves rather than
     * shortening the stack behind the caller's back.
     *
     * @return list<CompensationEntry>
     *
     * @throws Throwable
     */
    public function collectCompensations(FlowRun $flowRun): array
    {
        $this->rejectWhileDeciding();

        return $this->tenancy->for(
            $flowRun,
            $flowRun->workflow_class,
            fn (): array => $this->collectCompensationsInner($flowRun),
        );
    }

    /**
     * @return list<CompensationEntry>
     *
     * @throws Throwable
     */
    private function collectCompensationsInner(FlowRun $flowRun): array
    {
        $this->runtime->bind($flowRun, RunMode::Queued);
        $this->runtime->reset();
        $this->runtime->beginCollecting();

        // Every exit unbinds, including the ones that leave by throwing: the runtime
        // is a singleton, and a pass that left it collecting would make the next
        // ordinary drive of any run refuse to start work.
        try {
            try {
                $workflow = app()->make($flowRun->workflow_class, ['runtime' => $this->runtime]);

                /** @var array<int, mixed> $arguments */
                $arguments = (array) $this->serializer->deserialize($flowRun->arguments ?? []);

                $this->callWithDependencies($workflow, 'handle', $arguments);
            } catch (InternalFlowControl|ActionFailedException|FlowExpiredException|AwaitSignalTimeoutException) {
                // The four classes that end a replay: the frontier, and a step failure,
                // an expiry or a signal timeout already recorded in this run's history.
                // Membership is all this tests — a caller raising one of these itself
                // is read as an ending too, which is why the set is kept small.
                //
                // Nothing else ends it. A throw from a builder argument, a workflow
                // helper or anything else the replay runs is a fault, and swallowing it
                // hands back a stack truncated at that point for compensate() to roll
                // back and report as a complete unwind. It leaves here instead, before
                // the run has been touched, so the operator sees the cause and still
                // has a run to retry the rollback on.
            }

            return $this->runtime->sagaStack()->entries();
        } finally {
            $this->runtime->endCollecting();
            $this->runtime->clear();
        }
    }

    private function suspend(FlowRun $flowRun): FlowRun
    {
        $flowRun->markWaiting();

        $this->recorder->flowWaiting($flowRun);

        return $flowRun;
    }

    private function completeFlow(FlowRun $flowRun, mixed $result): FlowRun
    {
        $flowRun->result = $this->serializer->serialize($result);

        $flowRun->markCompleted();

        $this->recorder->flowCompleted($flowRun);

        app(ChildWorkflowManager::class)->onFlowFinalized($flowRun, true);

        return $flowRun;
    }

    /**
     * Business failure: roll back the compensation stack gathered by the failing
     * pass (LIFO), then land in Failed. An empty stack — or a history-contract
     * mismatch, which must bypass compensation — fails directly.
     *
     * @throws Throwable
     */
    private function failAndCompensate(FlowRun $flowRun, Throwable $exception, RunMode $mode): FlowRun
    {
        if ($exception instanceof HistoryContractMismatchException) {
            return $this->failFlow($flowRun, $exception);
        }

        $entries = $this->runtime->sagaStack()->entries();

        if ($entries === []) {
            return $this->failFlow($flowRun, $exception);
        }

        $this->stateMachine->transition($flowRun, FlowStatus::Cancelling);

        $this->compensationRecorder->started($flowRun);

        $this->sagaRunner->rollback(
            $flowRun,
            $entries,
            $this->exceptionToArray($exception),
            $mode,
            FlowStatus::Failed
        );

        return $flowRun;
    }

    private function failFlow(FlowRun $flowRun, Throwable $exception): FlowRun
    {
        $flowRun->markFailed([
            'class' => $exception::class,
            'message' => $exception->getMessage(),
            'code' => $exception->getCode(),
        ]);

        $this->recorder->flowFailed($flowRun, $exception);

        app(ChildWorkflowManager::class)->onFlowFinalized($flowRun, true);

        return $flowRun;
    }

    /**
     * Expire an overdue run. Mirrors FlowHandle::compensate() but lands in Expired:
     * rebuild the compensation stack by a compensation-only replay, then roll it back
     * (queued) and finalize as Expired — or, with nothing to undo, expire directly.
     * Shared by the monitor's sweep (FlowMonitor::expireRun) and the lazy drive check.
     *
     * Guarded separately from drive(), because the sweep reaches it directly: one run
     * claimed by someone else in the meantime must not end the whole pass.
     *
     * @throws Throwable
     */
    public function expireRun(FlowRun $flowRun): FlowRun
    {
        try {
            return $this->expireRunInner($flowRun);
        } catch (ConcurrentFlowTransitionException) {
            return $this->reread($flowRun);
        }
    }

    /**
     * Read the run as the winner of a refused transition left it. From the writer: the
     * whole point of this read is the state that was just committed, and it is what a
     * caller — a parent resolving this run as a child among them — decides on.
     */
    private function reread(FlowRun $flowRun): FlowRun
    {
        return $flowRun->newQuery()->useWritePdo()->find($flowRun->getKey()) ?? $flowRun;
    }

    /**
     * @throws Throwable
     */
    private function expireRunInner(FlowRun $flowRun): FlowRun
    {
        $primary = $this->exceptionToArray(FlowExpiredException::forFlowRun($flowRun));

        // Named, because what the sweep does about a failure depends entirely on
        // whether it happened here. Nothing has been written yet, so a run whose
        // rollback cannot be planned is still exactly where it was found.
        try {
            $entries = $this->collectCompensations($flowRun);
        } catch (Throwable $planning) {
            throw ExpirationNotPlannedException::for($flowRun, $planning);
        }

        if ($entries === []) {
            $flowRun->exception = $primary;

            $flowRun->markExpired();

            $this->recorder->flowExpired($flowRun);

            app(ChildWorkflowManager::class)->onFlowFinalized($flowRun, false);

            return $flowRun;
        }

        $this->stateMachine->transition($flowRun, FlowStatus::Cancelling);

        $this->sagaRunner->rollback($flowRun, $entries, $primary, RunMode::Queued, FlowStatus::Expired);

        return $flowRun;
    }

    /**
     * A run is overdue when expiration is enabled and its deadline has passed.
     */
    private function isExpired(FlowRun $flowRun): bool
    {
        return (bool) config('saga-lara-flow.monitor.expiration.enabled')
            && $flowRun->expires_at !== null
            && $flowRun->expires_at->lessThanOrEqualTo(now());
    }
}
