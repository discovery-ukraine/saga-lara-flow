<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Concerns\ResolvesMethodDependencies;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
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
    ) {
    }

    /**
     * @throws Throwable
     */
    public function drive(FlowRun $flowRun, RunMode $mode): FlowRun
    {
        $this->tenancy->restore($flowRun);

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
            } catch (Throwable $exception) {
                return $this->failAndCompensate($flowRun, $exception, $mode);
            }

            return $this->completeFlow($flowRun, $result);
        }
    }

    /**
     * Rebuild the compensation stack for a run without executing any business
     * logic: replay handle() in collecting mode so completed steps register their
     * compensations and the replay stops at the live frontier. Used by the manual
     * FlowHandle::compensate() path.
     *
     * @return list<CompensationEntry>
     *
     * @throws HistoryContractMismatchException
     */
    public function collectCompensations(FlowRun $flowRun): array
    {
        $this->tenancy->restore($flowRun);

        $this->runtime->bind($flowRun, RunMode::Queued);
        $this->runtime->reset();
        $this->runtime->beginCollecting();

        try {
            $workflow = app()->make($flowRun->workflow_class, ['runtime' => $this->runtime]);

            /** @var array<int, mixed> $arguments */
            $arguments = (array) $this->serializer->deserialize($flowRun->arguments ?? []);

            $this->callWithDependencies($workflow, 'handle', $arguments);
        } catch (HistoryContractMismatchException $mismatch) {
            $this->runtime->endCollecting();
            $this->runtime->clear();

            throw $mismatch;
        } catch (Throwable) {
            // FlowSuspended at the frontier, or a recorded business failure replaying
            // as a throw — either way the stack is complete up to this point.
        }

        $entries = $this->runtime->sagaStack()->entries();

        $this->runtime->endCollecting();
        $this->runtime->clear();

        return $entries;
    }

    private function suspend(FlowRun $flowRun): FlowRun
    {
        $flowRun->markWaiting();

        $this->recorder->flowWaiting($flowRun);

        return $flowRun;
    }

    private function completeFlow(FlowRun $flowRun, mixed $result): FlowRun
    {
        $flowRun->result = $this->normalizeResult($result);

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
     * @throws Throwable
     */
    public function expireRun(FlowRun $flowRun): FlowRun
    {
        $primary = $this->exceptionToArray(FlowExpiredException::forFlowRun($flowRun));

        $entries = $this->collectCompensations($flowRun);

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

    /**
     * @return array<int|string, mixed>|null
     */
    private function normalizeResult(mixed $result): ?array
    {
        $serialized = $this->serializer->serialize($result);

        if ($serialized === null) {
            return null;
        }

        return is_array($serialized) ? $serialized : ['value' => $serialized];
    }
}
