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
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowCancelledException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ExpirationNotPlannedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RollbackNotPlannedException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Illuminate\Database\Connection;
use Illuminate\Support\Str;
use ReflectionProperty;
use Throwable;

/**
 * catch (Throwable) sits after catch (InternalFlowControl), so a control signal is never taken
 * for a business failure.
 */
class FlowExecutor
{
    use NormalizesExceptions;
    use ResolvesMethodDependencies;

    /**
     * @var list<FlowRuntime>
     */
    private array $runtimes = [];

    /**
     * @var list<array{name: ?string, joined: list<Connection>}>
     */
    private array $writerPasses = [];

    public function __construct(
        private readonly StateMachine $stateMachine,
        private readonly FlowLifecycleRecorder $recorder,
        private readonly TenancyManager $tenancy,
        private readonly Serializer $serializer,
        private readonly CompensationRecorder $compensationRecorder,
        private readonly SagaRunner $sagaRunner,
    ) {}

    /**
     * @template TPass
     *
     * @param  callable(FlowRuntime): TPass  $pass
     * @return TPass
     *
     * @throws Throwable
     */
    private function inOwnRuntime(callable $pass): mixed
    {
        $this->runtimes[] = $runtime = new FlowRuntime;

        try {
            return $pass($runtime);
        } finally {
            array_pop($this->runtimes);
        }
    }

    /**
     * Asks every pass in flight: an inner one is no reason to let a write into a run an outer one
     * is deciding for.
     */
    public function isDecidingRun(string $flowRunId): bool
    {
        return array_any(
            $this->runtimes,
            fn (FlowRuntime $runtime): bool => $runtime->isDecidingRun($flowRunId),
        );
    }

    /**
     * A job to resume a run this process is driving would only meet the pass still holding it.
     */
    public function replayIfDriving(string $flowRunId): bool
    {
        /** @var ?FlowRuntime $pass */
        $pass = array_find(
            array_reverse($this->runtimes),
            fn (FlowRuntime $runtime): bool => $runtime->isDriving($flowRunId),
        );

        $pass?->requestReplay();

        return $pass !== null;
    }

    /**
     * @throws RetryPolicyReentryException
     */
    private function rejectWhileDeciding(): void
    {
        $deciding = array_any(
            $this->runtimes,
            fn (FlowRuntime $runtime): bool => $runtime->isDeciding(),
        );

        if ($deciding) {
            throw RetryPolicyReentryException::for('a nested flow execution');
        }
    }

    /**
     * @throws Throwable
     */
    public function drive(FlowRun $flowRun, RunMode $mode): FlowRun
    {
        $this->rejectWhileDeciding();

        if ($this->closedBeforeStart($flowRun)) {
            return $this->reread($flowRun);
        }

        try {
            return $this->inOwnRuntime(fn (FlowRuntime $runtime): FlowRun => $this->tenancy->for(
                $flowRun,
                $flowRun->workflow_class,
                fn (): FlowRun => $this->onWriter(
                    $flowRun,
                    fn (): FlowRun => $this->driveInner($runtime, $flowRun, $mode),
                ),
            ));
        } catch (ConcurrentFlowTransitionException) {
            // Returned, not thrown: a job ends cleanly and a caller reads the run as the winner left it.
            return $this->reread($flowRun);
        }
    }

    /**
     * Before the tenant is entered, so a tenant that cannot be restored does not keep the child
     * from being fenced.
     */
    private function closedBeforeStart(FlowRun $flowRun): bool
    {
        if ($flowRun->parent_id === null || $flowRun->status !== FlowStatus::Pending) {
            return false;
        }

        $current = $this->reread($flowRun);

        return $current->status === FlowStatus::Pending && app(ChildWorkflowManager::class)->closedBeforeStart($current);
    }

    /**
     * The deadline is weighed after the status check: a run already rolling back would otherwise be
     * expired twice. The pass goes on with the caller's snapshot, the value its transitions fence on.
     *
     * @throws Throwable
     */
    private function driveInner(FlowRuntime $runtime, FlowRun $flowRun, RunMode $mode): FlowRun
    {
        $current = $this->reread($flowRun);

        if (! $current->status->canStartWork()) {
            return $current;
        }

        if ($this->isExpired($flowRun)) {
            return $this->expireRun($flowRun);
        }

        $resuming = $flowRun->status !== FlowStatus::Pending;

        $this->stateMachine->transition($flowRun, FlowStatus::Running);

        $resuming ? $this->recorder->flowResumed($flowRun) : $this->recorder->flowStarted($flowRun);

        return $this->replay($runtime, $flowRun, $mode);
    }

    /**
     * @throws Throwable
     */
    private function replay(FlowRuntime $runtime, FlowRun $flowRun, RunMode $mode): FlowRun
    {
        while (true) {
            $runtime->bind($flowRun, $mode);
            $runtime->reset();

            try {
                $result = $this->callHandle($runtime, $flowRun);
            } catch (FlowSuspended $suspended) {
                if ($suspended->inlineResolved || $runtime->replayRequested()) {
                    continue; // The work ran in this process; replay from the top.
                }

                return $this->suspendOn($suspended, $runtime, $flowRun, $mode);
            } catch (InternalFlowControl) {
                return $this->suspend($flowRun);
            } catch (ConcurrentFlowTransitionException $lost) {
                // Must precede the catch below: compensating over a refused transition
                // would roll back a run this pass no longer owns.
                throw $lost;
            } catch (Throwable $exception) {
                return $this->failAndCompensate($runtime, $flowRun, $exception, $mode);
            }

            return $this->completeFlow($flowRun, $result);
        }
    }

    /**
     * @throws Throwable
     */
    private function callHandle(FlowRuntime $runtime, FlowRun $flowRun): mixed
    {
        try {
            $workflow = app()->make($flowRun->workflow_class, ['runtime' => $runtime]);

            /** @var array<int, mixed> $arguments */
            $arguments = (array) $this->serializer->deserialize($flowRun->arguments ?? []);

            return $this->callWithDependencies($workflow, 'handle', $arguments);
        } finally {
            $runtime->clear();
        }
    }

    /**
     * A throw no seam of this run raised is a fault, not a frontier: it leaves rather than
     * shortening the stack behind the caller's back.
     *
     * @return list<CompensationEntry>
     *
     * @throws Throwable
     */
    public function collectCompensations(FlowRun $flowRun): array
    {
        $this->rejectWhileDeciding();

        return $this->inOwnRuntime(fn (FlowRuntime $runtime): array => $this->tenancy->for(
            $flowRun,
            $flowRun->workflow_class,
            fn (): array => $this->onWriter(
                $flowRun,
                fn (): array => $this->collectCompensationsInner($runtime, $flowRun),
            ),
        ));
    }

    /**
     * The plan drawn under Cancelling is not always the longer one: a step claimed in the gap is
     * Running again, so an ordinal missing from it is restored from the earlier plan.
     *
     * @param  list<CompensationEntry>  $planned
     * @return list<CompensationEntry>
     *
     * @throws RetryPolicyReentryException
     */
    public function replanCompensations(FlowRun $flowRun, array $planned): array
    {
        $this->rejectWhileDeciding();

        try {
            $replanned = $this->collectCompensations($flowRun);
        } catch (Throwable $replanning) {
            app(AnomalyLog::class)->log(AnomalyLog::REASON_REPLAN_FAILED, [
                'entity' => 'flow',
                'flow_run_id' => $flowRun->id,
                'workflow_class' => $flowRun->workflow_class,
                'status' => $flowRun->status->value,
                'planned' => count($planned),
                'exception' => $this->exceptionToArray($replanning),
            ]);

            return $planned;
        }

        $dropped = array_values(array_diff($this->ordinals($planned), $this->ordinals($replanned)));

        if ($dropped === []) {
            return $replanned;
        }

        app(AnomalyLog::class)->log(AnomalyLog::REASON_REPLAN_INCOMPLETE, [
            'entity' => 'flow',
            'flow_run_id' => $flowRun->id,
            'workflow_class' => $flowRun->workflow_class,
            'status' => $flowRun->status->value,
            'planned' => count($planned),
            'replanned' => count($replanned),
            'dropped_sequences' => $dropped,
        ]);

        return $this->merge($planned, $replanned);
    }

    /**
     * The later reading of an ordinal wins: it is the one taken under the fence.
     *
     * @param  list<CompensationEntry>  $planned
     * @param  list<CompensationEntry>  $replanned
     * @return list<CompensationEntry>
     */
    private function merge(array $planned, array $replanned): array
    {
        $merged = [];

        foreach ([...$planned, ...$replanned] as $entry) {
            $merged[$entry->sequence] = $entry;
        }

        ksort($merged);

        return array_values($merged);
    }

    /**
     * @param  list<CompensationEntry>  $entries
     * @return list<int>
     */
    private function ordinals(array $entries): array
    {
        return array_map(static fn (CompensationEntry $entry): int => $entry->sequence, $entries);
    }

    /**
     * @return list<CompensationEntry>
     *
     * @throws Throwable
     */
    private function collectCompensationsInner(FlowRuntime $runtime, FlowRun $flowRun): array
    {
        $runtime->bind($flowRun, RunMode::Queued);
        $runtime->reset();
        $runtime->beginCollecting();

        try {
            try {
                $workflow = app()->make($flowRun->workflow_class, ['runtime' => $runtime]);

                /** @var array<int, mixed> $arguments */
                $arguments = (array) $this->serializer->deserialize($flowRun->arguments ?? []);

                // Any other throw leaves: swallowed, it would hand back a cut-short stack that the
                // caller unwinds and reports as a complete rollback.
                $this->callWithDependencies($workflow, 'handle', $arguments);
            } catch (InternalFlowControl) {
                // The frontier: a seam that cannot be resolved from history yet.
            } catch (ActionFailedException|FlowExpiredException|AwaitSignalTimeoutException|ChildWorkflowFailedException|ChildWorkflowExpiredException|ChildWorkflowCancelledException $ending) {
                // All six are public classes a workflow can throw itself; only this run's seams end the pass.
                if (! $runtime->raised($ending)) {
                    throw $ending;
                }
            }

            return $runtime->sagaStack()->entries();
        } finally {
            $runtime->endCollecting();
            $runtime->clear();
        }
    }

    /**
     * @throws Throwable
     */
    private function suspendOn(FlowSuspended $suspended, FlowRuntime $runtime, FlowRun $flowRun, RunMode $mode): FlowRun
    {
        $this->suspend($flowRun);

        if (
            $suspended->childRunId === null
            || ! app(ChildWorkflowManager::class)->childHasEnded($flowRun, $suspended->childRunId)
        ) {
            return $flowRun;
        }

        return $this->driveInner($runtime, $flowRun, $mode);
    }

    /**
     * @template TPass
     *
     * @param  callable(): TPass  $pass
     * @return TPass
     *
     * @throws Throwable
     */
    private function onWriter(FlowRun $flowRun, callable $pass): mixed
    {
        $connection = $flowRun->getConnection();

        $pinned = (bool) new ReflectionProperty($connection, 'readOnWriteConnection')->getValue($connection);

        $this->writerPasses[] = ['name' => $connection->getName(), 'joined' => []];

        try {
            $connection->useWriteConnectionWhenReading();

            return $pass();
        } finally {
            foreach (array_pop($this->writerPasses)['joined'] as $joined) {
                $joined->useWriteConnectionWhenReading(false);
            }

            $connection->useWriteConnectionWhenReading($pinned);
        }
    }

    /**
     * A connection opened under the name a pass reads from — a tenancy hook reconnecting it
     * mid-pass, say — reads from the writer until the outermost such pass ends.
     */
    public function joinWriterPass(Connection $connection): void
    {
        foreach ($this->writerPasses as $index => $pass) {
            if ($pass['name'] === $connection->getName()) {
                $connection->useWriteConnectionWhenReading();

                $this->writerPasses[$index]['joined'][] = $connection;

                return;
            }
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
     * @throws Throwable
     */
    private function failAndCompensate(
        FlowRuntime $runtime,
        FlowRun $flowRun,
        Throwable $exception,
        RunMode $mode
    ): FlowRun {
        // The history no longer matches the code, so a stack built from it cannot be trusted.
        if ($exception instanceof HistoryContractMismatchException) {
            return $this->failFlow($flowRun, $exception);
        }

        $entries = $runtime->sagaStack()->entries();

        if ($entries === []) {
            return $this->failFlow($flowRun, $exception);
        }

        $cause = $this->exceptionToArray($exception);

        $this->stateMachine->transition($flowRun->intendRollback(FlowStatus::Failed, $cause), FlowStatus::Cancelling);

        $this->compensationRecorder->started($flowRun);

        $this->sagaRunner->rollback($flowRun, $entries, $cause, $mode, FlowStatus::Failed);

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
     * Guarded apart from drive(): the sweep calls it directly, and one run taken by someone else
     * must not end the whole sweep. The re-entry refusal is raised here: the planning replay's own
     * wrapper would hand a predicate a planning fault to read as an ordinary failure.
     *
     * @throws Throwable
     */
    public function expireRun(FlowRun $flowRun): FlowRun
    {
        $this->rejectWhileDeciding();

        try {
            return $this->expireRunInner($flowRun);
        } catch (ConcurrentFlowTransitionException) {
            return $this->reread($flowRun);
        }
    }

    /**
     * A pruned row falls back to the caller's snapshot; a fenced write refuses anything acted on it.
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

        // Named: the sweep treats a plan that fails before the move apart from every other failure.
        try {
            $entries = $this->collectCompensations($flowRun);
        } catch (Throwable $planning) {
            throw ExpirationNotPlannedException::for($flowRun, $planning);
        }

        $this->stateMachine->transition($flowRun->intendRollback(FlowStatus::Expired, $primary), FlowStatus::Cancelling);

        $entries = $this->replanCompensations($flowRun, $entries);

        $this->sagaRunner->rollback($flowRun, $entries, $primary, RunMode::Queued, FlowStatus::Expired);

        return $flowRun;
    }

    /**
     * The takeover replaces the owner token the caller read, so of two recoverers one wins; the
     * earlier owner's late compensation claims are refused on the token. Returns whether it took over.
     *
     * @throws RollbackNotPlannedException the rollback was taken over but could not be planned
     * @throws Throwable
     */
    public function resumeRollback(FlowRun $flowRun): bool
    {
        $this->rejectWhileDeciding();

        $token = (string) Str::ulid();

        $taken = $flowRun->newQuery()
            ->whereKey($flowRun->getKey())
            ->where('status', FlowStatus::Cancelling)
            ->where('rollback_token', $flowRun->rollback_token)
            ->update(['rollback_token' => $token]) === 1;

        if (! $taken) {
            return false;
        }

        $owned = $this->reread($flowRun);

        try {
            $entries = $owned->rollback_compensates === false ? [] : $this->collectCompensations($owned);
        } catch (Throwable $planning) {
            throw RollbackNotPlannedException::for($owned, $planning);
        }

        try {
            $this->sagaRunner->rollback(
                $owned,
                $entries,
                $owned->rollback_cause,
                RunMode::Queued,
                $owned->rollback_target ?? FlowStatus::Cancelled,
            );
        } catch (ConcurrentFlowTransitionException) {
            // Finished by somebody else between the takeover and the landing.
        }

        return true;
    }

    private function isExpired(FlowRun $flowRun): bool
    {
        return (bool) config('saga-lara-flow.monitor.expiration.enabled')
            && $flowRun->expires_at !== null
            && $flowRun->expires_at->lessThanOrEqualTo(now());
    }
}
