<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowChildRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Data\ChildSchedule;
use DiscoveryUkraine\SagaLaraFlow\Data\SignalRetry;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowCancelledException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FencedWriteLost;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ParentClosedException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Retry\RecordedFailure;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Support\AttributeReader;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Foundation\Bus\PendingDispatch;
use Throwable;

readonly class ChildWorkflowManager
{
    public function __construct(
        private HistoryContractGuard $guard,
        private FlowChildRepository $children,
        private ChildRecorder $recorder,
        private FlowRepository $repository,
        private FlowSuspender $suspender,
        private Serializer $serializer,
        private FlowExecutor $executor,
        private StartWorkGuard $startWork,
        private AnomalyLog $anomalies,
        private AttributeReader $attributes,
        private SignalRepository $signals,
        private SignalRecorder $signalRecorder,
        private FlowResumer $resumer,
    ) {}

    /**
     * @throws HistoryContractMismatchException
     * @throws FlowSuspended
     * @throws ChildWorkflowFailedException
     * @throws ChildWorkflowExpiredException
     * @throws ChildWorkflowCancelledException
     * @throws Throwable
     */
    public function await(FlowRuntime $runtime, ChildSchedule $schedule): mixed
    {
        $parent = $runtime->run();
        $sequence = $runtime->nextSequence();

        $link = $this->guard->expectChild($parent->id, $sequence, $schedule->workflowClass);

        // A recorded child resolves the same way for both replays: what it already
        // came to is history, and a rollback has to be planned past it.
        if ($link !== null) {
            return $this->resolve($runtime, $link, $sequence, $schedule);
        }

        // Compensation-only planning stops at a child this run never started: that is
        // the live frontier, and the pass must not start one to find out.
        if ($runtime->isCollecting()) {
            $this->suspender->suspend('child', $sequence);
        }

        $child = $this->startChild($parent, $schedule, $sequence);

        $this->launch($runtime, $child, $sequence);
    }

    /**
     * A child's start can commit after its parent's close read the links. A pruned parent counts
     * as closed: only finished runs are pruned.
     */
    public function closedBeforeStart(FlowRun $child): bool
    {
        $policy = ChildClosePolicy::tryFrom((string) $child->parent_close_policy);

        if ($child->parent_id === null || $policy === null || $policy === ChildClosePolicy::Abandon) {
            return false;
        }

        $parent = $this->repository->findOnWriter($child->parent_id);

        if ($parent?->status->canStartWork()) {
            return false;
        }

        $closed = $parent === null
            ? ParentClosedException::forPruned($child->parent_id)
            : ParentClosedException::for($parent);

        if ($this->recorder->fence($child, $policy, false, $closed)) {
            $this->dispatch(CancelChildWorkflowJob::dispatch(
                $child->id,
                $policy === ChildClosePolicy::Fail ? FlowStatus::Failed : FlowStatus::Cancelled,
                $policy === ChildClosePolicy::Fail,
            ), $parent ?? $child);
        }

        return true;
    }

    public function onFlowFinalized(FlowRun $run, bool $withCompensation): void
    {
        if ($run->parent_id !== null) {
            $this->notifyParent($run);
        }

        if (in_array($run->status, [FlowStatus::Failed, FlowStatus::Cancelled, FlowStatus::Expired], true)) {
            $this->closeChildren($run, $withCompensation);
        }
    }

    /**
     * The compensation pass runs through here too, where a suspension would end the stack short.
     *
     * @throws FlowSuspended
     * @throws ChildWorkflowFailedException
     * @throws ChildWorkflowExpiredException
     * @throws ChildWorkflowCancelledException
     * @throws Throwable
     */
    private function resolve(FlowRuntime $runtime, FlowChild $link, int $sequence, ChildSchedule $schedule): mixed
    {
        if ($link->status === ChildStatus::AwaitingRetry) {
            return $this->resolveParked($runtime, $link, $sequence, $schedule);
        }

        $child = $link->child;

        return match ($child->status) {
            FlowStatus::Completed => $this->resolveCompleted($runtime, $child, $sequence, $schedule),
            FlowStatus::Failed, FlowStatus::Expired => $this->resolveFailure(
                $runtime,
                $link,
                $child,
                $sequence,
                $schedule,
            ),
            FlowStatus::Cancelled => throw $runtime->raising(
                ChildWorkflowCancelledException::for($child, $sequence),
            ),
            // Still in flight, Cancelling included: park until it finalizes.
            default => $this->suspender->suspendOnChild($sequence, $child->id),
        };
    }

    private function resolveCompleted(
        FlowRuntime $runtime,
        FlowRun $child,
        int $sequence,
        ChildSchedule $schedule,
    ): mixed {
        if ($schedule->compensation !== null) {
            $runtime->sagaStack()->push(new CompensationEntry(
                null,
                $sequence,
                $schedule->compensation,
                $schedule->compensationFailurePolicy,
            ));
        }

        return $this->serializer->deserialize($child->result);
    }

    /**
     * @throws FlowSuspended
     * @throws ChildWorkflowFailedException
     * @throws ChildWorkflowExpiredException
     * @throws Throwable
     */
    private function resolveFailure(
        FlowRuntime $runtime,
        FlowChild $link,
        FlowRun $child,
        int $sequence,
        ChildSchedule $schedule,
    ): mixed {
        if ($this->shouldPark($runtime, $link, $child, $sequence, $schedule->retry)) {
            $this->parkForRetry($runtime, $link, $child, $sequence, $schedule);
        }

        return $this->surfaceFailure($runtime, $child, $sequence, $schedule->continueParentOnFailure);
    }

    /**
     * @throws ChildWorkflowFailedException
     * @throws ChildWorkflowExpiredException
     */
    private function surfaceFailure(
        FlowRuntime $runtime,
        FlowRun $child,
        int $sequence,
        bool $continueParentOnFailure,
    ): null {
        if ($continueParentOnFailure) {
            return null;
        }

        throw $runtime->raising($child->status === FlowStatus::Expired
            ? ChildWorkflowExpiredException::for($child, $sequence)
            : ChildWorkflowFailedException::for($child, $sequence));
    }

    /**
     * The gates of ActionBuilder::shouldRetryOnSignal(), in its order.
     *
     * @throws InternalFlowControl
     */
    private function shouldPark(
        FlowRuntime $runtime,
        FlowChild $link,
        FlowRun $child,
        int $sequence,
        ?SignalRetry $retry,
    ): bool {
        // Planning resolves the failure as though there were no policy: the plan comes out
        // the same, and asking would run caller code that pass must not.
        if ($retry === null || $runtime->isCollecting()) {
            return false;
        }

        $cap = $this->budgetFor($link, $retry);

        if ($cap !== null && $link->retry_signal_attempts >= $cap) {
            return false;
        }

        if ($this->signals->latestForSequence($link->parent_flow_run_id, $sequence)?->status === SignalStatus::TimedOut) {
            return false;
        }

        $failure = RecordedFailure::fromRecord($child->exception);

        if (! $retry->matches($failure)) {
            return false;
        }

        // A parent that went on past this child already took the failure as its answer.
        if ($this->hasHistoryAfter($link->parent_flow_run_id, $sequence)) {
            return false;
        }

        $parent = $runtime->run();

        return $retry->allows($runtime, new RetryContext(
            runId: $parent->id,
            workflowClass: $parent->workflow_class,
            actionClass: $child->workflow_class,
            sequence: $sequence,
            signal: $retry->signal,
            cyclesSpent: $link->retry_signal_attempts,
            cap: $cap,
            executions: $link->retry_signal_attempts + 1,
            failure: $failure,
            childRunId: $child->id,
        ), ['child_flow_run_id' => $child->id]);
    }

    private function budgetFor(FlowChild $link, ?SignalRetry $retry): ?int
    {
        return $link->retry_signal === null
            ? $retry?->resolvedMaxRetries()
            : $link->retry_signal_max_attempts;
    }

    /**
     * Read from the writer: a replica that has not caught up would hide what this protects.
     */
    private function hasHistoryAfter(string $parentId, int $sequence): bool
    {
        return array_any([
            ['action_run', 'flow_run_id', 'sequence'],
            ['flow_child', 'parent_flow_run_id', 'sequence'],
            ['side_effect', 'flow_run_id', 'sequence'],
            ['flow_signal', 'flow_run_id', 'wait_sequence'],
        ], function (array $source) use ($parentId, $sequence): bool {
            [$model, $run, $ordinal] = $source;

            /** @var class-string<Model> $class */
            $class = config("saga-lara-flow.models.{$model}");

            return $class::query()
                ->useWritePdo()
                ->where($run, $parentId)
                ->where($ordinal, '>', $sequence)
                ->exists();
        });
    }

    /**
     * The wait-signal and the link are written together, so neither outlives the other.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function parkForRetry(
        FlowRuntime $runtime,
        FlowChild $link,
        FlowRun $child,
        int $sequence,
        ChildSchedule $schedule,
    ): never {
        /** @var SignalRetry $retry */
        $retry = $schedule->retry;
        $parent = $runtime->run();

        // A floating signal older than the failed attempt belongs to something else.
        $delivered = $this->signals->earliestPendingSince($parent->id, $retry->signal, $child->finished_at);

        if ($delivered !== null) {
            $this->retry($runtime, $link, $sequence, $schedule, $retry->signal, function () use (
                $parent,
                $delivered,
                $sequence,
            ): bool {
                $this->signalRecorder->consumeSignal($parent, $delivered, $sequence);

                return true;
            });
        }

        try {
            $parent->getConnection()->transaction(function () use ($parent, $link, $child, $sequence, $retry): void {
                $this->signalRecorder->recordSignalWaiting(
                    $parent,
                    $retry->signal,
                    $sequence,
                    $retry->waitDeadline(),
                );

                if (! $this->recorder->awaitRetry($link, $child, $retry->signal, $this->budgetFor($link, $retry))) {
                    throw new FencedWriteLost;
                }
            });
        } catch (FencedWriteLost) {
            // Nothing was written; whatever moved the link is read on the next replay.
            $this->suspender->suspend('child', $sequence);
        }

        if ($this->parkSurvivedCommit($parent, $child, $sequence)) {
            $this->recorder->childAwaitingRetry($child, $retry->signal);
        }

        $this->suspender->suspend('child', $sequence);
    }

    private function parkSurvivedCommit(FlowRun $parent, FlowRun $child, int $sequence): bool
    {
        /** @var class-string<FlowChild> $model */
        $model = config('saga-lara-flow.models.flow_child');

        $parked = $model::query()
            ->useWritePdo()
            ->where('parent_flow_run_id', $parent->id)
            ->where('child_flow_run_id', $child->id)
            ->where('sequence', $sequence)
            ->where('status', ChildStatus::AwaitingRetry)
            ->exists();

        if (! $parked) {
            $this->anomalies->log(AnomalyLog::REASON_CLAIM_NOT_COMMITTED, [
                'entity' => 'child',
                'site' => 'await_retry',
                'flow_run_id' => $parent->id,
                'child_flow_run_id' => $child->id,
                'sequence' => $sequence,
                'child_workflow_class' => $child->workflow_class,
            ]);
        }

        return $parked;
    }

    /**
     * The link, not the builder, says what the parent waits on, whatever a later deploy did
     * to the child() call.
     *
     * @throws FlowSuspended
     * @throws ChildWorkflowFailedException
     * @throws ChildWorkflowExpiredException
     * @throws Throwable
     */
    private function resolveParked(
        FlowRuntime $runtime,
        FlowChild $link,
        int $sequence,
        ChildSchedule $schedule,
    ): mixed {
        // The parked ordinal is the live frontier: planning stops here.
        if ($runtime->isCollecting()) {
            $this->suspender->suspend('child', $sequence);
        }

        $parent = $runtime->run();
        $child = $link->child;
        $signal = (string) $link->retry_signal;
        $wait = $this->signals->latestForSequence($parent->id, $sequence);

        if ($wait?->status === SignalStatus::TimedOut) {
            if (! $this->recorder->settleAwaitingRetry($link, $child)) {
                $this->suspender->suspend('child', $sequence);
            }

            return $this->surfaceFailure($runtime, $child, $sequence, $schedule->continueParentOnFailure);
        }

        if ($wait?->status === SignalStatus::Received) {
            $this->retry($runtime, $link, $sequence, $schedule, $signal, function () use (
                $parent,
                $wait,
                $sequence,
            ): bool {
                $this->signalRecorder->consumeSignal($parent, $wait, $sequence);

                return true;
            });
        }

        // A delivery that raced the parking found no wait-signal and floated instead.
        $delivered = $wait?->status === SignalStatus::Waiting
            ? $this->signals->earliestPendingSince($parent->id, $wait->name, $child->finished_at)
            : null;

        if ($delivered !== null) {
            $this->retry($runtime, $link, $sequence, $schedule, $signal, function () use (
                $parent,
                $wait,
                $delivered,
                $sequence,
            ): bool {
                if (! $this->signalRecorder->consumeWhileWaiting($parent, $wait, $sequence)) {
                    return false;
                }

                $this->signalRecorder->consumeSignal($parent, $delivered, $sequence);

                return true;
            });
        }

        $this->suspender->suspend('child', $sequence);
    }

    /**
     * One transaction: no delivery is spent without its attempt, and no run exists that the
     * link does not own.
     *
     * @param  Closure(): bool  $consume
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function retry(
        FlowRuntime $runtime,
        FlowChild $link,
        int $sequence,
        ChildSchedule $schedule,
        string $signal,
        Closure $consume,
    ): never {
        $parent = $runtime->run();
        $previous = $link->child;
        $cap = $this->budgetFor($link, $schedule->retry);

        try {
            $next = $parent->getConnection()->transaction(function () use (
                $parent,
                $link,
                $previous,
                $schedule,
                $signal,
                $cap,
                $consume,
            ): FlowRun {
                if (! $consume()) {
                    throw new FencedWriteLost;
                }

                // The attempt it replaces was given these; the builder's are replayed now.
                $next = $this->createChild($parent, $schedule, $previous->arguments);

                if (! $this->recorder->retryChild($link, $previous, $next, $signal, $cap)) {
                    throw new FencedWriteLost;
                }

                return $next;
            });
        } catch (FencedWriteLost) {
            // Nothing was written; the next replay reads where things stand.
            $this->suspender->suspend('child', $sequence);
        }

        $this->requireStartSurvivedCommit($parent, $next, $sequence);

        $this->recorder->childRetried($parent, $previous, $next);

        $this->launch($runtime, $next, $sequence);
    }

    /**
     * @throws FlowSuspended
     */
    private function launch(FlowRuntime $runtime, FlowRun $child, int $sequence): never
    {
        if ($runtime->mode() === RunMode::Sync) {
            $driven = $this->executor->drive($child, RunMode::Sync);

            // The child finished inline: replay the parent so the seam resolves it.
            // Otherwise it parked on a genuine external wait — the parent waits too.
            if ($driven->isTerminal()) {
                $this->suspender->suspendInline('child', $sequence);
            }

            $this->suspender->suspendOnChild($sequence, $child->id);
        }

        $this->dispatch(RunWorkflowJob::dispatch($child->id), $child);

        $this->suspender->suspendOnChild($sequence, $child->id);
    }

    /**
     * The parent is asked inside the transaction of the two writes; the host hears of the child,
     * and its job goes out, only once that transaction is on record.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function startChild(FlowRun $parent, ChildSchedule $schedule, int $sequence): FlowRun
    {
        $child = $parent->getConnection()->transaction(function () use ($parent, $schedule, $sequence): FlowRun {
            $this->startWork->expect($parent, 'child', $sequence);

            $child = $this->createChild($parent, $schedule, $this->serializer->serialize($schedule->arguments));

            $this->recorder->startChild(
                $parent,
                $child,
                $sequence,
                $schedule->closePolicy,
                $schedule->continueParentOnFailure,
            );

            return $child;
        });

        $this->requireStartSurvivedCommit($parent, $child, $sequence);

        $this->recorder->childStarted($parent, $child);

        return $child;
    }

    /**
     * On PostgreSQL a failure swallowed inside the transaction turns its COMMIT into a rollback.
     *
     * @throws FlowSuspended
     */
    private function requireStartSurvivedCommit(FlowRun $parent, FlowRun $child, int $sequence): void
    {
        /** @var class-string<FlowChild> $model */
        $model = config('saga-lara-flow.models.flow_child');

        $onRecord = $model::query()
            ->useWritePdo()
            ->where('parent_flow_run_id', $parent->id)
            ->where('child_flow_run_id', $child->id)
            ->where('sequence', $sequence)
            ->exists();

        if ($onRecord) {
            return;
        }

        $this->anomalies->log(AnomalyLog::REASON_CLAIM_NOT_COMMITTED, [
            'entity' => 'child',
            'flow_run_id' => $parent->id,
            'child_flow_run_id' => $child->id,
            'sequence' => $sequence,
            'child_workflow_class' => $child->workflow_class,
        ]);

        $this->suspender->suspend('child', $sequence);
    }

    private function createChild(FlowRun $parent, ChildSchedule $schedule, mixed $arguments): FlowRun
    {
        $attributes = $this->attributes->workflow($schedule->workflowClass);

        return $this->repository->create([
            'workflow_class' => $schedule->workflowClass,
            'workflow_name' => $attributes->name,
            'workflow_version' => $attributes->version,
            'status' => FlowStatus::Pending,
            'arguments' => $arguments,
            'parent_id' => $parent->id,
            'parent_close_policy' => $schedule->closePolicy->value,
            'connection' => $attributes->connectionWithin($parent->connection),
            'queue' => $attributes->queueWithin($parent->queue),
            'expires_at' => $schedule->expiresAt ?? $attributes->expiresAt(),
            'tenancy_context' => $parent->tenancy_context,
        ], $attributes->tagsWith($schedule->tags));
    }

    /**
     * Wakes the parent by replay when this process drives it, otherwise once it is Waiting;
     * childHasEnded() is the other half.
     */
    private function notifyParent(FlowRun $run): void
    {
        $link = $this->linkFor($run);

        if ($link === null) {
            return;
        }

        match ($run->status) {
            FlowStatus::Completed => $this->recorder->recordCompleted($link, $run),
            FlowStatus::Failed => $this->recorder->recordFailed($link, $run),
            FlowStatus::Expired => $this->recorder->recordExpired($link, $run),
            FlowStatus::Cancelled => $this->recorder->recordCancelled($link, $run),
            default => null,
        };

        if ($this->executor->replayIfDriving((string) $run->parent_id)) {
            return;
        }

        $parent = $run->newQuery()->useWritePdo()->find($run->parent_id);

        if ($parent?->status === FlowStatus::Waiting) {
            $this->resumer->resume($parent);
        }
    }

    public function childHasEnded(FlowRun $parent, string $childRunId): bool
    {
        /** @var ?FlowStatus $status */
        $status = $parent->newQuery()->useWritePdo()->whereKey($childRunId)->value('status');

        return $status?->isTerminal() ?? false;
    }

    private function closeChildren(FlowRun $parent, bool $withCompensation): void
    {
        foreach ($this->children->active($parent->id) as $link) {
            match ($link->close_policy) {
                ChildClosePolicy::Abandon => null,
                ChildClosePolicy::Cancel => $this->dispatch(
                    CancelChildWorkflowJob::dispatch(
                        $link->child_flow_run_id,
                        FlowStatus::Cancelled,
                        $withCompensation
                    ),
                    $parent,
                ),
                ChildClosePolicy::Fail => $this->dispatch(
                    CancelChildWorkflowJob::dispatch(
                        $link->child_flow_run_id,
                        FlowStatus::Failed,
                        true
                    ),
                    $parent,
                ),
            };
        }
    }

    private function linkFor(FlowRun $run): ?FlowChild
    {
        /** @var class-string<FlowChild> $model */
        $model = config('saga-lara-flow.models.flow_child');

        return $model::query()
            ->useWritePdo()
            ->with(['parent' => fn (Relation $parent) => $parent->useWritePdo()])
            ->where('parent_flow_run_id', $run->parent_id)
            ->where('child_flow_run_id', $run->id)
            ->first();
    }

    private function dispatch(PendingDispatch $dispatch, FlowRun $flowRun): void
    {
        if ($flowRun->connection !== null) {
            $dispatch->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $dispatch->onQueue($flowRun->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $dispatch->afterCommit();
        }
    }
}
