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
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Retry\RecordedFailure;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Support\AttributeReader;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Foundation\Bus\PendingDispatch;
use Throwable;

/**
 * The child()->run() seam and the parent/child lifecycle wiring. await() resolves
 * a child workflow by the parent's deterministic (flow_run_id, sequence) ordinal —
 * replaying a finished child, parking the parent while it runs, or starting it and
 * suspending — exactly like SignalWaiter does for signals.
 *
 * A child is a normal FlowRun with parent_id; it is driven by its own jobs. When a
 * run finalizes, onFlowFinalized() both notifies its parent (so the parent resumes
 * and replays the child()->run() seam) and applies its own children's close policy
 * when the run did not complete cleanly.
 */
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
    ) {}

    /**
     * Resolve a child workflow for the current run: replay it, park on it, or start
     * and suspend.
     *
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

        // First encounter: create and start the child, then suspend the parent.
        $child = $this->startChild($parent, $schedule, $sequence);

        $this->launch($runtime, $child, $sequence);
    }

    /**
     * Called whenever a run lands in a terminal state. Notifies the run's parent (so
     * it can resume) and, if the run did not complete cleanly, applies its children's
     * close policy. $withCompensation controls whether closed children roll back.
     */
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
     * Resolve a child already recorded against this ordinal. A terminal child has an
     * answer either way — a result, a failure or expiry the parent may be told to survive
     * or to retry, or a cancellation it cannot — and only one still in flight is a wait.
     * The compensation pass runs through here too, where a suspension would end the stack
     * short.
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
            default => $this->suspender->suspend('child', $sequence),
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

    /**
     * Whether the parking is on record now its transaction has closed, for the reason
     * requireStartSurvivedCommit() reads back a start.
     */
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

            $this->suspender->suspend('child', $sequence);
        }

        $this->dispatch(RunWorkflowJob::dispatch($child->id), $child);

        $this->suspender->suspend('child', $sequence);
    }

    /**
     * The parent is asked inside the same transaction as the two writes, so a check lost to
     * a rollback takes both down rather than leaving a FlowRun nobody owns. The host hears
     * about the child, and its job goes out, only once that transaction is on record.
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
     * Whether the link is on record now the transaction that wrote it has closed. Mirrors
     * ActionRecorder::claimSurvivedCommit(): a commit reporting success is not proof, since
     * a model observer on either row can run a failing query and swallow it, which on
     * PostgreSQL turns the eventual COMMIT into a rollback.
     *
     * Neither row survives such a rollback, so nothing is half-written and no child is
     * announced or driven that the writes did not keep. The ordinal is simply not reached
     * and the parent parks on one it comes back to — which without this read it would do
     * with nothing anywhere to say why.
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
        ], $attributes->tagsWith([]));
    }

    /**
     * Update this run's link in its parent and wake the parent if it is waiting.
     * The parent resume is skipped while it is still Running (sync inline drive),
     * where the seam resolves the child via replay instead.
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

        $parent = $this->repository->find((string) $run->parent_id);

        if ($parent !== null && $parent->status === FlowStatus::Waiting) {
            $this->dispatch(ResumeWorkflowJob::dispatch($parent->id), $parent);
        }
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

    /**
     * This run's link in its parent, looked up by the (parent, child) pair.
     */
    private function linkFor(FlowRun $run): ?FlowChild
    {
        /** @var class-string<FlowChild> $model */
        $model = config('saga-lara-flow.models.flow_child');

        return $model::query()
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
