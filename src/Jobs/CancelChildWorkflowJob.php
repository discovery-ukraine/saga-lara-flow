<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ParentClosedException;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildWorkflowManager;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowLifecycleRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SagaRunner;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Str;
use Symfony\Component\Uid\Ulid;
use Throwable;

/**
 * Closes a still-active child workflow because its parent became terminal under a
 * Cancel or Fail close policy. Lands the child in $finalState (Cancelled or Failed),
 * optionally rolling back the child's completed steps first ($withCompensation).
 * The final transition fires onFlowFinalized() via the runner/recorder chain, so a
 * Fail/Cancel cascades recursively to the child's own children. Idempotent.
 *
 * $rollbackToken is fixed when the job is queued, so a retry of the same payload knows the
 * child's rollback as its own. A job queued before it existed unserializes with null and
 * takes the id the queue gave its payload instead, which a redelivery carries too.
 *
 * A child its parent fenced is already in Cancelling with the close it is owed; the first
 * close job claims that fence for its own token and carries out the close the row records.
 * One that reaches a child nothing fenced fences it first, then plans. A close that fails once
 * the child is fenced leaves it in Cancelling with that close recorded, for recovery.
 */
class CancelChildWorkflowJob implements ShouldQueue
{
    use NormalizesExceptions;
    use Queueable;

    public ?string $rollbackToken = null;

    public function __construct(
        public string $childFlowRunId,
        public FlowStatus $finalState,
        public bool $withCompensation,
    ) {
        $this->rollbackToken = (string) Str::ulid();
    }

    /**
     * @throws Throwable
     */
    public function handle(
        FlowExecutor $executor,
        SagaRunner $sagaRunner,
        FlowRepository $repository,
        StateMachine $stateMachine,
        FlowLifecycleRecorder $lifecycle,
        TenancyManager $tenancy,
    ): void {
        $payload = $this->job?->uuid();

        $this->rollbackToken ??= $payload === null ? (string) Str::ulid() : (string) Ulid::fromString($payload);

        $child = $repository->findOnWriter($this->childFlowRunId);

        if ($child !== null && $this->claimedFence($child)) {
            $child = $repository->findOnWriter($this->childFlowRunId);
        }

        if ($child === null || $child->isTerminal() || $this->ownedByAnother($child)) {
            return;
        }

        try {
            $this->close($executor, $sagaRunner, $repository, $stateMachine, $lifecycle, $tenancy, $child);
        } catch (ConcurrentFlowTransitionException $lost) {
            // The per-run lock covers jobs and nothing else, so losing the child to an
            // operator or to a sync drive under the parent's own replay is an ordinary
            // race. Ending quietly is only right once the child is genuinely closed, or
            // once another rollback owns its landing — a child still live is one this
            // job was sent to close, and dropping it here would strand it with a
            // terminal parent and no close policy left to apply. One that moved into
            // Cancelling meanwhile is rolling back under its own owner, and lands there.
            $current = $repository->findOnWriter($this->childFlowRunId);

            if ($current !== null && ! $current->isTerminal() && $current->status !== FlowStatus::Cancelling) {
                throw $lost;
            }
        }
    }

    /**
     * @throws Throwable
     */
    private function close(
        FlowExecutor $executor,
        SagaRunner $sagaRunner,
        FlowRepository $repository,
        StateMachine $stateMachine,
        FlowLifecycleRecorder $lifecycle,
        TenancyManager $tenancy,
        FlowRun $child,
    ): void {
        // A retry of this job, or a close its parent fenced, finds the child already in
        // Cancelling under this job's token and rolls back as the row records.
        $retry = $child->status === FlowStatus::Cancelling;

        $withCompensation = $retry ? $child->rollback_compensates !== false : $this->withCompensation;

        $cause = $retry ? $child->rollback_cause : $this->cause($repository, $child);

        // Fenced before the child's tenant is entered, so a close that fails from here on
        // leaves the child in Cancelling with its close recorded.
        if (! $retry) {
            $child = $this->fence($stateMachine, $repository, $child, $cause, $withCompensation);

            if ($child === null) {
                return;
            }
        }

        $tenancy->for($child, $child->workflow_class, function () use (
            $executor,
            $sagaRunner,
            $lifecycle,
            $child,
            $withCompensation,
            $cause,
        ): void {
            if ($withCompensation) {
                // Planned with the child fenced: no drive() leaves Cancelling and no step is
                // claimed under it, so no other compensatable step can complete meanwhile.
                $entries = $executor->collectCompensations($child);

                // Roll back inline (Sync), inside THIS job, rather than dispatching another
                // queued Bus::batch: we are already on a worker, we just collected the stack
                // synchronously, and a single in-process pass keeps the finalize + the
                // recursive close of the child's own children deterministic within one job.
                //
                // Caveat (at-least-once): the whole child close — collect, every
                // compensation, finalize, and the recursive grandchild close — runs in one
                // worker iteration. On managed queues with a hard shutdown/visibility
                // timeout (e.g. SQS on Laravel Cloud, ~90s) a child with many or slow
                // compensations can exceed it, be force-terminated, and redelivered. The job
                // is idempotent (terminal child → early return; each CompensationRun is
                // resumable), so redelivery is safe — but compensations should be quick and
                // idempotent. If a close can be genuinely long, raise the timeouts or revisit
                // a queued (Bus::batch) rollback by threading a RunMode into this job.
                $sagaRunner->rollback($child, $entries, $cause, RunMode::Sync, $this->finalState);

                return;
            }

            $this->finalizeWithoutCompensation($child, $lifecycle, $cause);
        });
    }

    /**
     * Move a child nothing fenced into Cancelling with this close. A move lost to the child
     * stepping between live states is made again from where it now stands; one lost to a
     * closing or rolling-back child leaves it to that, and returns null.
     *
     * @param  array<string, mixed>|null  $cause
     *
     * @throws ConcurrentFlowTransitionException a move that lost although nothing moved the child
     */
    private function fence(
        StateMachine $stateMachine,
        FlowRepository $repository,
        FlowRun $child,
        ?array $cause,
        bool $withCompensation,
    ): ?FlowRun {
        while (true) {
            try {
                return $stateMachine->transition(
                    $child->intendRollback($this->finalState, $cause, $withCompensation, $this->rollbackToken),
                    FlowStatus::Cancelling,
                );
            } catch (ConcurrentFlowTransitionException $lost) {
                $current = $repository->findOnWriter($this->childFlowRunId);

                if ($current === null || ! $current->status->canStartWork()) {
                    return null;
                }

                if ($current->status === $child->status) {
                    throw $lost;
                }

                $child = $current;
            }
        }
    }

    /**
     * Take a parent's fence over for this job's token, guarded by the fence token itself, so
     * of two close jobs one owns the close and a recovery that took it since is left alone.
     */
    private function claimedFence(FlowRun $child): bool
    {
        return $child->newQuery()
            ->whereKey($child->getKey())
            ->where('rollback_token', $child->parentFenceToken())
            ->update(['rollback_token' => $this->rollbackToken]) === 1;
    }

    /**
     * A child already rolling back under another owner — its own failure, its expiry, an
     * operator, a recovery — is left to that rollback, which lands it where it intended.
     */
    private function ownedByAnother(FlowRun $child): bool
    {
        return $child->status === FlowStatus::Cancelling && $child->rollback_token !== $this->rollbackToken;
    }

    public function failed(Throwable $exception): void
    {
        app(AnomalyLog::class)->log(AnomalyLog::REASON_CLOSE_FAILED, [
            'entity' => 'flow',
            'flow_run_id' => $this->childFlowRunId,
            'final_state' => $this->finalState->value,
            'exception' => $this->exceptionToArray($exception),
        ]);
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return app(LockMiddlewareFactory::class)->workflowMiddleware($this->childFlowRunId);
    }

    /**
     * @param  array<string, mixed>|null  $cause
     */
    private function finalizeWithoutCompensation(FlowRun $child, FlowLifecycleRecorder $lifecycle, ?array $cause): void
    {
        if ($this->finalState === FlowStatus::Failed) {
            $child->markFailed($cause);

            $lifecycle->flowFailedFromArray($child, $cause);
        } else {
            $child->markCancelled();

            $lifecycle->flowCancelled($child);
        }

        app(ChildWorkflowManager::class)->onFlowFinalized($child, false);
    }

    /**
     * @return array<string, mixed>|null
     */
    private function cause(FlowRepository $repository, FlowRun $child): ?array
    {
        if ($this->finalState !== FlowStatus::Failed || $child->parent_id === null) {
            return null;
        }

        $parent = $repository->findOnWriter($child->parent_id);

        return $this->exceptionToArray($parent === null
            ? ParentClosedException::forPruned($child->parent_id)
            : ParentClosedException::for($parent));
    }
}
