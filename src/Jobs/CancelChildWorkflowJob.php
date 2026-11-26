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
 * $rollbackToken is fixed at dispatch, so a retry of the same payload knows the child's rollback
 * as its own; a job queued before it existed takes its payload's id, which a redelivery carries.
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
            // Quiet only for a child that is closed or rolling back: a live one is still owed this
            // close.
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
        $retry = $child->status === FlowStatus::Cancelling;

        $withCompensation = $retry ? $child->rollback_compensates !== false : $this->withCompensation;

        $cause = $retry ? $child->rollback_cause : $this->cause($repository, $child);

        // Before the tenant is entered, so a close that fails from here on stays recorded.
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
                $entries = $executor->collectCompensations($child);

                // The whole close runs in this one job: a queue's hard timeout can cut a long one
                // short, and the redelivery resumes it.
                $sagaRunner->rollback($child, $entries, $cause, RunMode::Sync, $this->finalState);

                return;
            }

            $this->finalizeWithoutCompensation($child, $lifecycle, $cause);
        });
    }

    /**
     * A move lost to the child stepping between live states is made again; null when the child
     * is closed or rolling back.
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
     * Guarded on the fence token, so of two close jobs one owns the close, and a recovery that took
     * the rollback over since is left alone.
     */
    private function claimedFence(FlowRun $child): bool
    {
        return $child->newQuery()
            ->whereKey($child->getKey())
            ->where('rollback_token', $child->parentFenceToken())
            ->update(['rollback_token' => $this->rollbackToken]) === 1;
    }

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
