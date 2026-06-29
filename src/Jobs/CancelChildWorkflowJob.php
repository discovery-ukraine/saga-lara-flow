<?php

namespace DiscoveryUkraine\SagaLaraFlow\Jobs;

use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ParentClosedException;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildWorkflowManager;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowLifecycleRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SagaRunner;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Throwable;

/**
 * Closes a still-active child workflow because its parent became terminal under a
 * Cancel or Fail close policy. Lands the child in $finalState (Cancelled or Failed),
 * optionally rolling back the child's completed steps first ($withCompensation).
 * The final transition fires onFlowFinalized() via the runner/recorder chain, so a
 * Fail/Cancel cascades recursively to the child's own children. Idempotent.
 */
class CancelChildWorkflowJob implements ShouldQueue
{
    use NormalizesExceptions;
    use Queueable;

    public function __construct(
        public string $childFlowRunId,
        public FlowStatus $finalState,
        public bool $withCompensation,
    ) {}

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
        $child = $repository->find($this->childFlowRunId);

        if ($child === null || $child->isTerminal()) {
            return;
        }

        $tenancy->restore($child);

        // Take control of the child from any non-terminal state (Pending/Running/
        // Waiting) — the state machine allows each into Cancelling.
        $stateMachine->transition($child, FlowStatus::Cancelling);

        $cause = $this->cause($repository, $child);

        if ($this->withCompensation) {
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

        $parent = $repository->find($child->parent_id);

        if ($parent === null) {
            return null;
        }

        return $this->exceptionToArray(ParentClosedException::for($parent));
    }
}
