<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowChildRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowCancelledException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
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
    ) {}

    /**
     * Resolve a child workflow for the current run: replay it, park on it, or start
     * and suspend.
     *
     * @param  array<int, mixed>  $arguments
     *
     * @throws HistoryContractMismatchException
     * @throws FlowSuspended
     * @throws ChildWorkflowFailedException
     * @throws ChildWorkflowCancelledException
     * @throws Throwable
     */
    public function await(
        FlowRuntime $runtime,
        string $workflowClass,
        array $arguments,
        ChildClosePolicy $closePolicy,
        bool $continueParentOnFailure,
    ): mixed {
        $parent = $runtime->run();
        $sequence = $runtime->nextSequence();

        $link = $this->guard->expectChild($parent->id, $sequence, $workflowClass);

        // Compensation-only planning: replay a completed child, otherwise stop here
        // (an unfinished child is the live frontier).
        if ($runtime->isCollecting()) {
            if ($link !== null && $link->child->status === FlowStatus::Completed) {
                return $this->serializer->deserialize($link->child->result);
            }

            $this->suspender->suspend('child', $sequence);
        }

        if ($link !== null) {
            return $this->resolve($link, $continueParentOnFailure, $sequence);
        }

        // First encounter: create and start the child, then suspend the parent.
        $child = $this->createChild($parent, $workflowClass, $arguments, $closePolicy);

        $this->recorder->startChild($parent, $child, $sequence, $closePolicy, $continueParentOnFailure);

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
     * @throws FlowSuspended
     * @throws ChildWorkflowFailedException
     * @throws ChildWorkflowCancelledException
     */
    private function resolve(FlowChild $link, bool $continueParentOnFailure, int $sequence): mixed
    {
        $child = $link->child;

        return match ($child->status) {
            FlowStatus::Completed => $this->serializer->deserialize($child->result),
            FlowStatus::Failed => $continueParentOnFailure
                ? null
                : throw ChildWorkflowFailedException::for($child, $sequence),
            FlowStatus::Cancelled => throw ChildWorkflowCancelledException::for($child, $sequence),
            // Still in flight (Pending/Running/Waiting): park until it finalizes.
            default => $this->suspender->suspend('child', $sequence),
        };
    }

    /**
     * @param  array<int, mixed>  $arguments
     */
    private function createChild(
        FlowRun $parent,
        string $workflowClass,
        array $arguments,
        ChildClosePolicy $closePolicy,
    ): FlowRun {
        return $this->repository->create([
            'workflow_class' => $workflowClass,
            'status' => FlowStatus::Pending,
            'arguments' => $this->serializer->serialize($arguments),
            'parent_id' => $parent->id,
            'parent_close_policy' => $closePolicy->value,
            'connection' => $parent->connection,
            'queue' => $parent->queue,
            'tenancy_context' => $parent->tenancy_context,
        ]);
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
