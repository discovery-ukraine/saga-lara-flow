<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowAwaitingRetry;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowCancelled;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowExpired;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowRetried;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowStarted;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;

/**
 * Persists the child-workflow link lifecycle (started → completed/failed/cancelled)
 * and appends the matching child.* events to the PARENT's event log, so a child's
 * progress is visible from the parent's history at the child's sequence. Child link
 * rows are looked up by their (parent_flow_run_id, sequence) identity.
 */
final readonly class ChildRecorder
{
    public function __construct(
        private EventLog $events,
        private AnomalyLog $anomalies,
    ) {}

    /**
     * Record the link for a newly started child and append the child.started event. The
     * host hears about it from childStarted() below rather than here, so its listeners
     * run outside the transaction these two writes share.
     */
    public function startChild(
        FlowRun $parent,
        FlowRun $child,
        int $sequence,
        ChildClosePolicy $closePolicy,
        bool $continueParentOnFailure,
    ): FlowChild {
        /** @var class-string<FlowChild> $model */
        $model = config('saga-lara-flow.models.flow_child');

        $link = new $model;

        $link->fill([
            'parent_flow_run_id' => $parent->id,
            'child_flow_run_id' => $child->id,
            'sequence' => $sequence,
            'child_workflow_class' => $child->workflow_class,
            'close_policy' => $closePolicy,
            'continue_parent_on_failure' => $continueParentOnFailure,
            'status' => ChildStatus::Running,
        ]);

        $link->save();

        $this->events->record($parent, FlowEventType::ChildStarted, $sequence, $child, [
            'child_workflow_class' => $child->workflow_class,
        ]);

        return $link;
    }

    /**
     * Announce a child whose link is on record.
     */
    public function childStarted(FlowRun $parent, FlowRun $child): void
    {
        event(new ChildWorkflowStarted($parent, $child));
    }

    public function recordCompleted(FlowChild $link, FlowRun $child): void
    {
        $this->transition($link, ChildStatus::Completed, FlowEventType::ChildCompleted, $child);

        event(new ChildWorkflowCompleted($child));
    }

    public function recordFailed(FlowChild $link, FlowRun $child): void
    {
        $this->transition($link, ChildStatus::Failed, FlowEventType::ChildFailed, $child);

        event(new ChildWorkflowFailed($child));
    }

    public function recordExpired(FlowChild $link, FlowRun $child): void
    {
        $this->transition($link, ChildStatus::Expired, FlowEventType::ChildExpired, $child);

        event(new ChildWorkflowExpired($child));
    }

    public function recordCancelled(FlowChild $link, FlowRun $child): void
    {
        $this->transition($link, ChildStatus::Cancelled, FlowEventType::ChildCancelled, $child);

        event(new ChildWorkflowCancelled($child));
    }

    /**
     * Fenced as ActionRecorder::awaitRetry() is: on what the seam read, and on a live parent.
     */
    public function awaitRetry(FlowChild $link, FlowRun $child, string $signal, ?int $maxAttempts): bool
    {
        $asRead = $this->asRead($link);

        $link->status = ChildStatus::AwaitingRetry;
        $link->retry_signal = $signal;
        $link->retry_signal_max_attempts ??= $maxAttempts;

        if (! $this->writeFenced($link, $asRead, 'await_retry', FlowRun::live(...))) {
            return false;
        }

        $this->events->record($link->parent, FlowEventType::ChildAwaitingRetry, $link->sequence, $child, [
            'signal' => $signal,
            'retry_signal_attempts' => $link->retry_signal_attempts,
            'retry_signal_max_attempts' => $link->retry_signal_max_attempts,
        ]);

        return true;
    }

    public function childAwaitingRetry(FlowRun $child, string $signal): void
    {
        event(new ChildWorkflowAwaitingRetry($child, $signal));
    }

    /**
     * Fenced as awaitRetry() is, and on a parent that may still start work: this begins a run.
     */
    public function retryChild(
        FlowChild $link,
        FlowRun $previous,
        FlowRun $next,
        string $signal,
        ?int $maxAttempts,
    ): bool {
        $asRead = $this->asRead($link);

        $link->child_flow_run_id = $next->id;
        $link->status = ChildStatus::Running;
        $link->retry_signal ??= $signal;
        $link->retry_signal_max_attempts ??= $maxAttempts;
        $link->retry_signal_attempts = $link->retry_signal_attempts + 1;

        if (! $this->writeFenced($link, $asRead, 'retry_child', FlowRun::mayStartWork(...))) {
            return false;
        }

        $link->setRelation('child', $next);

        $this->events->record($link->parent, FlowEventType::ChildRetried, $link->sequence, $next, [
            'signal' => $link->retry_signal,
            'retry_signal_attempts' => $link->retry_signal_attempts,
            'retry_signal_max_attempts' => $link->retry_signal_max_attempts,
            'previous_child_flow_run_id' => $previous->id,
        ]);

        $this->events->record($link->parent, FlowEventType::ChildStarted, $link->sequence, $next, [
            'child_workflow_class' => $next->workflow_class,
        ]);

        return true;
    }

    public function childRetried(FlowRun $parent, FlowRun $previous, FlowRun $next): void
    {
        event(new ChildWorkflowRetried($next, $previous));

        $this->childStarted($parent, $next);
    }

    /**
     * No event: the attempt's own child.failed or child.expired stands, and the timed-out
     * wait-signal records the give-up.
     */
    public function settleAwaitingRetry(FlowChild $link, FlowRun $child): bool
    {
        $asRead = $this->asRead($link);

        $link->status = $this->statusOf($child);

        return $this->writeFenced($link, $asRead, 'settle_awaiting_retry', FlowRun::live(...));
    }

    /**
     * Same reasoning, and no event, as ActionRecorder::settleOpenSteps().
     */
    public function settleParked(FlowRun $parent): int
    {
        /** @var class-string<FlowChild> $model */
        $model = config('saga-lara-flow.models.flow_child');

        $parked = fn () => $model::query()
            ->where('parent_flow_run_id', $parent->id)
            ->where('status', ChildStatus::AwaitingRetry);

        return $parked()
            ->whereHas('child', fn (Builder $query) => $query->where('status', FlowStatus::Expired))
            ->update(['status' => ChildStatus::Expired])
            + $parked()->update(['status' => ChildStatus::Failed]);
    }

    /**
     * @return array<string, mixed>
     */
    private function asRead(FlowChild $link): array
    {
        return [
            'status' => $link->getOriginal('status'),
            'child_flow_run_id' => $link->getOriginal('child_flow_run_id'),
            'retry_signal_attempts' => $link->getOriginal('retry_signal_attempts'),
        ];
    }

    private function statusOf(FlowRun $child): ChildStatus
    {
        return $child->status === FlowStatus::Expired ? ChildStatus::Expired : ChildStatus::Failed;
    }

    /**
     * Every caller changes the status, so a row the condition matched is a row MySQL reports.
     *
     * @param  array<string, mixed>  $expected
     * @param  Closure(Builder<Model>): void  $parentFence
     */
    private function writeFenced(FlowChild $link, array $expected, string $site, Closure $parentFence): bool
    {
        $query = $link->newQuery()
            ->whereKey($link->getKey())
            ->whereHas('parent', $parentFence);

        foreach ($expected as $column => $value) {
            $query->where($column, $value);
        }

        if ($query->update($link->getDirty()) === 1) {
            $link->syncOriginal();

            return true;
        }

        $this->anomalies->log(AnomalyLog::REASON_WRITE_REFUSED, [
            'entity' => 'child',
            'site' => $site,
            'flow_run_id' => $link->parent_flow_run_id,
            'child_flow_run_id' => $link->getOriginal('child_flow_run_id'),
            'sequence' => $link->sequence,
            'child_workflow_class' => $link->child_workflow_class,
        ]);

        $link->discardChanges();

        return false;
    }

    /**
     * Only the attempt the link still points at, and not while parked: overwriting a parked
     * link would leave the parent on a wait-signal the next replay no longer finds.
     */
    private function transition(FlowChild $link, ChildStatus $status, FlowEventType $eventType, FlowRun $child): void
    {
        $link->newQuery()
            ->whereKey($link->getKey())
            ->where('child_flow_run_id', $child->id)
            ->where('status', '!=', ChildStatus::AwaitingRetry)
            ->update(['status' => $status]);

        $parent = $link->parent;

        $this->events->record($parent, $eventType, $link->sequence, $child, [
            'child_workflow_class' => $link->child_workflow_class,
        ]);
    }
}
