<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowCancelled;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowStarted;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

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
    ) {}

    /**
     * Record the link for a newly started child and append the child.started event.
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

        event(new ChildWorkflowStarted($parent, $child));

        return $link;
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

    public function recordCancelled(FlowChild $link, FlowRun $child): void
    {
        $this->transition($link, ChildStatus::Cancelled, FlowEventType::ChildCancelled, $child);

        event(new ChildWorkflowCancelled($child));
    }

    private function transition(FlowChild $link, ChildStatus $status, FlowEventType $eventType, FlowRun $child): void
    {
        $link->status = $status;
        $link->save();

        $parent = $link->parent;

        $this->events->record($parent, $eventType, $link->sequence, $child, [
            'child_workflow_class' => $link->child_workflow_class,
        ]);
    }
}
