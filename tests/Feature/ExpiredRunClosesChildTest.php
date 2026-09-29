<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * An expired run closes its children under their close policy with a rollback, whether or
 * not it had anything of its own to undo. With nothing of its own, the run skips Cancelling
 * and lands straight in Expired — which must not also spare its children the rollback.
 */
beforeEach(function (): void {
    CompensationLog::reset();
});

final class CompensatedWaitingChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'c')->compensateWith(UndoAction::class, 'c')->run();
        $this->awaitSignal('child.go');
    }
}

final class CancellingParentWithNothingToUndoWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(CompensatedWaitingChildWorkflow::class)->closePolicy(ChildClosePolicy::Cancel)->run();
    }
}

it('rolls back a child it cancels on expiring with nothing of its own to undo', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(CancellingParentWithNothingToUndoWorkflow::class)->run();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and($child->status)->toBe(FlowStatus::Waiting);

    $parent->newQuery()->toBase()->where('id', $parent->id)->update(['expires_at' => now()->subMinute()]);

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1);

    drainQueue();

    expect($parent->fresh()->status)->toBe(FlowStatus::Expired)
        ->and($child->fresh()->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:c']);
});
