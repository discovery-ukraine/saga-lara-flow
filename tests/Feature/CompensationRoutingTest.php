<?php

use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowQueue;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailedStepWithCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;

/**
 * A rollback's jobs go where every other job of the run goes. The worker here listens only
 * on the queues the runs are routed to, so a compensation sent anywhere else is never
 * picked up and the run stays in Cancelling.
 */
beforeEach(function (): void {
    CompensationLog::reset();
});

#[FlowQueue(queue: 'heavy')]
final class RoutedFailingChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'c')->compensateWith(UndoAction::class, 'c')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

final class ParentOfRoutedFailingChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(RoutedFailingChildWorkflow::class)->continueParentOnFailure()->run();
    }
}

function drainOnly(string $queues): void
{
    Artisan::call('queue:work', [
        '--queue' => $queues,
        '--stop-when-empty' => true,
        '--sleep' => 0,
        '--memory' => 4096,
        '--no-interaction' => true,
    ]);
}

it('rolls a routed root back on its own queue', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(FailedStepWithCompensationWorkflow::class)->onQueue('routed')->run();
    drainOnly('routed');

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:a'])
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

it('rolls a child back on the queue its class routes it to', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(ParentOfRoutedFailingChildWorkflow::class)->onQueue('parent-queue')->run();
    drainOnly('parent-queue,heavy');

    $child = FlowRun::query()->where('parent_id', $run->id)->firstOrFail();

    expect($child->queue)->toBe('heavy')
        ->and($child->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});
