<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use Illuminate\Support\Facades\DB;

/**
 * Stage a flow whose single sequential action was scheduled (Pending) but whose
 * RunActionJob was lost: drive once to schedule it, drop the queued jobs, then age
 * the action past the repair grace window.
 */
function stageStuckPendingAction(): ActionRun
{
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    expect($driven->status)->toBe(FlowStatus::Waiting);

    // Simulate the lost job: clear the queue so only the doctor can move things on.
    DB::connection('testing')->table('jobs')->delete();

    $action = $driven->actions()->first();

    expect($action->status)->toBe(ActionStatus::Pending);

    ActionRun::query()->whereKey($action->id)->update(['created_at' => now()->subMinutes(2)]);

    return $action->fresh();
}

it('re-dispatches a stuck sequential Pending action and lets it complete', function () {
    $action = stageStuckPendingAction();

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(1)
        ->and($report->rewokenFlows)->toBe(0);

    $action->refresh();

    expect($action->repair_attempts)->toBe(1)
        ->and($action->repair_available_at)->not->toBeNull()
        ->and($action->repair_available_at->isFuture())->toBeTrue()
        ->and(SagaFlow::findRun($action->flow_run_id)->events()
            ->where('type', FlowEventType::ActionRedispatched->value)->count())->toBe(1);

    drainQueue();

    $final = SagaFlow::findRun($action->flow_run_id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->actions()->first()->status)->toBe(ActionStatus::Completed);
});

it('does not re-dispatch the same action twice within its backoff window', function () {
    $action = stageStuckPendingAction();

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(1);

    // Second pass before repair_available_at: throttled, no further attempt.
    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(0)
        ->and($action->fresh()->repair_attempts)->toBe(1);
});
