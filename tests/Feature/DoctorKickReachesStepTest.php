<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelEchoWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TwoStepWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UnsavedFlowRun;
use Illuminate\Support\Facades\DB;

/**
 * Drive a workflow far enough to schedule its first step, then take away everything
 * that would ever run it again: the queued job is gone, the step is old enough to be
 * a repair candidate, and its repair budget is spent. This is the state the doctor
 * has given up on for good.
 *
 * @param  list<mixed>  $arguments
 */
function kickReachStageStuckRun(string $workflow, array $arguments = []): FlowRun
{
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = app(FlowRepository::class)->create([
        'workflow_class' => $workflow,
        'status' => FlowStatus::Pending,
        'arguments' => app(Serializer::class)->serialize($arguments),
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    expect($driven->status)->toBe(FlowStatus::Waiting);

    // The lost job: nothing on the queue can move this run on any more.
    DB::connection('testing')->table('jobs')->delete();

    ActionRun::query()->where('flow_run_id', $driven->id)->update([
        'created_at' => now()->subHours(2),
        'repair_attempts' => (int) config('saga-lara-flow.repair.max_attempts'),
        'repair_available_at' => null,
    ]);

    return $driven;
}

function kickReachJobCount(): int
{
    return DB::connection('testing')->table('jobs')->count();
}

it('runs a sequential step the doctor has given up on', function () {
    $run = kickReachStageStuckRun(TwoStepWorkflow::class, ['order-1']);

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(0)
        ->and(kickReachJobCount())->toBe(0);

    app(FlowDoctor::class)->kick($run);

    drainQueue();

    $final = SagaFlow::findRun($run->id);
    $steps = $final->actions()->orderBy('sequence')->get();

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($steps)->toHaveCount(2)
        ->and($steps[0]->status)->toBe(ActionStatus::Completed)
        ->and($steps[0]->repair_attempts)->toBe(0)
        ->and($steps[1]->status)->toBe(ActionStatus::Completed);
});

it('sends the step job for the retry cycle the row is on', function () {
    $run = kickReachStageStuckRun(OneActionWorkflow::class);

    // The row was rewound by a retry cycle before it got stuck. A job sent for cycle 0
    // loses the claim against it, so the step only runs if the kick carries the
    // generation the row actually carries.
    ActionRun::query()->where('flow_run_id', $run->id)->update(['retry_signal_attempts' => 1]);

    app(FlowDoctor::class)->kick($run);

    drainQueue();

    $step = SagaFlow::findRun($run->id)->actions()->first();

    expect($step->status)->toBe(ActionStatus::Completed)
        ->and($step->retry_signal_attempts)->toBe(1);
});

it('clears the repair budget of a parallel step without sending its job', function () {
    $run = kickReachStageStuckRun(ParallelEchoWorkflow::class);

    $before = ActionRun::query()->where('flow_run_id', $run->id)->get();

    expect($before)->toHaveCount(3)
        ->and($before->every(fn (ActionRun $step): bool => $step->parallel_group !== null))->toBeTrue();

    app(FlowDoctor::class)->kick($run);

    // Only the resume: a batch-bound step cannot be put back on its batch.
    expect(kickReachJobCount())->toBe(1);

    $after = ActionRun::query()->where('flow_run_id', $run->id)->get();

    expect($after->every(fn (ActionRun $step): bool => $step->repair_attempts === 0))->toBeTrue()
        ->and($after->every(fn (ActionRun $step): bool => $step->status === ActionStatus::Pending))->toBeTrue();
});

it('clears the run own repair budget so the doctor can wake it again', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => app(Serializer::class)->serialize([]),
    ]);

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Completed,
        'result' => ['label' => 'value'],
        'attempts' => 1,
    ]);

    FlowRun::query()->whereKey($run->id)->update([
        'updated_at' => now()->subHours(2),
        'repair_attempts' => (int) config('saga-lara-flow.repair.max_attempts'),
        'repair_available_at' => null,
    ]);

    expect(app(FlowDoctor::class)->repair()->rewokenFlows)->toBe(0);

    app(FlowDoctor::class)->kick($run->fresh());

    expect(SagaFlow::findRun($run->id)->repair_attempts)->toBe(0);
});

it('leaves a Running step inside its reclaim window alone', function () {
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.after_seconds', 900);

    $run = kickReachStageStuckRun(OneActionWorkflow::class);

    // A live worker holds the row: the claim resolved its reclaim window into a
    // deadline that has not passed.
    $step = ActionRun::query()->where('flow_run_id', $run->id)->firstOrFail();

    expect(app(ActionRecorder::class)->startAction($step))->toBeTrue();

    $step->refresh();

    expect($step->status)->toBe(ActionStatus::Running)
        ->and($step->reclaim_stale_at)->not->toBeNull()
        ->and($step->reclaim_stale_at->isFuture())->toBeTrue();

    DB::connection('testing')->table('jobs')->delete();

    app(FlowDoctor::class)->kick($run);

    // Only the resume: the row is still the live worker's to finish.
    expect(kickReachJobCount())->toBe(1)
        ->and($step->fresh()->repair_attempts)->toBe(0);
});

it('sends a Running step its job back once its reclaim window has passed', function () {
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.after_seconds', 900);

    $run = kickReachStageStuckRun(OneActionWorkflow::class);

    $step = ActionRun::query()->where('flow_run_id', $run->id)->firstOrFail();

    expect(app(ActionRecorder::class)->startAction($step))->toBeTrue();

    DB::connection('testing')->table('jobs')->delete();

    $this->travel(1800)->seconds();

    app(FlowDoctor::class)->kick($run);

    expect(kickReachJobCount())->toBe(2);

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('answers with the budget the database holds, not the one it wrote', function () {
    config()->set('saga-lara-flow.models.flow_run', UnsavedFlowRun::class);

    $run = kickReachStageStuckRun(OneActionWorkflow::class);

    FlowRun::query()->whereKey($run->id)->update([
        'repair_attempts' => (int) config('saga-lara-flow.repair.max_attempts'),
    ]);

    $log = base_path('kick-budget.log');
    logToFile($log);

    // The re-wake event runs host listeners inside the kick's transaction, and one that
    // swallows a failing query turns the commit into a rollback on PostgreSQL. The
    // caller is then handed a model claiming a budget the database never took.
    UnsavedFlowRun::$swallowSaves = true;

    try {
        $kicked = app(FlowDoctor::class)->kick($run);
    } finally {
        UnsavedFlowRun::reset();
    }

    $stored = SagaFlow::findRun($run->id);

    expect($stored->repair_attempts)->toBe((int) config('saga-lara-flow.repair.max_attempts'))
        ->and($kicked->repair_attempts)->toBe($stored->repair_attempts)
        ->and(file_get_contents($log))->toContain('claim_not_committed');
});
