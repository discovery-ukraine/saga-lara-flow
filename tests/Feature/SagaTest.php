<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ClosureCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensateFailedStepWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ContinuePolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailedStepWithCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ManualCompensateWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelSagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SagaRollbackWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\StopPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;

beforeEach(fn () => CompensationLog::reset());

it('rolls back completed compensations in reverse order on failure (sync)', function () {
    $run = SagaFlow::create(SagaRollbackWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['message'] ?? null)->toBe('boom')
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);

    $compensations = $run->compensations()->orderBy('sequence')->get();

    expect($compensations)->toHaveCount(2)
        ->and($compensations[0]->status)->toBe(CompensationStatus::Completed)
        ->and($compensations[1]->status)->toBe(CompensationStatus::Completed);
});

it('records compensation lifecycle events', function () {
    $run = SagaFlow::create(SagaRollbackWorkflow::class)->runSync();

    $types = $run->events()->pluck('type')->all();

    expect($types)->toContain(FlowEventType::CompensationStarted)
        ->and($types)->toContain(FlowEventType::CompensationCompleted)
        ->and($types)->toContain(FlowEventType::FlowFailed);
});

it('runs a closure-based compensation on rollback', function () {
    $run = SagaFlow::create(ClosureCompensationWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['closure-undo:x']);

    $compensation = $run->compensations()->first();

    expect($compensation->compensation_type)->toBe('closure')
        ->and($compensation->compensation_class)->toBeNull()
        ->and($compensation->status)->toBe(CompensationStatus::Completed);
});

it('halts the rollback on a failed compensation under the Stop policy', function () {
    $run = SagaFlow::create(StopPolicyWorkflow::class)->runSync();

    // Rollback order is c, b, a. undo b fails; Stop halts before undo a.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c']);

    $compensations = $run->compensations()->orderBy('sequence')->get();

    expect($compensations)->toHaveCount(2)
        ->and($compensations[0]->status)->toBe(CompensationStatus::Completed)
        ->and($compensations[1]->status)->toBe(CompensationStatus::Failed)
        ->and($run->exception['compensation'] ?? null)->not->toBeNull();
});

it('continues the rollback past a failed compensation under the Continue policy', function () {
    $run = SagaFlow::create(ContinuePolicyWorkflow::class)->runSync();

    // undo c ok, undo b fails (Continue), undo a still runs.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c', 'undo:a']);

    $compensations = $run->compensations()->orderBy('sequence')->get();

    expect($compensations)->toHaveCount(3)
        ->and($compensations[0]->status)->toBe(CompensationStatus::Completed)
        ->and($compensations[1]->status)->toBe(CompensationStatus::Failed)
        ->and($compensations[2]->status)->toBe(CompensationStatus::Completed);
});

it('rolls back a parallel saga group as one level (sync)', function () {
    $run = SagaFlow::create(ParallelSagaWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toContain('undo:a')
        ->and(CompensationLog::all())->toContain('undo:b');

    $compensations = $run->compensations()->get();

    expect($compensations)->toHaveCount(2)
        ->and($compensations->every(fn ($c) => $c->status === CompensationStatus::Completed))->toBeTrue();
});

it('rolls back a parallel saga group via Bus::batch (queued)', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(ParallelSagaWorkflow::class)->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Failed);

    $log = CompensationLog::all();
    sort($log);

    expect($log)->toBe(['undo:a', 'undo:b']);

    $compensations = $final->compensations()->get();

    expect($compensations)->toHaveCount(2)
        ->and($compensations->every(fn ($c) => $c->status === CompensationStatus::Completed))->toBeTrue();
});

it('manually compensates a non-terminal run and cancels it', function () {
    $run = SagaFlow::create(ManualCompensateWorkflow::class)->runSync();

    // Parked on awaitSignal('go').
    expect($run->status)->toBe(FlowStatus::Waiting);

    $compensated = SagaFlow::loadFlow($run->id)->compensate();

    expect($compensated->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:a']);

    $compensations = $compensated->compensations()->get();

    expect($compensations)->toHaveCount(1)
        ->and($compensations[0]->status)->toBe(CompensationStatus::Completed);
});

it('does not compensate the failed step by default', function () {
    $run = SagaFlow::create(FailedStepWithCompensationWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:a']);

    expect($run->compensations()->get())->toHaveCount(1);
});

it('compensates the failed step when opted in per action', function () {
    $run = SagaFlow::create(CompensateFailedStepWorkflow::class)->runSync();

    // Failed step rolled back first (LIFO), then the prior completed step.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:failed-step', 'undo:a']);

    expect($run->compensations()->get())->toHaveCount(2);
});

it('compensates the failed step when enabled via config', function () {
    config()->set('saga-lara-flow.sagas.compensate_failed_step', true);

    SagaFlow::create(FailedStepWithCompensationWorkflow::class)->runSync();

    expect(CompensationLog::all())->toBe(['undo:failed-step', 'undo:a']);
});

it('bypasses compensation when a history-contract mismatch fails the run', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => SagaRollbackWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    // Step 0 completed (compensatable); step 1 recorded with a different class than
    // the workflow will request, so replay raises a history-contract mismatch.
    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Completed,
        'has_compensation' => true,
        'result' => ['label' => 'a'],
        'attempts' => 1,
    ]);

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 1,
        'action_class' => ThrowingAction::class,
        'status' => ActionStatus::Completed,
        'result' => [],
        'attempts' => 1,
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and($run->compensations()->count())->toBe(0)
        ->and(CompensationLog::all())->toBe([]);
});
