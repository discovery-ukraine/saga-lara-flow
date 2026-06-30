<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelEchoWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelFailFastWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelOptionalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelWaitAllWorkflow;

beforeEach(fn () => CompensationLog::reset());

it('joins a parallel block and returns results in declaration order (sync)', function () {
    $run = SagaFlow::create(ParallelEchoWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['results'] ?? null)->toBe([
            ['label' => 'a'],
            ['label' => 'b'],
            ['label' => 'c'],
        ]);

    $actions = $run->actions()->orderBy('sequence')->get();

    expect($actions)->toHaveCount(3)
        ->and($actions->every(fn ($a) => $a->status === ActionStatus::Completed))->toBeTrue()
        ->and($actions->every(fn ($a) => $a->parallel_group === 0))->toBeTrue();
});

it('dispatches every parallel step together before any resumes (queued)', function () {
    useDatabaseQueue();

    $run = app(FlowRepository::class)->create([
        'workflow_class' => ParallelEchoWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    // The whole block is scheduled in one pass, then the flow waits for the batch.
    expect($driven->status)->toBe(FlowStatus::Waiting)
        ->and(ActionRun::query()->where('flow_run_id', $run->id)->where('status', ActionStatus::Pending)->count())->toBe(3);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['results'] ?? null)->toBe([
            ['label' => 'a'],
            ['label' => 'b'],
            ['label' => 'c'],
        ]);
});

it('reaches an identical final state in sync and queued modes', function () {
    $sync = SagaFlow::create(ParallelEchoWorkflow::class)->runSync();

    useDatabaseQueue();
    $queued = SagaFlow::create(ParallelEchoWorkflow::class)->run();
    drainQueue();

    expect(runStateSnapshot($queued->id))->toEqual(runStateSnapshot($sync->id));
});

it('fails fast and compensates completed steps (sync)', function () {
    $run = SagaFlow::create(ParallelFailFastWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['message'] ?? '')->toContain('boom')
        ->and(CompensationLog::all())->toBe(['undo:a']);

    $compensations = $run->compensations()->get();

    expect($compensations)->toHaveCount(1)
        ->and($compensations[0]->status)->toBe(CompensationStatus::Completed);
});

it('waits for all steps then fails, compensating every completed step (sync)', function () {
    $run = SagaFlow::create(ParallelWaitAllWorkflow::class)->runSync();

    $log = CompensationLog::all();
    sort($log);

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($log)->toBe(['undo:a', 'undo:b']);

    $actions = $run->actions()->orderBy('sequence')->get();

    expect($actions)->toHaveCount(3)
        ->and($actions[0]->status)->toBe(ActionStatus::Completed)
        ->and($actions[1]->status)->toBe(ActionStatus::Completed)
        ->and($actions[2]->status)->toBe(ActionStatus::Failed);
});

it('does not fail the block when an optional parallel step fails (sync)', function () {
    $run = SagaFlow::create(ParallelOptionalWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['results'] ?? null)->toBe([
            ['label' => 'a'],
            'skipped',
            ['label' => 'c'],
        ]);

    $optional = $run->actions()->orderBy('sequence')->get()[1];

    expect($optional->status)->toBe(ActionStatus::OptionalFailed);
});

it('rolls a parallel block back as one level when a later step fails (sync)', function () {
    $run = SagaFlow::create(ParallelCompensationWorkflow::class)->runSync();

    $log = CompensationLog::all();
    sort($log);

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($log)->toBe(['undo:a', 'undo:b']);

    $compensations = $run->compensations()->get();

    expect($compensations)->toHaveCount(2)
        ->and($compensations->every(fn ($c) => $c->status === CompensationStatus::Completed))->toBeTrue();
});

it('rolls a parallel block back via Bus::batch (queued)', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(ParallelCompensationWorkflow::class)->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    $log = CompensationLog::all();
    sort($log);

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($log)->toBe(['undo:a', 'undo:b']);

    expect($final->compensations()->get())->toHaveCount(2);
});
