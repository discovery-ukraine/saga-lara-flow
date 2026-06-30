<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;

beforeEach(fn () => useDatabaseQueue());

it('re-drives a stuck Waiting run on manual kick and completes it', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Completed,
        'result' => ['label' => 'value'],
        'attempts' => 1,
    ]);

    app(FlowDoctor::class)->kick($run);

    $rewoken = SagaFlow::findRun($run->id)->events()
        ->where('type', FlowEventType::FlowRewoken->value)->get();

    expect($rewoken)->toHaveCount(1)
        ->and($rewoken->first()->payload['reason'] ?? null)->toBe('manual');

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('re-drives a crashed Running run without a state-machine error', function () {
    // A run left in Running (crash mid-iteration) with no history yet: kick must
    // re-drive it — the same-state Running transition is an idempotent no-op.
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Running,
        'arguments' => [],
    ]);

    app(FlowDoctor::class)->kick($run);

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('leaves a terminal run untouched on kick', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Completed,
        'arguments' => [],
    ]);

    DB::connection('testing')->table('jobs')->delete();

    app(FlowDoctor::class)->kick($run);

    expect(SagaFlow::findRun($run->id)->events()
        ->where('type', FlowEventType::FlowRewoken->value)->count())->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

it('exposes repair and kick as console commands', function () {
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Completed,
        'result' => ['label' => 'value'],
        'attempts' => 1,
    ]);

    expect(Artisan::call('saga-flow:repair'))->toBe(0)
        ->and(Artisan::call('saga-flow:kick', ['run' => $run->id]))->toBe(0)
        ->and(Artisan::call('saga-flow:kick', ['run' => 'missing-id']))->toBe(1);
});
