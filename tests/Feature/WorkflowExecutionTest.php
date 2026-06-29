<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunActionJob;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailingWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TwoStepWorkflow;
use Illuminate\Support\Facades\Queue;

it('runs two sequential actions to completion in sync mode', function () {
    $run = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->runSync();

    expect($run->status)->toBe(FlowStatus::Completed);

    $snapshot = runStateSnapshot($run->id);

    expect($snapshot['actions'])->toHaveCount(2)
        ->and($snapshot['actions'][0]['sequence'])->toBe(0)
        ->and($snapshot['actions'][0]['status'])->toBe(ActionStatus::Completed)
        ->and($snapshot['actions'][0]['result'])->toBe(['label' => 'order-1'])
        ->and($snapshot['actions'][1]['sequence'])->toBe(1)
        ->and($snapshot['actions'][0]['status'])->toBe(ActionStatus::Completed)
        ->and($snapshot['actions'][1]['result'])->toBe(['label' => 'order-2']);
});

it('runs two sequential actions to completion in queued mode', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->run();

    expect($run->status)->toBe(FlowStatus::Pending);

    drainQueue();

    $snapshot = runStateSnapshot($run->id);

    expect($snapshot['status'])->toBe(FlowStatus::Completed)
        ->and($snapshot['actions'])->toHaveCount(2)
        ->and($snapshot['actions'][0]['sequence'])->toBe(0)
        ->and($snapshot['actions'][0]['status'])->toBe(ActionStatus::Completed)
        ->and($snapshot['actions'][1]['sequence'])->toBe(1)
        ->and($snapshot['actions'][0]['result'])->toBe(['label' => 'order-1'])
        ->and($snapshot['actions'][1]['status'])->toBe(ActionStatus::Completed)
        ->and($snapshot['actions'][1]['result'])->toBe(['label' => 'order-2']);
});

it('reaches an identical final database state in sync and queued modes', function () {
    $sync = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->runSync();

    useDatabaseQueue();
    $queued = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->run();
    drainQueue();

    expect(runStateSnapshot($queued->id))->toEqual(runStateSnapshot($sync->id));
});

it('fails the flow on a business error and never schedules later steps', function () {
    $run = SagaFlow::create(FailingWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['message'] ?? null)->toBe('boom');

    $actions = $run->actions()->orderBy('sequence')->get();

    expect($actions)->toHaveCount(1)
        ->and($actions[0]->status)->toBe(ActionStatus::Failed);
});

it('never treats an internal control signal as a business failure', function () {
    Queue::fake();

    $run = app(FlowRepository::class)->create([
        'workflow_class' => FailingWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => ['order'],
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    // The first action suspends the flow (control signal) — it must wait, not fail.
    expect($driven->status)->toBe(FlowStatus::Waiting);

    Queue::assertPushed(RunActionJob::class);
});
