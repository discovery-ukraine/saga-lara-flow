<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;

/**
 * Age a run's updated_at past the repair grace window without bumping it back
 * (an explicit updated_at in the update array is left untouched by Eloquent).
 */
function ageWaitingRun(string $id): void
{
    config('saga-lara-flow.models.flow_run')::query()
        ->whereKey($id)
        ->update(['updated_at' => now()->subMinutes(2)]);
}

beforeEach(function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
});

it('re-wakes a stuck Waiting run whose resume was lost, then replay completes it', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

    // The blocking action already finished, but the resume never fired.
    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Completed,
        'result' => ['label' => 'value'],
        'attempts' => 1,
    ]);

    ageWaitingRun($run->id);

    $report = app(FlowDoctor::class)->repair();

    expect($report->rewokenFlows)->toBe(1)
        ->and($report->redispatchedActions)->toBe(0);

    $rewoken = SagaFlow::findRun($run->id)->events()
        ->where('type', FlowEventType::FlowRewoken->value)->get();

    expect($rewoken)->toHaveCount(1)
        ->and($rewoken->first()->payload['reason'] ?? null)->toBe('lost_resume');

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('does not re-wake a run legitimately waiting on a signal', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => SignalOnlyWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

    FlowSignal::create([
        'flow_run_id' => $run->id,
        'name' => 'go',
        'status' => SignalStatus::Waiting,
        'wait_sequence' => 0,
    ]);

    ageWaitingRun($run->id);

    expect(app(FlowDoctor::class)->repair()->rewokenFlows)->toBe(0)
        ->and(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting)
        ->and(SagaFlow::findRun($run->id)->events()
            ->where('type', FlowEventType::FlowRewoken->value)->count())->toBe(0);
});

it('does not re-wake (R2) a run still waiting on a Pending action', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

    // Pending action, fresh (within grace) so R1 also leaves it alone.
    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Pending,
        'attempts' => 0,
    ]);

    ageWaitingRun($run->id);

    $report = app(FlowDoctor::class)->repair();

    expect($report->rewokenFlows)->toBe(0)
        ->and($report->redispatchedActions)->toBe(0)
        ->and(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);
});
