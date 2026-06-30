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

it('does nothing when the repair pass is disabled (default)', function () {
    useDatabaseQueue();

    // Default is off; assert the disabled path even if a global config flips it.
    config()->set('saga-lara-flow.repair.enabled', false);

    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Pending,
        'attempts' => 0,
    ]);

    // Age it well past any grace window so only the kill-switch keeps it untouched.
    ActionRun::query()->whereKey($action->id)->update(['created_at' => now()->subMinutes(5)]);

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(0)
        ->and($report->rewokenFlows)->toBe(0)
        ->and($report->skipped)->toBe(0)
        ->and($action->fresh()->repair_attempts)->toBe(0)
        ->and(SagaFlow::findRun($run->id)->events()
            ->where('type', FlowEventType::ActionRedispatched->value)->count())->toBe(0);
});
