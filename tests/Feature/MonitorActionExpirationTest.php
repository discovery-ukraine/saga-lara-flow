<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ExpiringActionSagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ExpiringActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalExpiringActionWorkflow;

beforeEach(fn () => CompensationLog::reset());

it('persists expiresAt and expires a stuck action, failing the flow', function () {
    useDatabaseQueue();

    $run = app(FlowRepository::class)->create([
        'workflow_class' => ExpiringActionWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    // Drive once to schedule the action (Pending) without running its job yet.
    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    expect($driven->status)->toBe(FlowStatus::Waiting);

    $action = $driven->actions()->first();

    expect($action->status)->toBe(ActionStatus::Pending)
        ->and($action->expires_at)->not->toBeNull()
        ->and($action->expires_at->isPast())->toBeTrue();

    $report = app(FlowMonitor::class)->sweep();

    expect($report['actions'])->toBe(1);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($final->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and($final->actions()->first()->status)->toBe(ActionStatus::Expired)
        ->and($final->events()->where('type', FlowEventType::ActionExpired->value)->count())->toBe(1);
});

it('rolls back completed steps when a later action expires', function () {
    useDatabaseQueue();

    $run = app(FlowRepository::class)->create([
        'workflow_class' => ExpiringActionSagaWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

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
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Pending,
        'expires_at' => now()->subMinute(),
        'attempts' => 0,
    ]);

    expect(app(FlowMonitor::class)->sweep()['actions'])->toBe(1);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($final->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and(CompensationLog::all())->toContain('undo:a');
});

it('tolerates an expired optional action and completes the flow', function () {
    useDatabaseQueue();

    $run = app(FlowRepository::class)->create([
        'workflow_class' => OptionalExpiringActionWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    expect($driven->status)->toBe(FlowStatus::Waiting);

    expect(app(FlowMonitor::class)->sweep()['actions'])->toBe(1);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result)->toBe(['skipped' => true])
        ->and($final->actions()->first()->status)->toBe(ActionStatus::Expired);
});
