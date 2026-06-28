<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Events\SideEffectRecorded;
use DiscoveryUkraine\SagaLaraFlow\Events\SideEffectReused;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SideEffectCounter;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SideEffectOnlyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SideEffectWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use Illuminate\Support\Facades\Event;

beforeEach(fn () => SideEffectCounter::reset());

/**
 * Durable side-effect state for cross-mode comparison.
 */
function sideEffectRows(string $flowRunId): array
{
    return SagaFlow::findRun($flowRunId)
        ->sideEffects()
        ->orderBy('sequence')
        ->get()
        ->map(fn ($sideEffect) => [
            'sequence' => $sideEffect->sequence,
            'key' => $sideEffect->key,
            'value' => $sideEffect->value,
        ])
        ->all();
}

it('runs a side-effect factory exactly once across replays', function () {
    $run = SagaFlow::create(SideEffectWorkflow::class)->withArguments('order')->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and(SideEffectCounter::$count)->toBe(1);

    $rows = sideEffectRows($run->id);

    expect($rows)->toHaveCount(1)
        ->and($rows[0]['sequence'])->toBe(0)
        ->and($rows[0]['key'])->toBe('token')
        ->and($rows[0]['value'])->toBe(1);
});

it('reaches an identical final database state in sync and queued modes with a side effect', function () {
    SideEffectCounter::reset();
    $sync = SagaFlow::create(SideEffectWorkflow::class)->withArguments('order')->runSync();

    useDatabaseQueue();
    SideEffectCounter::reset();
    $queued = SagaFlow::create(SideEffectWorkflow::class)->withArguments('order')->run();
    drainQueue();

    expect(runStateSnapshot($queued->id))->toEqual(runStateSnapshot($sync->id))
        ->and(sideEffectRows($queued->id))->toEqual(sideEffectRows($sync->id));
});

it('records the side effect once and reuses it silently on replay', function () {
    Event::fake([SideEffectRecorded::class, SideEffectReused::class]);

    $run = SagaFlow::create(SideEffectWorkflow::class)->withArguments('order')->runSync();

    Event::assertDispatched(SideEffectRecorded::class, 1);
    Event::assertDispatched(SideEffectReused::class);

    $recorded = $run->events()->where('type', FlowEventType::SideEffectRecorded->value)->count();
    $reused = $run->events()->where('type', FlowEventType::SideEffectReused->value)->count();

    expect($recorded)->toBe(1)
        ->and($reused)->toBe(0);
});

it('records a flow event for each reuse when history.record_side_effect_reuse is enabled', function () {
    config()->set('saga-lara-flow.history.record_side_effect_reuse', true);

    $run = SagaFlow::create(SideEffectWorkflow::class)->withArguments('order')->runSync();

    $recorded = $run->events()->where('type', FlowEventType::SideEffectRecorded->value)->count();
    $reused = $run->events()->where('type', FlowEventType::SideEffectReused->value)->count();

    // One record on the first pass, one reuse per later replay (two actions => two replays).
    expect($recorded)->toBe(1)
        ->and($reused)->toBe(2);
});

it('fails the flow when a different action class is recorded at the same sequence', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => ThrowingAction::class,
        'status' => ActionStatus::Completed,
        'result' => ['label' => 'stale'],
        'attempts' => 1,
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and($driven->exception['class'] ?? null)->toBe(HistoryContractMismatchException::class)
        ->and($driven->exception['message'] ?? '')->toContain(ThrowingAction::class)
        ->and($driven->exception['message'] ?? '')->toContain(MakeValueAction::class);
});

it('fails the flow when an action is requested where a side effect is recorded', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    SideEffect::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'key' => 'token',
        'value' => 'stale',
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and($driven->exception['class'] ?? null)->toBe(HistoryContractMismatchException::class)
        ->and($driven->exception['message'] ?? '')->toContain('side effect');
});

it('fails the flow when a side effect is requested where an action is recorded', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => SideEffectOnlyWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Completed,
        'result' => ['label' => 'stale'],
        'attempts' => 1,
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and($driven->exception['class'] ?? null)->toBe(HistoryContractMismatchException::class)
        ->and($driven->exception['message'] ?? '')->toContain(MakeValueAction::class);
});
