<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowSignalConsumed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowSignalReceived;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\EarlySignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TwoStepWorkflow;
use Illuminate\Support\Facades\Event;

it('parks on a signal then resumes and consumes the delivered payload', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalWorkflow::class)->withArguments('order')->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);

    expect($run->status)->toBe(FlowStatus::Waiting);

    $marker = $run->signals()->first();

    expect($run->signals()->get())->toHaveCount(1)
        ->and($marker->status)->toBe(SignalStatus::Waiting)
        ->and($marker->name)->toBe('approval')
        ->and($marker->wait_sequence)->toBe(1);

    SagaFlow::loadFlow($run->id)->signal('approval', ['by' => 'mgr']);
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed);

    $consumed = $final->signals()->first();

    expect($consumed->status)->toBe(SignalStatus::Consumed)
        ->and($consumed->payload)->toBe(['by' => 'mgr'])
        ->and($consumed->wait_sequence)->toBe(1);

    $actions = $final->actions()->orderBy('sequence')->get();

    expect($actions)->toHaveCount(2)
        ->and($actions[1]->sequence)->toBe(2)
        ->and($actions[1]->result)->toBe(['label' => 'approved-by-mgr']);
});

it('consumes a signal delivered before the workflow awaits it', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => EarlySignalWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    FlowSignal::create([
        'flow_run_id' => $run->id,
        'name' => 'go',
        'payload' => ['v' => 'early'],
        'status' => SignalStatus::Received,
        'received_at' => now(),
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Completed);

    $signal = $driven->signals()->first();

    expect($signal->status)->toBe(SignalStatus::Consumed)
        ->and($signal->wait_sequence)->toBe(0);

    expect($driven->actions()->first()->result)->toBe(['label' => 'got-early']);
});

it('rejects a signal on a terminal run and reports it via signalIfRunning', function () {
    $run = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->runSync();

    expect($run->status)->toBe(FlowStatus::Completed);

    $handle = SagaFlow::loadFlow($run->id);

    expect(fn () => $handle->signal('whatever'))->toThrow(CannotSignalTerminalFlowException::class);
    expect($handle->signalIfRunning('whatever'))->toBeFalse();
});

it('reports signalIfRunning true for a waiting run', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalWorkflow::class)->withArguments('order')->run();
    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting)
        ->and(SagaFlow::loadFlow($run->id)->signalIfRunning('approval', ['by' => 'ok']))->toBeTrue();
});

it('records signal history and dispatches signal events', function () {
    Event::fake([FlowSignalReceived::class, FlowSignalConsumed::class]);

    useDatabaseQueue();

    $run = SagaFlow::create(SignalWorkflow::class)->withArguments('order')->run();
    drainQueue();

    SagaFlow::loadFlow($run->id)->signal('approval', ['by' => 'mgr']);
    drainQueue();

    Event::assertDispatched(FlowSignalReceived::class, 1);
    Event::assertDispatched(FlowSignalConsumed::class, 1);

    $final = SagaFlow::findRun($run->id);

    $received = $final->events()->where('type', FlowEventType::SignalReceived->value)->count();
    $consumed = $final->events()->where('type', FlowEventType::SignalConsumed->value)->count();

    expect($received)->toBe(1)
        ->and($consumed)->toBe(1)
        ->and(SagaFlow::loadFlow($run->id)->signals())->toHaveCount(1);
});

it('fails the flow when a signal is awaited under a changed name at the same sequence', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => SignalOnlyWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    FlowSignal::create([
        'flow_run_id' => $run->id,
        'name' => 'approval',
        'status' => SignalStatus::Waiting,
        'wait_sequence' => 0,
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and($driven->exception['class'] ?? null)->toBe(HistoryContractMismatchException::class)
        ->and($driven->exception['message'] ?? '')->toContain("signal 'approval'")
        ->and($driven->exception['message'] ?? '')->toContain("signal 'go'");
});

it('fails the flow when a signal is awaited where an action is recorded', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => SignalOnlyWorkflow::class,
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
        ->and($driven->exception['message'] ?? '')->toContain("signal 'go'")
        ->and($driven->exception['message'] ?? '')->toContain(MakeValueAction::class);
});

it('fails the flow when an action is requested where a signal is recorded', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    FlowSignal::create([
        'flow_run_id' => $run->id,
        'name' => 'go',
        'status' => SignalStatus::Waiting,
        'wait_sequence' => 0,
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and($driven->exception['class'] ?? null)->toBe(HistoryContractMismatchException::class)
        ->and($driven->exception['message'] ?? '')->toContain("signal 'go'")
        ->and($driven->exception['message'] ?? '')->toContain(MakeValueAction::class);
});
