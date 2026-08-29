<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionAwaitingRetry;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionRetried;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalSagaGroupWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Event;

/**
 * The operator surface of retryOnSignal(): the saga() group mirror, the two
 * lifecycle events, and what saga-flow:show / :list / :signal make of a parked
 * run. The mechanics themselves are covered by RetryOnSignalTest (sync) and
 * RetryOnSignalQueuedTest (queued).
 *
 * Waking the run on delivery is off by default here: most of these tests replay
 * the run inline afterwards, which keeps them on the sync path instead of letting
 * a resume job decide the outcome. The tests that are about delivery itself turn
 * it back on and use the real database queue.
 */
beforeEach(function () {
    FlakyPaymentAction::reset();
    CompensationLog::reset();

    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);
});

/**
 * Deliver the retry signal and replay the run inline, the way a resumed job would.
 */
function driveAfterSignal(FlowRun $run): FlowRun
{
    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    return app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Sync);
}

/**
 * Park a run on 'balance-refilled' at sequence 1 on the real database queue, with
 * waking left on so a delivery drives the run exactly as it would in production.
 */
function parkedQueuedRun(int $failures = 1): FlowRun
{
    useDatabaseQueue();

    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', true);

    FlakyPaymentAction::reset(failures: $failures);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-cli', 3)
        ->run();

    drainQueue();

    return SagaFlow::findRun($run->id);
}

/**
 * Run a console command and hand back everything it printed. Chained
 * expectsOutputToContain() assertions are consumed against successive writes, so
 * several substrings of one rendered table are checked here instead.
 */
function commandOutput(string $command, array $parameters = []): string
{
    Artisan::call($command, $parameters);

    return Artisan::output();
}

it('mirrors retryOnSignal on a saga() group step', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalSagaGroupWorkflow::class)
        ->withArguments('order-group')
        ->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $step = $run->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal)->toBe('balance-refilled')
        // The step ahead of it completed and its compensation is still only stacked:
        // parking is not a rollback.
        ->and(CompensationLog::all())->toBe([]);
});

it('retries a saga() group step when its signal arrives', function () {
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalSagaGroupWorkflow::class)
        ->withArguments('order-group-2')
        ->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $final = driveAfterSignal($run);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->actions()->count())->toBe(3);

    $step = $final->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::Completed)
        ->and($step->retry_signal_attempts)->toBe(1)
        ->and(CompensationLog::all())->toBe([]);
});

it('carries the budget from a saga() group step into the give-up', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalSagaGroupWorkflow::class)
        ->withArguments('order-group-3', 1)
        ->runSync();

    $step = $run->actions()->where('sequence', 1)->first();

    expect($step->retry_signal_max_attempts)->toBe(1);

    // One cycle is all the mirrored budget allows: the second failure is final and
    // the completed step ahead of it rolls back.
    $final = driveAfterSignal($run);

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:created']);
});

it('dispatches one awaiting-retry event per parking and one retried event per cycle', function () {
    Event::fake([ActionAwaitingRetry::class, ActionRetried::class]);

    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-events')
        ->runSync();

    Event::assertDispatchedTimes(ActionAwaitingRetry::class, 1);
    Event::assertDispatchedTimes(ActionRetried::class, 0);

    Event::assertDispatched(
        ActionAwaitingRetry::class,
        fn (ActionAwaitingRetry $event): bool => $event->signal === 'balance-refilled'
            && $event->actionRun->sequence === 1
            && $event->actionRun->status === ActionStatus::AwaitingRetry,
    );

    driveAfterSignal($run);

    // One cycle spent, and the failure that ended it parked the step again.
    Event::assertDispatchedTimes(ActionRetried::class, 1);
    Event::assertDispatchedTimes(ActionAwaitingRetry::class, 2);

    Event::assertDispatched(
        ActionRetried::class,
        fn (ActionRetried $event): bool => $event->actionRun->retry_signal_attempts === 1,
    );
});

it('dispatches no retry events for a step that never parks', function () {
    Event::fake([ActionAwaitingRetry::class, ActionRetried::class]);

    FlakyPaymentAction::reset(failures: 0);

    SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-clean')->runSync();

    Event::assertNotDispatched(ActionAwaitingRetry::class);
    Event::assertNotDispatched(ActionRetried::class);
});

it('renders a parked step in saga-flow:show', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-show', 3, null, 900)
        ->runSync();

    $output = commandOutput('saga-flow:show', ['run' => $run->id]);

    expect($output)->toContain('awaiting_retry')
        // The signal, how much of the budget is spent, and how long the wait still
        // has — everything needed to decide whether to send the signal by hand.
        ->and($output)->toContain('balance-refilled 0/3 until')
        // The wait-signal is tied to the step it parks.
        ->and($output)->toMatch('/balance-refilled\s+\|\s+waiting\s+\|\s+1\s+\|/');

    // The retry cell lives in the actions table, so --compact keeps it.
    expect(commandOutput('saga-flow:show', ['run' => $run->id, '--compact' => true]))
        ->toContain('balance-refilled 0/3');
});

it('shows an unbounded retry budget as infinite in saga-flow:show', function () {
    FlakyPaymentAction::reset(failures: 99);

    // No maxRetries and no configured default: the budget is unbounded.
    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-unbounded')
        ->runSync();

    expect(commandOutput('saga-flow:show', ['run' => $run->id]))
        ->toContain('balance-refilled 0/∞');
});

it('keeps the retry column blank for steps without a policy', function () {
    FlakyPaymentAction::reset(failures: 0);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-plain')->runSync();

    expect($run->status)->toBe(FlowStatus::Completed);

    $output = commandOutput('saga-flow:show', ['run' => $run->id]);

    // The policy stays visible on a step that succeeded first time — with a spent
    // budget of zero, which is how an operator tells "never parked" from "recovered".
    expect($output)->toContain('balance-refilled 0/')
        ->and($output)->not->toContain('awaiting_retry')
        ->and($output)->toMatch('/MakeValueAction\s+\|\s+1\s+\|\s+—\s+\|/');
});

it('marks a run parked on a retry signal in saga-flow:list', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-list')
        ->runSync();

    expect(commandOutput('saga-flow:list'))
        ->toContain($run->id)
        ->toContain('waiting (retry: balance-refilled)');
});

it('drops the retry annotation once the run is no longer waiting', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-cancelled')
        ->runSync();

    SagaFlow::loadFlow($run->id)->cancel('operator');

    $cancelled = SagaFlow::findRun($run->id);

    // Cancelling the run settles the step it was parked on, so both commands see a
    // step that is plainly over. The retry policy it carried is still on the row for
    // an operator to read.
    expect($cancelled->status)->toBe(FlowStatus::Cancelled)
        ->and($cancelled->actions()->where('sequence', 1)->first()->status)
        ->toBe(ActionStatus::Cancelled);

    expect(commandOutput('saga-flow:list'))
        ->toContain('cancelled')
        ->not->toContain('(retry:');

    // Same in saga-flow:show. The wait-signal keeps its timeout_at, so the deadline is
    // still there to print — but counting down to it would promise the operator a wait
    // that nothing is going to resolve.
    expect(commandOutput('saga-flow:show', ['run' => $run->id]))
        ->toContain('balance-refilled 0/')
        ->not->toContain('until');
});

it('drops the retry annotation while the run is rolling back', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-rolling-back')
        ->runSync();

    // Cancelling is not terminal, so the step stays parked with its deadline intact —
    // the case the run-status gate exists for. Naming its signal would send the operator
    // after a delivery a run already taken over by its rollback will not act on.
    app(StateMachine::class)->transition(SagaFlow::findRun($run->id), FlowStatus::Cancelling);

    $rollingBack = SagaFlow::findRun($run->id);

    expect($rollingBack->status)->toBe(FlowStatus::Cancelling)
        ->and($rollingBack->actions()->where('sequence', 1)->first()->status)
        ->toBe(ActionStatus::AwaitingRetry);

    expect(commandOutput('saga-flow:list'))
        ->toContain('cancelling')
        ->not->toContain('(retry:');

    expect(commandOutput('saga-flow:show', ['run' => $run->id]))
        ->toContain('balance-refilled 0/')
        ->not->toContain('until');
});

it('leaves an ordinary waiting run unannotated in saga-flow:list', function () {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting)
        ->and(commandOutput('saga-flow:list'))->not->toContain('(retry:');
});

it('finds a parked run through the query API', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-query')
        ->runSync();

    // A parked run is Waiting like any other, so it is both listable and still a
    // legal signal target — nothing about the retry seam narrows that.
    expect(SagaFlow::query()->waiting()->get()->pluck('id')->all())->toContain($run->id)
        ->and(SagaFlow::query()->signalable()->count())->toBe(1)
        ->and(SagaFlow::query()->active()->count())->toBe(1);
});

it('drives a parked run to completion through saga-flow:signal', function () {
    $run = parkedQueuedRun();

    expect($run->status)->toBe(FlowStatus::Waiting)
        ->and($run->actions()->where('sequence', 1)->first()->status)
        ->toBe(ActionStatus::AwaitingRetry);

    Artisan::call('saga-flow:signal', ['run' => $run->id, 'name' => 'balance-refilled']);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->actions()->where('sequence', 1)->first()->retry_signal_attempts)->toBe(1)
        ->and(CompensationLog::all())->toBe([]);
});

it('carries a signal payload from saga-flow:signal into the retried run', function () {
    $run = parkedQueuedRun();

    Artisan::call('saga-flow:signal', [
        'run' => $run->id,
        'name' => 'balance-refilled',
        '--payload' => '{"topped_up":500}',
    ]);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed);

    // The delivery that ended the wait is the marker at the step's own ordinal, and
    // it kept the payload the operator sent.
    $marker = $final->signals()->reorder()->orderByDesc('id')->first();

    expect($marker->wait_sequence)->toBe(1)
        ->and($marker->payload)->toBe(['topped_up' => 500]);
});
