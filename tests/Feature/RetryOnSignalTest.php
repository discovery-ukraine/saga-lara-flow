<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalWorkflow;
use Illuminate\Support\Facades\Event;

/**
 * Sync-mode coverage for retryOnSignal(). Waking the run on delivery is switched
 * off here so the signal does not queue a ResumeWorkflowJob: these tests drive the
 * run again explicitly in RunMode::Sync, which keeps the inline path under test
 * instead of silently falling through to the queued one (that is covered separately).
 */
beforeEach(function () {
    FlakyPaymentAction::reset();
    CompensationLog::reset();

    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);
});

/**
 * Deliver the retry signal and replay the run inline, the way a resumed job would.
 */
function refillAndDrive(FlowRun $run): FlowRun
{
    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    return app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Sync);
}

it('parks a failed step on its signal instead of failing the flow', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $actions = $run->actions()->orderBy('sequence')->get();

    // Only the two steps reached so far exist: parking does not consume a sequence
    // of its own, and the downstream step has not been scheduled.
    expect($actions)->toHaveCount(2)
        ->and($actions[1]->sequence)->toBe(1)
        ->and($actions[1]->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($actions[1]->retry_signal)->toBe('balance-refilled')
        ->and($actions[1]->retry_signal_attempts)->toBe(0)
        ->and($actions[1]->exception['message'] ?? null)->toBe('insufficient balance');

    $marker = $run->signals()->first();

    expect($run->signals()->get())->toHaveCount(1)
        ->and($marker->status)->toBe(SignalStatus::Waiting)
        ->and($marker->name)->toBe('balance-refilled')
        ->and($marker->wait_sequence)->toBe(1);

    // A parked step is not terminal, so nothing has rolled back.
    expect(CompensationLog::all())->toBe([])
        ->and($run->events()->pluck('type')->all())->toContain(FlowEventType::ActionAwaitingRetry);
});

it('retries only the parked step when the signal arrives', function () {
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-2')->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $final = refillAndDrive($run);

    // Read key by key rather than comparing the map whole: a map read back out of a
    // json column comes back in whatever order the driver stored it, and MySQL's json
    // type is a binary format that sorts an object's keys. Comparing the whole map
    // would have to drop to a loose ==, which would stop noticing an int that turned
    // into a string.
    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['charged']['charged'] ?? null)->toBe('order-2')
        ->and($final->result['charged']['calls'] ?? null)->toBe(2);

    $actions = $final->actions()->orderBy('sequence')->get();

    expect($actions)->toHaveCount(3)
        ->and($actions[1]->status)->toBe(ActionStatus::Completed)
        ->and($actions[1]->sequence)->toBe(1)
        ->and($actions[1]->retry_signal_attempts)->toBe(1)
        // The step before it was not re-executed, and the step after it still sits
        // at the sequence it would have had without any retry.
        ->and($actions[0]->attempts)->toBe(1)
        ->and($actions[2]->sequence)->toBe(2)
        ->and($actions[2]->result)->toBe(['label' => 'shipped']);

    $marker = $final->signals()->first();

    expect($final->signals()->get())->toHaveCount(1)
        ->and($marker->status)->toBe(SignalStatus::Consumed)
        ->and($marker->wait_sequence)->toBe(1);

    expect(CompensationLog::all())->toBe([])
        ->and($final->events()->pluck('type')->all())->toContain(FlowEventType::ActionRetried);
});

it('parks again when the retried step fails once more', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-3')->runSync();

    $again = refillAndDrive($run);

    expect($again->status)->toBe(FlowStatus::Waiting)
        ->and(FlakyPaymentAction::$calls)->toBe(2);

    $step = $again->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal_attempts)->toBe(1);

    // One marker per retry cycle, all at the step's own ordinal: the spent one is
    // consumed history, the fresh one is the wait we are parked on now.
    $markers = $again->signals()->orderBy('id')->get();

    expect($markers)->toHaveCount(2)
        ->and($markers[0]->status)->toBe(SignalStatus::Consumed)
        ->and($markers[1]->status)->toBe(SignalStatus::Waiting)
        ->and($markers->pluck('wait_sequence')->all())->toBe([1, 1]);

    expect(CompensationLog::all())->toBe([]);
});

it('succeeds on the third attempt and carries the saga on', function () {
    FlakyPaymentAction::reset(failures: 2);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-4')->runSync();

    $run = refillAndDrive($run);

    expect($run->status)->toBe(FlowStatus::Waiting);

    $final = refillAndDrive($run);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['charged']['calls'] ?? null)->toBe(3);

    $step = $final->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::Completed)
        ->and($step->retry_signal_attempts)->toBe(2);

    expect($final->actions()->count())->toBe(3)
        ->and(CompensationLog::all())->toBe([]);
});

it('fails normally when the failure falls outside only', function () {
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-5', null, [LogicException::class])
        ->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(FlakyPaymentAction::$calls)->toBe(1);

    $step = $run->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and($run->signals()->count())->toBe(0);

    // The completed step before it rolled back, exactly as it would have without
    // any retry policy in play.
    expect(CompensationLog::all())->toBe(['undo:created']);
});

it('writes a retry ceiling only for a step that carries the policy', function () {
    config()->set('saga-lara-flow.actions.retry_on_signal.max_retries', 2);
    FlakyPaymentAction::reset(failures: 99);

    $plain = SagaFlow::create(OneActionWorkflow::class)->runSync();

    // A configured cap describes the retry policy, not every action ever scheduled.
    // Left on an ordinary row it becomes an unrelated number that awaitRetry()'s ??=
    // would later keep in place of the ceiling the seam actually parked on.
    $step = $plain->actions()->where('sequence', 0)->first();

    expect($step->retry_signal)->toBeNull()
        ->and($step->retry_signal_max_attempts)->toBeNull();

    $parked = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-cap')
        ->runSync();

    // The same default still reaches a step that does carry the policy.
    $charged = $parked->actions()->where('sequence', 1)->first();

    expect($charged->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($charged->retry_signal_max_attempts)->toBe(2);
});

it('rejects a negative retry budget or wait', function () {
    // Both columns are unsigned: a negative value is a MySQL error and a step that
    // silently never parks elsewhere. The seam refuses it up front instead, so the
    // run fails the same way on every driver, naming the value that is wrong.
    $budget = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-neg', -1)
        ->runSync();

    expect($budget->status)->toBe(FlowStatus::Failed)
        ->and($budget->exception['class'] ?? null)->toBe(InvalidArgumentException::class)
        ->and($budget->exception['message'] ?? '')->toContain('maxRetries must be zero or greater');

    $wait = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-neg-wait', null, null, -30)
        ->runSync();

    expect($wait->status)->toBe(FlowStatus::Failed)
        ->and($wait->exception['message'] ?? '')->toContain('waitSeconds must be zero or greater');

    // Refused before anything was scheduled, so no step carries a negative ceiling.
    expect($budget->actions()->whereNotNull('retry_signal_max_attempts')->count())->toBe(0);
});

it('rejects a negative configured retry budget', function () {
    config()->set('saga-lara-flow.actions.retry_on_signal.max_retries', -5);
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-neg-config')
        ->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['message'] ?? '')->toContain('actions.retry_on_signal.max_retries');
});

it('surfaces a failure that happened before the step could be recorded as failed', function () {
    FlakyPaymentAction::reset(failures: 0);

    // A listener (or an observer, or an action class that will not resolve) throws
    // before failAction() ever runs, so there is no Failed row for the seam to read.
    Event::listen(ActionStarted::class, function (ActionStarted $event): void {
        if ($event->actionRun->action_class === FlakyPaymentAction::class) {
            throw new RuntimeException('listener blew up before the step could fail');
        }
    });

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-inline')
        ->runSync();

    // Without the guard the seam replayed a step that never reached Failed, and a
    // sync run suspended on a job that does not exist: waiting for ever, no signal
    // to wake it, and the real error swallowed.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RuntimeException::class)
        ->and($run->signals()->count())->toBe(0);

    // And it rolls back the way it would have without any retry policy.
    expect(CompensationLog::all())->toBe(['undo:created']);
});
