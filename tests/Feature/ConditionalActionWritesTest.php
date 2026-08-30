<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SelfCancellingBeforeParkWorkflow;
use Illuminate\Support\Facades\Event;

/**
 * Every write to an action_runs row now states the row it expects to find, and the
 * run's liveness rides in the same UPDATE. Without that a worker, a queue hook or a
 * replay holding a row it read a moment ago writes over whatever happened since —
 * most visibly over terminal settlement, which runs once and never again, so anything
 * written after it stands for ever with nothing left to notice it.
 *
 * The window needs no race to open. Terminal settlement closes Pending, Running and
 * AwaitingRetry rows; a Failed row — the state between two of the queue's own native
 * tries — it leaves alone, and every one of these writers accepts a Failed row.
 */
beforeEach(function () {
    FlakyPaymentAction::reset();
    SelfCancellingBeforeParkWorkflow::reset();

    config()->set('saga-lara-flow.queue.after_commit', false);
});

function fencedFlowRun(FlowStatus $status = FlowStatus::Waiting): FlowRun
{
    return app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => $status,
        'arguments' => [],
    ]);
}

/**
 * @param  array<string, mixed>  $extra
 */
function fencedStep(FlowRun $run, ActionStatus $status, array $extra = []): ActionRun
{
    $step = ActionRun::create(array_merge([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => FlakyPaymentAction::class,
        'arguments' => app(Serializer::class)->serialize(['order-1']),
        'status' => $status,
        'reclaim_stale_after_seconds' => 900,
        'attempts' => 1,
    ], $extra));

    // Read back, so the model carries every column the schema defaulted — which is what
    // a row reaching any of these writers in production always has.
    return $step->refresh();
}

it('refuses to record a give-up on a step whose run has already finished', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::Failed, ['continue_on_failure' => true]);

    $run->markCancelled();

    // Settlement leaves a Failed row exactly as it found it, which is what makes this
    // reachable without a race at all.
    expect($step->fresh()->status)->toBe(ActionStatus::Failed);

    $written = app(ActionRecorder::class)->optionalFail($step->fresh());

    expect($written)->toBeFalse()
        ->and($step->fresh()->status)->toBe(ActionStatus::Failed)
        ->and($step->fresh()->finished_at)->toBeNull()
        ->and($run->events()->pluck('type')->all())->not->toContain('action.optional_failed');
});

it('records a give-up normally while the run is still live', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::Failed, ['continue_on_failure' => true]);

    $written = app(ActionRecorder::class)->optionalFail($step);

    expect($written)->toBeTrue()
        ->and($step->fresh()->status)->toBe(ActionStatus::OptionalFailed)
        ->and($step->fresh()->finished_at)->not->toBeNull();
});

it('records a give-up for a step whose wait timed out on an awaiting_retry row', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::AwaitingRetry, [
        'continue_on_failure' => true,
        'retry_signal' => 'balance-refilled',
    ]);

    // The optional branch of a timed-out wait never runs settleAwaitingRetry(), so the
    // row is still AwaitingRetry here. Measured on the suite: this arrives alongside
    // Failed, and a fence naming only Failed would refuse a legitimate give-up.
    expect(app(ActionRecorder::class)->optionalFail($step))->toBeTrue()
        ->and($step->fresh()->status)->toBe(ActionStatus::OptionalFailed);
});

it('refuses to settle a parked step that terminal settlement has already closed', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::AwaitingRetry, ['retry_signal' => 'balance-refilled']);

    // The seam is holding the row it read before the cancellation landed.
    $asRead = ActionRun::query()->findOrFail($step->id);

    $run->markCancelled();

    // Settlement closed it; the in-memory copy the seam holds still says otherwise, so
    // the early return does not fire and only the fence stands between the two.
    expect($step->fresh()->status)->toBe(ActionStatus::Cancelled)
        ->and($asRead->status)->toBe(ActionStatus::AwaitingRetry);

    expect(app(ActionRecorder::class)->settleAwaitingRetry($asRead))->toBeFalse()
        ->and($step->fresh()->status)->toBe(ActionStatus::Cancelled);
});

it('settles a parked step normally while the run is still live', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::AwaitingRetry, ['retry_signal' => 'balance-refilled']);

    expect(app(ActionRecorder::class)->settleAwaitingRetry($step))->toBeTrue()
        ->and($step->fresh()->status)->toBe(ActionStatus::Failed);
});

it('refuses queue bookkeeping for a retry cycle the row has already left', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::Failed, [
        'retry_signal' => 'balance-refilled',
        'retry_signal_attempts' => 0,
        'queue_attempts_exhausted' => false,
    ]);

    // RunActionJob::failed() resolved the row and passed its generation guard on cycle
    // 0. The seam then spends a cycle before this write lands.
    $asRead = ActionRun::query()->findOrFail($step->id);

    app(ActionRecorder::class)->retryAction(ActionRun::query()->findOrFail($step->id));

    expect($step->fresh()->retry_signal_attempts)->toBe(1)
        ->and($step->fresh()->status)->toBe(ActionStatus::Pending);

    expect(app(ActionRecorder::class)->markQueueAttemptsExhausted($asRead))->toBeFalse()
        // Cycle 1's job has not run yet. Left true, ActionBuilder's Failed branch would
        // stop waiting for the queue and spend a retry on a step still in flight.
        ->and($step->fresh()->queue_attempts_exhausted)->toBeFalse();
});

it('records queue bookkeeping for the cycle the row is actually on', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::Failed, [
        'retry_signal' => 'balance-refilled',
        'retry_signal_attempts' => 0,
        'queue_attempts_exhausted' => false,
    ]);

    expect(app(ActionRecorder::class)->markQueueAttemptsExhausted($step))->toBeTrue()
        ->and($step->fresh()->queue_attempts_exhausted)->toBeTrue();
});

/**
 * A claim raises `attempts` and moves the row to Running; it does not move the retry
 * cycle. So a queue hook holding the spent view of a Failed row still matches the live
 * attempt running under it unless the status it read is part of the fence too.
 */
it('does not mark a generation exhausted while an attempt is running under it', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::Failed, [
        'retry_signal' => 'balance-refilled',
        'retry_signal_attempts' => 0,
        'queue_attempts_exhausted' => false,
    ]);

    // The old hook resolved the row while it was Failed and has not written yet.
    $asRead = ActionRun::query()->findOrFail($step->id);

    // A duplicate delivery claims the same cycle and starts running it.
    expect(app(ActionRecorder::class)->startAction(ActionRun::query()->findOrFail($step->id)))->toBeTrue()
        ->and($step->fresh()->status)->toBe(ActionStatus::Running)
        ->and($step->fresh()->retry_signal_attempts)->toBe(0);

    expect(app(ActionRecorder::class)->markQueueAttemptsExhausted($asRead))->toBeFalse()
        // Flagged, the seam would stop waiting for a job that has only just started and
        // spend a retry cycle on a step still in flight.
        ->and($step->fresh()->queue_attempts_exhausted)->toBeFalse();
});

it('refuses to rewind a step that terminal settlement has already closed', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::AwaitingRetry, ['retry_signal' => 'balance-refilled']);

    $asRead = ActionRun::query()->findOrFail($step->id);

    $run->markCancelled();

    expect($step->fresh()->status)->toBe(ActionStatus::Cancelled);

    expect(app(ActionRecorder::class)->retryAction($asRead))->toBeFalse()
        // Rewound, this row would re-enter dueForRepair() looking live again, under a
        // run that finished before the cycle was ever spent.
        ->and($step->fresh()->status)->toBe(ActionStatus::Cancelled)
        ->and($step->fresh()->retry_signal_attempts)->toBe(0)
        ->and($run->events()->pluck('type')->all())->not->toContain('action.retried');
});

it('rewinds a step normally while the run is still live', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::AwaitingRetry, ['retry_signal' => 'balance-refilled']);

    expect(app(ActionRecorder::class)->retryAction($step))->toBeTrue()
        ->and($step->fresh()->status)->toBe(ActionStatus::Pending)
        ->and($step->fresh()->retry_signal_attempts)->toBe(1);
});

/**
 * A row can leave Failed for a retry and come back to it, so a status-only fence cannot
 * tell one generation from the next. A queue hook holding the spent cycle would end the
 * live one with a fallback nobody asked for.
 */
it('refuses a give-up from a cycle the row has already left and returned from', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::Failed, [
        'continue_on_failure' => true,
        'retry_signal' => 'balance-refilled',
        'retry_signal_attempts' => 0,
    ]);

    // The old failed() hook resolved the row on cycle 0 and has not written yet.
    $spentCycle = ActionRun::query()->findOrFail($step->id);

    $recorder = app(ActionRecorder::class);

    // A signal pays for cycle 1, whose attempt then fails back to Failed — the same
    // status the hook is holding, one generation on.
    $recorder->retryAction(ActionRun::query()->findOrFail($step->id));
    ActionRun::query()->whereKey($step->id)->update(['status' => ActionStatus::Failed]);

    expect($recorder->optionalFail($spentCycle))->toBeFalse()
        ->and($step->fresh()->status)->toBe(ActionStatus::Failed)
        ->and($step->fresh()->retry_signal_attempts)->toBe(1);
});

it('writes no retry state when a cancellation lands before the park', function () {
    FlakyPaymentAction::reset(failures: 5);

    // The cancellation the issue describes: a separate request landing between the
    // failure being recorded and the park committing. A listener puts it exactly there.
    $cancelled = null;

    Event::listen(ActionFailed::class, function (ActionFailed $event) use (&$cancelled): void {
        $cancelled = $event->actionRun->flow_run_id;

        SagaFlow::findRun($cancelled)->markCancelled();
    });

    try {
        SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-probe')->runSync();
    } catch (Throwable) {
        // The cancellation is the setup; the rows it leaves behind are the assertion.
    }

    $run = FlowRun::query()->findOrFail($cancelled);

    expect($run->status)->toBe(FlowStatus::Cancelled)
        ->and($run->actions()->pluck('status')->all())->not->toContain(ActionStatus::AwaitingRetry)
        ->and($run->signals()->pluck('status')->all())->not->toContain(SignalStatus::Waiting)
        ->and($run->events()->pluck('type')->all())->not->toContain('action.awaiting_retry');
});

it('writes no retry state when the run ends during the replay that parks, on the queue', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 5);
    SelfCancellingBeforeParkWorkflow::$armed = true;

    // A cancellation arriving before this replay could not reach the park at all: the
    // drive loop transitions to Running first, which no terminal status allows. So the
    // run is ended from inside the pass that is about to park — the window the queued
    // path really has, staged in one process.
    $handle = SagaFlow::create(SelfCancellingBeforeParkWorkflow::class)
        ->withArguments('order-probe')
        ->run();

    try {
        drainQueue();
    } catch (Throwable) {
        // The cancellation is the setup; the rows it leaves behind are the assertion.
    }

    $run = FlowRun::query()->findOrFail($handle->id);

    expect($run->status)->toBe(FlowStatus::Cancelled)
        ->and($run->actions()->pluck('status')->all())->not->toContain(ActionStatus::AwaitingRetry)
        ->and($run->signals()->pluck('status')->all())->not->toContain(SignalStatus::Waiting)
        ->and($run->events()->pluck('type')->all())->not->toContain('action.awaiting_retry');
});

/**
 * Two replays reading the same failed generation both try to park it. Only one can
 * win, and the one that loses must not carry the state it failed to write into the
 * next decision it makes — it holds a model saying AwaitingRetry over a row the winner
 * parked, and the give-up that follows would take that row and orphan the winner's wait.
 */
it('does not let a losing park take the row the winning park wrote', function () {
    $run = fencedFlowRun();
    $step = fencedStep($run, ActionStatus::Failed, [
        'continue_on_failure' => true,
        'retry_signal' => 'balance-refilled',
    ]);

    $winner = ActionRun::query()->findOrFail($step->id);
    $loser = ActionRun::query()->findOrFail($step->id);

    $recorder = app(ActionRecorder::class);

    expect($recorder->awaitRetry($winner, 'balance-refilled', null))->toBeTrue()
        ->and($recorder->awaitRetry($loser, 'balance-refilled', null))->toBeFalse();

    // A refused write leaves the model saying what the database says, so nothing it
    // failed to write is carried into the next one.
    expect($loser->status)->toBe(ActionStatus::Failed)
        ->and($loser->getDirty())->toBe([]);

    // The give-up the losing replay would go on to record must find the row it read,
    // not the row the winner parked.
    expect($recorder->optionalFail($loser))->toBeFalse()
        ->and($step->fresh()->status)->toBe(ActionStatus::AwaitingRetry);
});
