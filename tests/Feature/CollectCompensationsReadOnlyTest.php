<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalRetryCompensatedWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\VanishedArgumentWorkflow;

/**
 * collectCompensations() is the read half of a rollback: it replays handle() only to
 * learn what has to be undone. Two things used to break that. It wrote — an optional
 * step whose retry budget was spent was settled by the replay that was only supposed
 * to look at it. And it decided by catching, which cannot tell the frontier it was
 * looking for from a fault it was not: any throw ended the stack, and compensate()
 * rolled back the truncated result and reported a complete unwind.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    VanishedArgumentWorkflow::reset();
});

/**
 * The state the engine reaches on its own: the queue gave up on this cycle and the
 * retry budget is spent, but no ordinary replay has resolved the row yet. Whoever
 * calls compensate() at that moment gets there first.
 */
function spentOptionalRetry(): array
{
    FlakyPaymentAction::reset(failures: 99);
    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);

    $run = SagaFlow::create(OptionalRetryCompensatedWorkflow::class)
        ->withArguments('order-1')
        ->runSync();

    $step = $run->actions()->where('sequence', 0)->first();
    $step->status = ActionStatus::Failed;
    $step->queue_attempts_exhausted = true;
    $step->retry_signal_max_attempts = 0;
    $step->save();

    return [$run, $step->fresh()];
}

it('collects the compensation of a spent optional step without writing to it', function () {
    [$run, $step] = spentOptionalRetry();

    $before = $step->getAttributes();
    $eventsBefore = $run->events()->count();

    $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    // The replay resolved the give-up rather than stopping at it: the step's own
    // compensation is in the stack, which is the only way past that fork.
    expect($entries)->toHaveCount(1)
        ->and($entries[0]->sequence)->toBe(0)
        ->and($run->actions()->where('sequence', 0)->first()->getAttributes())->toBe($before)
        ->and($run->fresh()->events()->count())->toBe($eventsBefore);
});

it('leaves the optional_failed write to the next ordinary replay', function () {
    [$run] = spentOptionalRetry();

    app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    // Not withheld, only deferred: the pass that owns the row still records it.
    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Sync);

    expect($run->actions()->where('sequence', 0)->first()->status)
        ->toBe(ActionStatus::OptionalFailed);
});

it('refuses to report a rollback it could not finish planning', function () {
    $run = SagaFlow::create(VanishedArgumentWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting)
        ->and($run->actions()->count())->toBe(2);

    // Whatever the second step's argument was read from is gone by the time the
    // operator rolls back. The replay now throws where the original pass did not.
    VanishedArgumentWorkflow::$label = null;

    expect(fn () => SagaFlow::loadFlow($run->id)->compensate())
        ->toThrow(RuntimeException::class, 'the order this run was built from is gone');

    // Nothing half-done: the fault surfaces before the run is touched, so the
    // rollback is still there to be retried once the cause is fixed. The runtime is
    // a singleton, so leaving by throwing has to unbind it too — a pass that stayed
    // in collecting mode would park the next ordinary drive of any run at its first
    // unscheduled step.
    expect(CompensationLog::all())->toBe([])
        ->and($run->fresh()->status)->toBe(FlowStatus::Waiting)
        ->and(app(FlowRuntime::class)->isCollecting())->toBeFalse();

    VanishedArgumentWorkflow::reset();

    expect(SagaFlow::loadFlow($run->id)->compensate()->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});
