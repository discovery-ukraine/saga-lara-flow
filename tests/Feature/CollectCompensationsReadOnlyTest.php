<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalRetryCompensatedWorkflow;

/**
 * collectCompensations() is the read half of a rollback: it replays handle() only to
 * learn what has to be undone. It wrote, though — an optional step whose retry budget
 * was spent was settled by the replay that was only supposed to look at it, and the
 * give-up was recorded by a pass that does not own the row.
 */
beforeEach(fn () => CompensationLog::reset());

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
