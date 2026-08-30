<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;

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
