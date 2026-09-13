<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ActionStartsSagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\DeclinableChargeAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ManualCompensateWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\NestedExpireRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\NestedRunSyncWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ProbingRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\StartsASagaAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;

/**
 * A run driven while another is already being driven — a step that starts a saga of
 * its own, or handle() calling runSync() where child() belongs. The inner pass gets
 * a runtime of its own, so the ordinals, the compensation stack and the bound run of
 * the pass that made the call are still there when it returns.
 */
beforeEach(function () {
    CompensationLog::reset();
    DeclinableChargeAction::reset();
    ProbingRetryWorkflow::reset();
});

/**
 * @return list<string>
 */
function nestedDriveStepClasses(FlowRun $run): array
{
    return $run->actions()->orderBy('sequence')->pluck('action_class')->all();
}

function nestedDriveRunCount(string $workflowClass): int
{
    return FlowRun::query()->where('workflow_class', $workflowClass)->count();
}

it('keeps the ordinals of the pass that drove another run', function () {
    $run = SagaFlow::create(NestedRunSyncWorkflow::class)->runSync();

    // Every seam after the nested call still lands in the slot the workflow asks for.
    // A pass whose counter was rewound reaches the second step with an ordinal that
    // already belongs to the first, and dies blaming a workflow change.
    expect(nestedDriveStepClasses($run))->toBe([
        MakeValueAction::class,
        MakeValueAction::class,
        ThrowingAction::class,
    ]);
});

it('rolls back both steps around a run it drove', function () {
    $run = SagaFlow::create(NestedRunSyncWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves the caller a run to fail against', function () {
    $run = SagaFlow::create(NestedRunSyncWorkflow::class)->runSync();

    // The nested pass unbinds its own run on the way out. Taking the caller's with it
    // ends the outer pass on the next seam, on a fault that names no cause the
    // operator can act on.
    expect($run->exception['class'] ?? null)->toBe(RuntimeException::class)
        ->and($run->exception['message'] ?? null)->toBe('boom');
});

it('starts a run it cannot resolve again on every replay [pinning]', function () {
    SagaFlow::create(NestedRunSyncWorkflow::class)->runSync();

    // The pass survives the call; the call stays a mistake.
    expect(nestedDriveRunCount(OneActionWorkflow::class))->toBeGreaterThan(1);
});

it('drives a saga started by a step without disturbing the run that owns it', function () {
    $run = SagaFlow::create(ActionStartsSagaWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(nestedDriveStepClasses($run))->toBe([
            MakeValueAction::class,
            StartsASagaAction::class,
            ThrowingAction::class,
        ])
        ->and(CompensationLog::all())->toBe(['undo:inner', 'undo:a']);
});

it('completes the saga a step started', function () {
    SagaFlow::create(ActionStartsSagaWorkflow::class)->runSync();

    $started = FlowRun::query()->where('workflow_class', OneActionWorkflow::class)->first();

    // The contrast with a bare runSync(): the seam that starts this one is history.
    expect(nestedDriveRunCount(OneActionWorkflow::class))->toBe(1)
        ->and($started->status)->toBe(FlowStatus::Completed)
        ->and($started->actions()->where('status', ActionStatus::Completed)->count())->toBe(1);
});

it('answers the re-entry guard from the pass, not from the container', function () {
    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);
    DeclinableChargeAction::reset(failures: 99, code: 503);

    SagaFlow::create(ProbingRetryWorkflow::class)->withArguments('order-probe')->runSync();

    // What the container hands out is bound to nothing, which is why the guard asks
    // the executor for the answer instead.
    expect(ProbingRetryWorkflow::$probe)->toBe([
        'guarded' => true,
        'container_bound' => false,
    ]);
});

it('drops the runtime of a pass that has ended', function () {
    SagaFlow::create(ActionStartsSagaWorkflow::class)->runSync();
    SagaFlow::create(OneActionWorkflow::class)->runSync();

    // The register the guards read is also everything a pass keeps alive. A worker
    // that never drops one holds on to every run it has ever driven, and answers
    // for decisions that ended long ago.
    $inFlight = (fn () => $this->runtimes)->call(app(FlowExecutor::class));

    expect($inFlight)->toBe([]);
});

it('shares no runtime between container resolves', function () {
    // The executor makes its own for every pass it drives, so a registration here
    // would only hand two unrelated callers one object to write flow state into.
    expect(app(FlowRuntime::class))->not->toBe(app(FlowRuntime::class));
});

it('refuses a predicate that plans a rollback through the executor', function (bool $replan) {
    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);
    DeclinableChargeAction::reset(failures: 99);

    // Non-terminal, so the run is one the executor will act on rather than turn away.
    $other = SagaFlow::create(ManualCompensateWorkflow::class)->runSync();

    $run = SagaFlow::create(NestedExpireRetryWorkflow::class)
        ->withArguments('order-expire', $other->id, $replan)
        ->runSync();

    // Both entries plan by replaying, and both wrap what the replay throws. Refused
    // at the door instead, the predicate gets the re-entry itself rather than a
    // failure that reads as the policy simply declining.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and(SagaFlow::findRun($other->id)->status)->toBe(FlowStatus::Waiting)
        ->and(CompensationLog::all())->toBe([]);
})->with([[false], [true]]);
