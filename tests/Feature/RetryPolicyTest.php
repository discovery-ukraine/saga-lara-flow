<?php

use DiscoveryUkraine\SagaLaraFlow\Builders\ActionBuilder;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Retry\RecordedFailure;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\DeclinableChargeAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ManualCompensateWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\NestedCompensateRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\NestedDriveRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RecordingRetryPolicy;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ReentrantRetryCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalSagaGroupWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryPolicySagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryWhenWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SelfCancellingRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SuspendingRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TaggingRetryWorkflow;
use Illuminate\Support\Facades\Log;

/**
 * Sync-mode coverage for the retry policy object and the when: shorthand — the
 * fourth gate, which decides whether a failure is worth parking at all. Waking the
 * run on delivery is switched off so the signal does not queue a resume: these tests
 * drive the run again explicitly, keeping the inline path under test.
 */
beforeEach(function () {
    DeclinableChargeAction::reset();
    RecordingRetryPolicy::reset();
    CompensationLog::reset();

    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);
});

function refillAndDriveRun(FlowRun $run): FlowRun
{
    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    return app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Sync);
}

it('parks the step when the policy allows the failure', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-1')->runSync();

    $step = $run->actions()->where('sequence', 1)->first();

    expect($run->status)->toBe(FlowStatus::Waiting)
        ->and($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal)->toBe('balance-refilled')
        ->and(RecordingRetryPolicy::calls())->toBe(1)
        ->and(CompensationLog::all())->toBe([]);
});

it('fails the step exactly as a policy-less step when shouldRetry refuses', function () {
    DeclinableChargeAction::reset(failures: 99, code: 422);
    RecordingRetryPolicy::$refuseCode = 422;

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-2')->runSync();

    $step = $run->actions()->where('sequence', 1)->first();

    // The same outcome as a step that never carried a policy: hard failure, no wait
    // marker advertising a signal nobody will act on, and the completed step before
    // it rolled back.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($step->status)->toBe(ActionStatus::Failed)
        ->and($run->signals()->count())->toBe(0)
        ->and(CompensationLog::all())->toBe(['undo:created'])
        ->and(DeclinableChargeAction::$calls)->toBe(1);
});

it('hands the policy the failure recorded on the row', function () {
    DeclinableChargeAction::reset(failures: 99, code: 402);

    SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-3')->runSync();

    $context = RecordingRetryPolicy::last();

    expect($context->actionClass)->toBe(DeclinableChargeAction::class)
        ->and($context->workflowClass)->toBe(RetryPolicyWorkflow::class)
        ->and($context->sequence)->toBe(1)
        ->and($context->signal)->toBe('balance-refilled')
        ->and($context->failure->class)->toBe(RuntimeException::class)
        ->and($context->failure->message)->toBe('charge declined')
        ->and($context->failure->code)->toBe(402)
        ->and($context->failure->is(RuntimeException::class))->toBeTrue()
        ->and($context->failure->is(LogicException::class))->toBeFalse();
});

it('counts spent cycles rather than numbering attempts', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);
    RecordingRetryPolicy::$maxRetries = 3;

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-4')->runSync();

    // Nothing has been spent yet the first time the step parks.
    expect(RecordingRetryPolicy::last()->cyclesSpent)->toBe(0)
        ->and(RecordingRetryPolicy::last()->cap)->toBe(3)
        ->and(RecordingRetryPolicy::last()->executions)->toBe(1);

    refillAndDriveRun($run);

    expect(RecordingRetryPolicy::last()->cyclesSpent)->toBe(1)
        ->and(RecordingRetryPolicy::last()->cap)->toBe(3)
        ->and(RecordingRetryPolicy::last()->executions)->toBe(2);
});

it('never asks the policy once the budget is spent', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);
    RecordingRetryPolicy::$maxRetries = 0;

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-5')->runSync();

    // The three structural gates short-circuit before the one that runs user code.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(RecordingRetryPolicy::calls())->toBe(0);
});

it('never asks the policy about a failure outside only', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);
    RecordingRetryPolicy::$only = [LogicException::class];

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-6')->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(RecordingRetryPolicy::calls())->toBe(0);
});

it('reads a policy that throws as a refusal and records it', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);
    RecordingRetryPolicy::$throws = true;

    $logged = [];

    Log::listen(function ($message) use (&$logged) {
        $logged[] = $message->message;
    });

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-7')->runSync();

    // A broken predicate fails the STEP, not the run on its own terms: the run
    // ends carrying the step's business failure, exactly as it would have without
    // a policy — the policy's own exception never becomes the run's.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(ActionFailedException::class)
        ->and($run->exception['message'] ?? '')->toContain('charge declined')
        ->and($run->exception['message'] ?? '')->not->toContain('the policy itself is broken')
        ->and($logged)->toContain('saga-lara-flow: retry_policy_threw')
        ->and(CompensationLog::all())->toBe(['undo:created']);
});

it('wakes a parked step without asking the policy again', function () {
    DeclinableChargeAction::reset(failures: 1, code: 503);

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-8')->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting)
        ->and(RecordingRetryPolicy::calls())->toBe(1);

    // The gate decides whether to PARK, not whether to wake. A policy that would
    // now refuse must not swallow a delivery the run is already advertising.
    RecordingRetryPolicy::$throws = true;

    $final = refillAndDriveRun($run);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and(RecordingRetryPolicy::calls())->toBe(1);
});

it('applies the policy to a step inside a saga group', function () {
    DeclinableChargeAction::reset(failures: 99, code: 422);
    RecordingRetryPolicy::$refuseCode = 422;

    $run = SagaFlow::create(RetryPolicySagaWorkflow::class)->withArguments('order-9')->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(RecordingRetryPolicy::calls())->toBe(1)
        ->and(CompensationLog::all())->toBe(['undo:created']);
});

it('decides the same way through the when shorthand', function () {
    DeclinableChargeAction::reset(failures: 99, code: 422);

    $refused = SagaFlow::create(RetryWhenWorkflow::class)->withArguments('order-10', 422)->runSync();

    expect($refused->status)->toBe(FlowStatus::Failed)
        ->and($refused->signals()->count())->toBe(0);

    DeclinableChargeAction::reset(failures: 99, code: 503);

    $parked = SagaFlow::create(RetryWhenWorkflow::class)->withArguments('order-11', 422)->runSync();

    expect($parked->status)->toBe(FlowStatus::Waiting)
        ->and($parked->actions()->where('sequence', 0)->first()->status)
        ->toBe(ActionStatus::AwaitingRetry);
});

it('refuses a policy combined with the arguments it replaces', function () {
    $action = new ActionBuilder(app(FlowRuntime::class), DeclinableChargeAction::class, []);

    expect(fn () => $action->retryOnSignal(new RecordingRetryPolicy, maxRetries: 3))
        ->toThrow(InvalidArgumentException::class, 'drop maxRetries')
        ->and(fn () => $action->retryOnSignal(new RecordingRetryPolicy, only: [LogicException::class], when: fn () => true))
        ->toThrow(InvalidArgumentException::class, 'drop only, when');
});

it('holds a saga step misconfiguration back until the group replays', function () {
    CompensationLog::reset();

    $run = SagaFlow::create(RetryOnSignalSagaGroupWorkflow::class)
        ->withArguments('order-saga-neg', -1)
        ->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(InvalidArgumentException::class)
        ->and($run->exception['message'] ?? '')->toContain('maxRetries must be zero or greater');

    // The refusal has to arrive from the step's own execute(), not from the fluent
    // chain that builds the group: SagaBuilder::run() replays the steps in order, so
    // refusing while the chain is still being assembled would leave the completed
    // first step with no compensation registered and roll back nothing.
    expect(CompensationLog::all())->toBe(['undo:created']);
});

it('stores nothing about the policy on the row [pinning]', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);
    RecordingRetryPolicy::$maxRetries = 2;

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-12')->runSync();

    $step = $run->actions()->where('sequence', 1)->first();

    // Only the signal name and the ceiling are persisted, exactly as for the scalar
    // form. The policy is reconstructed by handle() on every replay, so a class name
    // in the row would be a second source of truth nothing keeps in step.
    $columns = $step->getAttributes();

    expect($step->retry_signal)->toBe('balance-refilled')
        ->and($step->retry_signal_max_attempts)->toBe(2)
        ->and(json_encode($columns))->not->toContain('RecordingRetryPolicy');
});

it('builds a recorded failure from a partial or absent record', function () {
    $absent = RecordedFailure::fromRecord(null);

    expect($absent->class)->toBeNull()
        ->and($absent->message)->toBe('')
        ->and($absent->code)->toBe(0)
        ->and($absent->is(RuntimeException::class))->toBeFalse();

    $partial = RecordedFailure::fromRecord(['message' => 'no class here']);

    expect($partial->class)->toBeNull()
        ->and($partial->message)->toBe('no class here');

    $stringCode = RecordedFailure::fromRecord([
        'class' => RuntimeException::class,
        'message' => 'pdo style',
        'code' => 'HY000',
    ]);

    expect($stringCode->code)->toBe('HY000')
        ->and($stringCode->is(LogicException::class, RuntimeException::class))->toBeTrue();
});

it('holds the cap on the row but re-reads the signal name [pinning]', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);
    RecordingRetryPolicy::$maxRetries = 3;

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-13')->runSync();

    $step = $run->actions()->where('sequence', 1)->first();

    expect($step->retry_signal)->toBe('balance-refilled')
        ->and($step->retry_signal_max_attempts)->toBe(3);

    // A deploy that rewrites the policy. Only one of the two survives it.
    RecordingRetryPolicy::$signalName = 'card-replaced';
    RecordingRetryPolicy::$maxRetries = 99;

    refillAndDriveRun($run);

    $step = $run->actions()->where('sequence', 1)->first();

    // The ceiling is read back off the row, so the deploy cannot move it. The signal
    // name is not: the seam uses whatever the workflow asks for now and rewrites the
    // column with it, so the step is left waiting on a name nobody has been told to
    // deliver, and the marker for the old one is spent.
    expect($step->retry_signal_max_attempts)->toBe(3)
        ->and(RecordingRetryPolicy::last()->cap)->toBe(3)
        ->and($step->retry_signal)->toBe('card-replaced')
        ->and($run->signals()->where('status', SignalStatus::Waiting)->pluck('name')->all())
        ->toBe(['card-replaced']);
});

it('refuses a predicate that reaches back into the workflow', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);

    $logged = [];

    Log::listen(function ($message) use (&$logged) {
        $logged[] = $message->message;
    });

    $run = SagaFlow::create(SuspendingRetryWorkflow::class)->withArguments('order-14')->runSync();

    // Refused before the seam took an ordinal, so no wait was recorded for a
    // predicate nobody would replay. Left to run, it would take the ordinal the step
    // after it needs — and once the guarded step succeeded the predicate would never
    // be asked again, so the run would fail claiming the workflow code had changed.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and($run->exception['message'] ?? '')->toContain('must be a pure function')
        ->and($run->signals()->count())->toBe(0)
        ->and($run->actions()->count())->toBe(1);

    // Not a policy defect with a safe answer: it is not absorbed and not logged as one.
    expect($logged)->not->toContain('saga-lara-flow: retry_policy_threw');
});

it('never asks the policy while planning a compensation', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-15')->runSync();

    // The state a queued step reaches when its job gave up and the resume was lost:
    // Failed, budget untouched so every cheap gate passes, nothing left to drive it.
    $step = $run->actions()->where('sequence', 1)->first();
    $step->status = ActionStatus::Failed;
    $step->queue_attempts_exhausted = true;
    $step->save();

    $asked = RecordingRetryPolicy::calls();

    app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    // collectCompensations() promises to replay without running business logic. A
    // predicate is business logic, and its answer cannot change where the planning
    // stops anyway.
    expect(RecordingRetryPolicy::calls())->toBe($asked);
});

it('refuses a predicate that writes to the run it is deciding for', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);

    $run = SagaFlow::create(SelfCancellingRetryWorkflow::class)->withArguments('order-16')->runSync();

    // Left to run, the cancellation would settle the run's open rows and the seam
    // would park the step immediately afterwards — a live wait advertising a signal
    // on a run nobody will drive again.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and($run->signals()->count())->toBe(0)
        ->and($run->actions()->where('status', ActionStatus::AwaitingRetry)->count())->toBe(0);
});

it('still compensates the failed step when its predicate re-enters', function () {
    DeclinableChargeAction::reset(failures: 99);

    $run = SagaFlow::create(ReentrantRetryCompensationWorkflow::class)
        ->withArguments('order-17')
        ->runSync();

    // The rollback is built from the failing pass alone, and the re-entry throw
    // leaves that pass before the step is resolved. Its own compensation has to be
    // registered on the way out, or the caller's compensateStepOnSelfFailure() is
    // the one instruction the run drops.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and(CompensationLog::all())->toBe(['undo:charge', 'undo:a']);
});

it('refuses a predicate that tags the run it is deciding for', function () {
    DeclinableChargeAction::reset(failures: 99);

    $run = SagaFlow::create(TaggingRetryWorkflow::class)->withArguments('order-18')->runSync();

    // tag() is the only workflow-facing write that never asks for an ordinal, so
    // the guard in nextSequence() never sees it and it needs its own.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and($run->tags()->count())->toBe(0);
});

it('holds the re-entry guard when the container forgets its scoped instances', function () {
    DeclinableChargeAction::reset(failures: 99, code: 503);

    // What a queue worker does between jobs: FlowExecutor is a singleton and keeps
    // the runtime it was built with, while a fresh resolve now answers for another
    // instance. The guard has to read the one the pass is actually driven with.
    $held = (fn () => $this->runtime)->call(app(FlowExecutor::class));
    app()->forgetScopedInstances();
    expect(app(FlowRuntime::class))->not->toBe($held);

    $run = SagaFlow::create(SelfCancellingRetryWorkflow::class)->withArguments('order-19')->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and($run->actions()->where('status', ActionStatus::AwaitingRetry)->count())->toBe(0)
        ->and($run->signals()->count())->toBe(0);
});

it('reads the policy config on every replay but asks the predicate only of a failure [pinning]', function () {
    DeclinableChargeAction::reset(failures: 1);

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-22')->runSync();
    $run = refillAndDriveRun($run);

    // Pins what the docs promise: handle() rebuilds the policy on every pass and the
    // builder reads its configuration off it again, a completed step included, while
    // the predicate is only ever put to a step that has actually failed.
    expect($run->status)->toBe(FlowStatus::Completed)
        ->and(RecordingRetryPolicy::calls())->toBe(1)
        ->and(RecordingRetryPolicy::$configReads)->toBeGreaterThan(RecordingRetryPolicy::calls());
});

it('refuses a predicate that drives another run', function () {
    DeclinableChargeAction::reset(failures: 99);

    $run = SagaFlow::create(NestedDriveRetryWorkflow::class)->withArguments('order-20')->runSync();

    // There is one runtime behind the singleton executor: a nested pass rebinds and
    // resets it, then clears it on the way out, leaving the deciding pass with no
    // bound run at all. The target being a different run is no protection.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and($run->signals()->count())->toBe(0)
        ->and($run->actions()->where('status', ActionStatus::AwaitingRetry)->count())->toBe(0);
});

it('refuses a predicate that compensates another run', function () {
    DeclinableChargeAction::reset(failures: 99);

    // Non-terminal, so compensate() gets past its own terminal check and reaches
    // the executor — which is the path under test.
    $other = SagaFlow::create(ManualCompensateWorkflow::class)->runSync();
    expect($other->status)->toBe(FlowStatus::Waiting);

    $run = SagaFlow::create(NestedCompensateRetryWorkflow::class)
        ->withArguments('order-21', $other->id)
        ->runSync();

    // compensate() reaches the same executor by a compensation-only replay, so it
    // is refused on the same grounds — and the other run is left as it was.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and(SagaFlow::findRun($other->id)->status)->toBe(FlowStatus::Waiting)
        ->and(CompensationLog::all())->toBe([]);
});
