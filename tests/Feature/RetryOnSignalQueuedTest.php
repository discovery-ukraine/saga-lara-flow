<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionRetried;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowSignalConsumed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowSignalReceived;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunActionJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\HistoryContractGuard;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalRecorder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\DeclinableChargeAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailingWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalFallbackWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalRetryOnSignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RecordingRetryPolicy;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryBudgetSagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UnreliablePaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UnreliableRetryOnSignalWorkflow;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * Queued-mode coverage for retryOnSignal(): the real RunActionJob →
 * ResumeWorkflowJob path, the deadlines the monitor enforces, the budget running
 * out, and the races between an externally delivered signal and the seam.
 */
beforeEach(function () {
    FlakyPaymentAction::reset();
    UnreliablePaymentAction::reset();
    DeclinableChargeAction::reset();
    RecordingRetryPolicy::reset();
    CompensationLog::reset();
});

/**
 * Process exactly one queued job, so a test can inspect the run between two steps
 * of the async path instead of after the whole queue has drained.
 */
function workOneJob(): void
{
    // --sleep=0: an empty queue would otherwise cost the worker's default three
    // seconds, and these helpers are called in loops.
    Artisan::call('queue:work', ['--once' => true, '--sleep' => 0, '--no-interaction' => true]);
}

/**
 * Drive the queue one job at a time until the step at $sequence satisfies $ready, so
 * a test can stop between two jobs instead of after the whole queue has drained.
 */
function workUntil(FlowRun $run, int $sequence, Closure $ready): void
{
    for ($i = 0; $i < 20; $i++) {
        $step = $run->actions()->where('sequence', $sequence)->first();

        if ($step !== null && $ready($step)) {
            return;
        }

        workOneJob();
    }

    throw new RuntimeException('the step never reached the state this test waits for');
}

function workUntilStatus(FlowRun $run, int $sequence, ActionStatus $status): void
{
    workUntil($run, $sequence, fn (ActionRun $step): bool => $step->status === $status);
}

it('retries the parked step through the real queue', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q1')->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);

    expect($run->status)->toBe(FlowStatus::Waiting)
        ->and($run->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::AwaitingRetry);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['charged']['calls'] ?? null)->toBe(2);

    $actions = $final->actions()->orderBy('sequence')->get();

    // The retried step kept its own ordinal and its own row; nothing around it moved
    // or ran a second time.
    expect($actions)->toHaveCount(3)
        ->and($actions[1]->sequence)->toBe(1)
        ->and($actions[1]->retry_signal_attempts)->toBe(1)
        ->and($actions[1]->attempts)->toBe(2)
        ->and($actions[0]->attempts)->toBe(1)
        ->and($actions[2]->sequence)->toBe(2)
        ->and(CompensationLog::all())->toBe([]);
});

it('gives up and rolls the saga back once the retry budget is spent', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryBudgetSagaWorkflow::class)->withArguments('order-q2', 1)->run();
    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    // One retry was allowed, it failed too, and the step then failed exactly as it
    // would have without any retry policy.
    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($final->exception['class'] ?? null)->toBe(ActionFailedException::class)
        ->and($final->exception['message'] ?? '')->toContain('insufficient balance')
        ->and(FlakyPaymentAction::$calls)->toBe(2);

    $step = $final->actions()->where('sequence', 2)->first();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and($step->retry_signal_attempts)->toBe(1);

    // Reverse order, and the failed step itself is not compensated by default.
    expect(CompensationLog::all())->toBe(['undo:packed', 'undo:reserved']);
});

it('gives up when the wait deadline passes', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-q3', null, null, 60)
        ->run();
    drainQueue();

    $marker = SagaFlow::findRun($run->id)->signals()->first();

    expect($marker->status)->toBe(SignalStatus::Waiting)
        ->and($marker->timeout_at)->not->toBeNull();

    $this->travel(120)->seconds();

    expect(app(FlowMonitor::class)->sweep()['signals'])->toBe(1);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($final->exception['class'] ?? null)->toBe(ActionFailedException::class)
        ->and($final->exception['message'] ?? '')->toContain('insufficient balance')
        ->and($final->signals()->first()->status)->toBe(SignalStatus::TimedOut)
        ->and(CompensationLog::all())->toBe(['undo:created'])
        // The step gave up: it is Failed, not still claiming to wait for a signal on
        // a run that has already compensated.
        ->and($final->actions()->where('sequence', 1)->first()->status)
        ->toBe(ActionStatus::Failed);
});

it('parks when the failure matches only by subclass', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)
        ->withArguments('order-q4', null, [Exception::class])
        ->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    // RuntimeException is not listed, but it is an Exception — is_a walks the
    // hierarchy, so the policy applies.
    expect($final->status)->toBe(FlowStatus::Waiting)
        ->and($final->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::AwaitingRetry)
        ->and(CompensationLog::all())->toBe([]);
});

it('reaches the optional fallback only after the retry budget is spent', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(OptionalRetryOnSignalWorkflow::class)
        ->withArguments('order-q5', 1)
        ->run();
    drainQueue();

    $parked = SagaFlow::findRun($run->id);

    // continueOnFailure() does not short-circuit the policy: the step parks first
    // and the downstream step has not been scheduled yet.
    expect($parked->status)->toBe(FlowStatus::Waiting)
        ->and($parked->actions()->count())->toBe(1)
        ->and($parked->actions()->first()->status)->toBe(ActionStatus::AwaitingRetry);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['charged'] ?? null)->toBe('unpaid');

    $step = $final->actions()->where('sequence', 0)->first();

    expect($step->status)->toBe(ActionStatus::OptionalFailed)
        ->and($step->retry_signal_attempts)->toBe(1);
});

it('leaves the same ordinals behind whether or not the step was retried', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 2);

    $retried = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q6')->run();
    drainQueue();

    SagaFlow::loadFlow($retried->id)->signal('balance-refilled');
    drainQueue();

    SagaFlow::loadFlow($retried->id)->signal('balance-refilled');
    drainQueue();

    FlakyPaymentAction::reset();

    $straight = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q6')->run();
    drainQueue();

    $withRetries = runStateSnapshot($retried->id);
    $withoutRetries = runStateSnapshot($straight->id);

    $shape = fn (array $snapshot): array => [
        'status' => $snapshot['status'],
        'actions' => array_map(
            fn (array $action): array => [
                'sequence' => $action['sequence'],
                'status' => $action['status'],
                'action_class' => $action['action_class'],
            ],
            $snapshot['actions'],
        ),
    ];

    // Two retry cycles leave no trace in the ordinals: the downstream step sits at
    // the same sequence it would have without them.
    expect($shape($withRetries))->toBe($shape($withoutRetries));
});

it('does not lose a signal delivered between the failure and the parking', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q7')->run();

    // Stop right after the attempt failed, before the resume that would park it.
    workUntilStatus($run, 1, ActionStatus::Failed);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    expect($run->signals()->where('status', SignalStatus::Received)->count())->toBe(1);

    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->actions()->where('sequence', 1)->first()->retry_signal_attempts)->toBe(1);

    // The seam took the floating signal instead of parking, so no wait marker was
    // ever written and the step never went AwaitingRetry.
    $signals = $final->signals()->get();

    expect($signals)->toHaveCount(1)
        ->and($signals[0]->status)->toBe(SignalStatus::Consumed)
        ->and($signals[0]->wait_sequence)->toBe(1)
        ->and($final->events()->where('type', FlowEventType::ActionAwaitingRetry->value)->count())->toBe(0);
});

it('ignores a floating signal that predates the failed attempt', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q8')->run();

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    // Age it: this signal belongs to something that happened before the attempt that
    // is about to fail, so it must not be claimed by this step's retry.
    $run->signals()->first()->update(['received_at' => now()->subMinute()]);

    drainQueue();

    $final = SagaFlow::findRun($run->id);
    $step = $final->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal_attempts)->toBe(0)
        ->and(FlakyPaymentAction::$calls)->toBe(1);

    $signals = $final->signals()->orderBy('id')->get();

    expect($signals)->toHaveCount(2)
        ->and($signals[0]->status)->toBe(SignalStatus::Received)
        ->and($signals[0]->wait_sequence)->toBeNull()
        ->and($signals[1]->status)->toBe(SignalStatus::Waiting);
});

it('takes a floating signal that landed after the wait marker was written', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q9')->run();
    drainQueue();

    $parked = SagaFlow::findRun($run->id);

    expect($parked->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::AwaitingRetry);

    // Reproduce the delivery that raced the marker: deliver() looked for a waiting
    // marker before the seam had written one, so the signal landed floating.
    app(SignalRecorder::class)->storeReceivedSignal($parked, 'balance-refilled', []);

    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->actions()->where('sequence', 1)->first()->retry_signal_attempts)->toBe(1);

    // Both rows end Consumed at this step's ordinal: the marker because its wait is
    // over, the floating row because it is the delivery that ended it.
    $signals = $final->signals()->orderBy('id')->get();

    expect($signals)->toHaveCount(2)
        ->and($signals->pluck('status')->all())->toBe([SignalStatus::Consumed, SignalStatus::Consumed])
        ->and($signals->pluck('wait_sequence')->all())->toBe([1, 1]);
});

it('waits for the queue to run out of attempts before spending a retry cycle', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(UnreliableRetryOnSignalWorkflow::class)->withArguments('order-q10')->run();

    // Stop after the first of the action's two native attempts.
    workUntilStatus($run, 0, ActionStatus::Failed);

    $step = $run->actions()->first();

    expect($step->attempts)->toBe(1)
        ->and(UnreliablePaymentAction::$calls)->toBe(1);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    // Age it so it unambiguously predates the attempt still to come: this test is
    // about the gate, not about which attempt may claim a borderline signal.
    $run->signals()->first()->update(['received_at' => now()->subMinute()]);

    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);

    $step->refresh();

    // The queue still owes this step an attempt, so the seam waited instead of
    // spending a retry cycle on it. Nothing has told it the queue is done.
    expect($step->status)->toBe(ActionStatus::Failed)
        ->and($step->queue_attempts_exhausted)->toBeFalse()
        ->and($step->retry_signal_attempts)->toBe(0)
        ->and($run->signals()->where('status', SignalStatus::Waiting)->count())->toBe(0);

    drainQueue();

    $step->refresh();

    // The second native attempt ran and failed; only then did the step park — and
    // the now-stale floating signal was not claimed by it.
    expect(UnreliablePaymentAction::$calls)->toBe(2)
        ->and($step->attempts)->toBe(2)
        ->and($step->queue_attempts_exhausted)->toBeTrue()
        ->and($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal_attempts)->toBe(0);
});

it('ignores a queued job left over from an earlier retry cycle', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q11')->run();
    drainQueue();

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    drainQueue();

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal_attempts)->toBe(1)
        ->and(FlakyPaymentAction::$calls)->toBe(2);

    // A job from the cycle before this one: even with the row back at Pending it must
    // not execute the step again.
    $step->status = ActionStatus::Pending;
    $step->save();

    RunActionJob::dispatch($step->id, FlakyPaymentAction::class, 0);
    drainQueue();

    expect(FlakyPaymentAction::$calls)->toBe(2);

    // A payload written before the generation field existed carries null. It predates
    // every retry, so it belongs to cycle 0 and must be refused here too — waving it
    // through would let it run alongside the live cycle's own job.
    $legacy = unserialize(serialize(new RunActionJob($step->id, FlakyPaymentAction::class)));

    expect($legacy->retryGeneration)->toBeNull();

    $resumes = fn (): int => SagaFlow::findRun($run->id)
        ->events()
        ->where('type', FlowEventType::FlowResumed->value)
        ->count();

    $before = $resumes();

    $legacy->handle(app(ActionDispatcher::class));
    drainQueue();

    // Refused, but not swallowed: a stale job may be the only wake left if the pass
    // that rewound the row died before sending the live one.
    expect(FlakyPaymentAction::$calls)->toBe(2)
        ->and($resumes())->toBeGreaterThan($before);

    // The job for the live cycle still runs, so the guard is a generation check and
    // not a blanket refusal.
    RunActionJob::dispatch($step->id, FlakyPaymentAction::class, 1);
    drainQueue();

    expect(FlakyPaymentAction::$calls)->toBe(3);
});

it('does not let a stale job execute a parked step', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q12')->run();
    drainQueue();

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry);

    // A job predating the generation token (null) still must not run a parked step:
    // only the seam restarts it, and only once the signal lands.
    RunActionJob::dispatch($step->id, FlakyPaymentAction::class);
    drainQueue();

    $step->refresh();

    expect(FlakyPaymentAction::$calls)->toBe(1)
        ->and($step->status)->toBe(ActionStatus::AwaitingRetry);
});

it('leaves a parked step alone in the monitor and the doctor', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.repair.grace_seconds', 0);
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q13')->run();
    drainQueue();

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry);

    // An execution deadline that has already passed: it belongs to running the step,
    // not to waiting for the signal, whose own deadline lives on the marker.
    $step->expires_at = now()->subMinute();
    $step->save();

    expect(app(FlowMonitor::class)->sweep())->toMatchArray(['actions' => 0, 'signals' => 0])
        ->and(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(0);

    drainQueue();

    $step->refresh();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->repair_attempts)->toBe(0)
        ->and(FlakyPaymentAction::$calls)->toBe(1);
});

/**
 * Park a step, age it past the repair grace window the way a real wait would, then
 * deliver the signal and stop the queue the moment the seam has committed the retry
 * and dispatched its job — the window in which the doctor may mistake that job for
 * a lost one.
 */
function stageDispatchedRetry(string $orderId): ActionRun
{
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments($orderId)->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);
    $step = $run->actions()->where('sequence', 1)->first();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry);

    // A real wait outlives the grace window many times over, and the row keeps the
    // created_at it was first scheduled with.
    ActionRun::query()->whereKey($step->id)->update(['created_at' => now()->subMinutes(10)]);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    workUntilStatus($run, 1, ActionStatus::Pending);

    return $step->fresh();
}

it('does not let the doctor duplicate the job of a retry cycle it just started', function () {
    FlakyPaymentAction::reset(failures: 99);

    $step = stageDispatchedRetry('order-q31');

    expect($step->retry_signal_attempts)->toBe(1)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);

    // The row is older than the grace window, so only the hold written with the retry
    // keeps the doctor off it.
    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);

    drainQueue();

    // One signal, one execution. A second job for the same generation would have run
    // the step again on the failure, since the generation token cannot tell the two
    // apart and Failed is deliberately not in the job's skip list.
    expect(FlakyPaymentAction::$calls)->toBe(2)
        ->and($step->fresh()->retry_signal_attempts)->toBe(1);
});

it('still recovers a retry job that was genuinely lost', function () {
    FlakyPaymentAction::reset(failures: 1);

    $step = stageDispatchedRetry('order-q32');

    // The hold is a delay, not an exemption: drop the job and let it expire.
    DB::connection('testing')->table('jobs')->delete();

    ActionRun::query()->whereKey($step->id)->update(['repair_available_at' => now()->subMinute()]);

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(1);

    drainQueue();

    $final = SagaFlow::findRun($step->flow_run_id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::Completed);
});

it('gives each retry cycle its own repair budget', function () {
    FlakyPaymentAction::reset(failures: 1);

    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q33')->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);
    $step = $run->actions()->where('sequence', 1)->first();

    // Repairs spent before the step ever parked: without a reset they would deny this
    // cycle the recovery it has not used yet.
    ActionRun::query()->whereKey($step->id)->update([
        'created_at' => now()->subMinutes(10),
        'repair_attempts' => (int) config('saga-lara-flow.repair.max_attempts'),
    ]);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    workUntilStatus($run, 1, ActionStatus::Pending);

    $step->refresh();

    expect($step->repair_attempts)->toBe(0);

    DB::connection('testing')->table('jobs')->delete();

    ActionRun::query()->whereKey($step->id)->update(['repair_available_at' => now()->subMinute()]);

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(1);
});

it('does not restart or re-park a parked step while collecting compensations', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q14')->run();
    drainQueue();

    $before = SagaFlow::findRun($run->id)->signals()->count();

    SagaFlow::loadFlow($run->id)->compensate();

    $final = SagaFlow::findRun($run->id);
    $step = $final->actions()->where('sequence', 1)->first();

    // Compensation-only planning stops at the parked frontier: the step is not
    // re-executed, not re-parked, and no second marker is written. It ends up
    // Cancelled because the run it belongs to finished, not because the seam touched
    // it — an unspent retry budget is what shows the park was left alone.
    expect($final->status)->toBe(FlowStatus::Cancelled)
        ->and(FlakyPaymentAction::$calls)->toBe(1)
        ->and($step->status)->toBe(ActionStatus::Cancelled)
        ->and($step->retry_signal)->toBe('balance-refilled')
        ->and($step->retry_signal_attempts)->toBe(0)
        ->and($final->signals()->count())->toBe($before)
        ->and(CompensationLog::all())->toBe(['undo:created']);
});

it('runs a job that was queued before the retry generation existed', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q15')->run();

    workUntilStatus($run, 0, ActionStatus::Pending);

    $step = $run->actions()->where('sequence', 0)->first();

    $payload = serialize(new RunActionJob($step->id, MakeValueAction::class));

    // SerializesModels omits any property that equals its class default and restores
    // it from that default on the way back, so this payload is byte-for-byte what a
    // worker would have written before the field existed. A promoted constructor
    // parameter carries no class default: the property would arrive uninitialized and
    // reading it below would throw, stranding every action queued across a deploy.
    expect($payload)->not->toContain('retryGeneration');

    $job = unserialize($payload);

    expect($job->retryGeneration)->toBeNull();

    $job->handle(app(ActionDispatcher::class));

    expect($step->fresh()->status)->toBe(ActionStatus::Completed);
});

it('still owes native attempts to a step on its second retry cycle', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(UnreliableRetryOnSignalWorkflow::class)->withArguments('order-q16')->run();
    drainQueue();

    $step = $run->actions()->first();

    // Cycle 0 spent both of the action's native attempts before parking.
    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->attempts)->toBe(2)
        ->and($step->retry_signal_attempts)->toBe(0);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    // Stop inside cycle 1, after its first native attempt failed and before the
    // second one has run. attempts is cumulative, so it now reads 3 — the gate has to
    // measure it against the whole allowance so far, not against $tries alone.
    workUntil($run, 0, fn (ActionRun $current): bool => $current->attempts === 3);

    $step->refresh();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and($step->retry_signal_attempts)->toBe(1);

    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);

    $step->refresh();

    // A wake arriving mid-cycle must not park the step or spend another cycle: the
    // queue still owes it the fourth attempt.
    expect($step->status)->toBe(ActionStatus::Failed)
        ->and($step->retry_signal_attempts)->toBe(1)
        ->and($run->signals()->where('status', SignalStatus::Waiting)->count())->toBe(0);

    drainQueue();

    $step->refresh();

    // The fourth native attempt ran, and only then did the step park again.
    expect(UnreliablePaymentAction::$calls)->toBe(4)
        ->and($step->attempts)->toBe(4)
        ->and($step->status)->toBe(ActionStatus::AwaitingRetry);
});

it('claims a wait marker only while it is still waiting', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q17')->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);
    $marker = $run->signals()->first();
    $recorder = app(SignalRecorder::class);

    expect($marker->status)->toBe(SignalStatus::Waiting)
        ->and($recorder->consumeWhileWaiting($run, $marker, 1))->toBeTrue()
        ->and($marker->fresh()->status)->toBe(SignalStatus::Consumed);

    // A marker that already carries a delivery is left exactly as it is: the caller
    // must resolve that delivery instead of writing over it.
    $delivered = $recorder->recordSignalWaiting($run, 'balance-refilled', 1);

    expect($recorder->fulfilWaitingSignal($delivered, ['by' => 'ops']))->not->toBeNull()
        ->and($recorder->consumeWhileWaiting($run, $delivered, 1))->toBeFalse();

    $delivered->refresh();

    expect($delivered->status)->toBe(SignalStatus::Received)
        ->and($delivered->payload)->toBe(['by' => 'ops'])
        ->and($delivered->consumed_at)->toBeNull();
});

it('keeps a delivery that lost the race for a marker instead of overwriting it', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q18')->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);
    $recorder = app(SignalRecorder::class);

    // deliver() reads the open marker, and the seam claims it before deliver() gets
    // to write. Reproduce that ordering with the model deliver() would be holding.
    $stale = $run->signals()->first();

    expect($recorder->consumeWhileWaiting($run, clone $stale, 1))->toBeTrue()
        ->and($recorder->fulfilWaitingSignal($stale, ['by' => 'ops']))->toBeNull();

    // The spent marker stays spent: writing the delivery into it would attach it to a
    // row that latestForSequence() stops returning after the next park.
    $marker = $run->signals()->first();

    expect($marker->status)->toBe(SignalStatus::Consumed)
        ->and($marker->payload)->toBeNull();

    // Through the dispatcher the same loss keeps the delivery alive as a floating row,
    // where the next park or replay can still claim it.
    app(SignalDispatcher::class)->deliver($run, 'balance-refilled', ['by' => 'ops']);

    $floating = $run->signals()->where('status', SignalStatus::Received)->get();

    expect($floating)->toHaveCount(1)
        ->and($floating[0]->wait_sequence)->toBeNull()
        ->and($floating[0]->payload)->toBe(['by' => 'ops']);
});

it('reads queue exhaustion off the row rather than off the action class', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(UnreliableRetryOnSignalWorkflow::class)->withArguments('order-q19')->run();
    drainQueue();

    $step = $run->actions()->first();

    // Baseline: the queue gave up after both native attempts and said so on the row.
    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->attempts)->toBe(2)
        ->and($step->queue_attempts_exhausted)->toBeTrue();

    // A job still in flight whose attempts already outnumber the action's current
    // $tries — what a deploy that lowered $tries leaves behind. The counter says
    // "exhausted", the queue has not said so, and the queue is the authority: wait.
    $step->status = ActionStatus::Failed;
    $step->attempts = 9;
    $step->queue_attempts_exhausted = false;
    $step->save();

    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);

    $step->refresh();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and($step->retry_signal_attempts)->toBe(0);

    // And the mirror case — a deploy that raised $tries, leaving fewer attempts on the
    // row than the class now allows. The queue is done, so the step parks.
    $step->attempts = 1;
    $step->queue_attempts_exhausted = true;
    $step->save();

    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);

    $step->refresh();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry);
});

it('reports one consumption per delivered signal when a floating row is handed over', function () {
    Event::fake([FlowSignalReceived::class, FlowSignalConsumed::class]);

    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q20')->run();
    drainQueue();

    $parked = SagaFlow::findRun($run->id);

    expect($parked->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::AwaitingRetry);

    // One delivery, landing as a floating row because it raced the marker.
    app(SignalRecorder::class)->storeReceivedSignal($parked, 'balance-refilled', []);

    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed);

    // Closing the spent marker is bookkeeping, not a consumption: a listener counting
    // signals must see exactly one of each for the one signal that was delivered.
    Event::assertDispatched(FlowSignalReceived::class, 1);
    Event::assertDispatched(FlowSignalConsumed::class, 1);

    // The full history still records both rows closing, with the spent one marked.
    $consumed = $final->events()->where('type', FlowEventType::SignalConsumed->value)->get();

    expect($consumed)->toHaveCount(2)
        ->and($consumed->pluck('payload.superseded')->filter()->count())->toBe(1);
});

it('adopts an abandoned wait signal instead of recording a second one', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q22')->run();
    drainQueue();

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 1)->first();

    // parkForRetry() writes the wait signal and the AwaitingRetry transition as two
    // separate commits. Rewind to the state a kill between them leaves behind: the
    // signal is committed, the step never left Failed.
    $step->status = ActionStatus::Failed;
    $step->queue_attempts_exhausted = true;
    $step->save();

    ResumeWorkflowJob::dispatch($run->id);
    drainQueue();

    $parked = SagaFlow::findRun($run->id);

    // Two open signals at one ordinal would strand the run: delivery fulfils the
    // OLDEST open row for a name, while the seam reads the NEWEST at the ordinal.
    expect($parked->signals()->where('status', SignalStatus::Waiting)->count())->toBe(1)
        ->and($parked->actions()->where('sequence', 1)->first()->status)
        ->toBe(ActionStatus::AwaitingRetry);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('rolls the consumption back when the retry transition fails', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q23')->run();
    drainQueue();

    // Blowing up on the write that spends the cycle stands in for a process that
    // dies mid-transition. Spending the signal and spending the cycle it pays for
    // have to land together or not at all: a consumed signal with the step still
    // Failed would park again and wait for a second delivery that nobody owes.
    ActionRun::saved(function (ActionRun $step): void {
        if ($step->retry_signal_attempts === 1) {
            throw new RuntimeException('the write exploded');
        }
    });

    $retried = 0;
    $consumed = 0;

    Event::listen(ActionRetried::class, function () use (&$retried): void {
        $retried++;
    });

    Event::listen(FlowSignalConsumed::class, function () use (&$consumed): void {
        $consumed++;
    });

    try {
        SagaFlow::loadFlow($run->id)->signal('balance-refilled');
        drainQueue();
    } catch (Throwable) {
        // The explosion is the setup; the rows it leaves behind are the assertion.
    }

    $signal = SagaFlow::findRun($run->id)->signals()->reorder()->orderByDesc('id')->first();
    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 1)->first();

    expect($signal->status)->toBe(SignalStatus::Received)
        ->and($step->retry_signal_attempts)->toBe(0)
        // Both events are after-commit, so a listener never reacts to a cycle the
        // database rolled back.
        ->and($retried)->toBe(0)
        ->and($consumed)->toBe(0);
});

it('leaves a delivered signal alone when the monitor times it out', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q24')->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);
    $marker = $run->signals()->firstOrFail();

    $marker->timeout_at = now()->subMinute();
    $marker->save();

    // The monitor reads a batch of overdue signals and writes them one at a time, so
    // this is its snapshot from before the delivery landed.
    $stale = $run->signals()->firstOrFail();

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    expect($marker->fresh()->status)->toBe(SignalStatus::Received);

    // The delivery was acknowledged to whoever sent it; declaring it a timeout now
    // would turn a signal the caller believes arrived into a rolled-back saga.
    expect(app(SignalRecorder::class)->timeoutSignal($stale))->toBeFalse()
        ->and($marker->fresh()->status)->toBe(SignalStatus::Received);
});

it('ends the policy when the wait times out, even with budget left', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.monitor.expiration.defaults.signal', 60);
    FlakyPaymentAction::reset(failures: 99);

    // Budget 3, of which the timeout spends none.
    $run = SagaFlow::create(OptionalRetryOnSignalWorkflow::class)
        ->withArguments('order-q25', 3)
        ->run();
    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);

    $this->travel(120)->seconds();

    app(FlowMonitor::class)->sweep();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    // The deadline bounds the waiting, not one wait out of many: otherwise an optional
    // step would fall back, replay and park again on the very next pass.
    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->signals()->count())->toBe(1)
        ->and($final->signals()->first()->status)->toBe(SignalStatus::TimedOut)
        ->and($final->result['charged'] ?? null)->toBe('unpaid');

    $step = $final->actions()->where('sequence', 0)->first();

    expect($step->status)->toBe(ActionStatus::OptionalFailed)
        ->and($step->retry_signal_attempts)->toBe(0);
});

it('reports a broken history contract when an awaitSignal lands on a parked step', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q26')->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);

    expect($run->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($run->signals()->first()->wait_sequence)->toBe(1);

    // A retry wait-signal is the one signal row that shares an ordinal with an action.
    // Editing an in-flight workflow so awaitSignal() takes that ordinal must be
    // reported, not resolved against the retry signal and walked past.
    expect(fn () => app(HistoryContractGuard::class)->expectSignal($run->id, 1, 'balance-refilled'))
        ->toThrow(HistoryContractMismatchException::class);
});

it('hands a delivered signal back with its payload intact', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q27')->run();
    drainQueue();

    $seen = null;

    Event::listen(FlowSignalReceived::class, function (FlowSignalReceived $event) use (&$seen): void {
        $seen = $event->signal->payload;
    });

    SagaFlow::loadFlow($run->id)->signal('balance-refilled', ['topped_up' => 500]);

    $stored = SagaFlow::findRun($run->id)->signals()->where('name', 'balance-refilled')->first();

    // Filling a claim's own database-ready values back into the model would encode the
    // payload twice: the row right, every listener seeing a JSON string. And this is
    // the ordinary awaitSignal delivery path, not just the retry one.
    expect($stored->payload)->toBe(['topped_up' => 500])
        ->and($seen)->toBe(['topped_up' => 500]);
});

it('holds the step to the budget persisted at scheduling time', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.actions.retry_on_signal.max_retries', 1);
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q28')->run();
    drainQueue();

    $parked = SagaFlow::findRun($run->id);

    expect($parked->actions()->where('sequence', 1)->first()->retry_signal_max_attempts)->toBe(1);

    // A redeploy raises the cap while the run is parked. The row still says 1, and so
    // do the events and saga-flow:show — enforcing anything else would move a limit
    // the operator is being shown.
    config()->set('saga-lara-flow.actions.retry_on_signal.max_retries', 5);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    drainQueue();

    $final = SagaFlow::findRun($run->id);
    $step = $final->actions()->where('sequence', 1)->first();

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($step->status)->toBe(ActionStatus::Failed)
        ->and($step->retry_signal_attempts)->toBe(1)
        ->and($step->retry_signal_max_attempts)->toBe(1);
});

it('settles an optional step whose retry policy was removed mid-flight', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(OptionalFallbackWorkflow::class)->run();

    // Stop after the first pass has scheduled the optional step.
    workOneJob();

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 0)->firstOrFail();

    // The state a rolling deploy leaves: the queue gave up while the row carried a
    // policy, so the hook left OptionalFailed to the seam — and the deploy meanwhile
    // removed retryOnSignal(). Nothing will write OptionalFailed and no job is left.
    $step->status = ActionStatus::Failed;
    $step->queue_attempts_exhausted = true;
    $step->retry_signal = 'balance-refilled';
    $step->save();

    DB::connection('testing')->table('jobs')->delete();

    ResumeWorkflowJob::dispatch($run->id);
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['optional'] ?? null)->toBe('skipped')
        ->and($final->actions()->where('sequence', 0)->first()->status)
        ->toBe(ActionStatus::OptionalFailed);
});

it('keeps retrying a step the queue has not finished with after the policy is removed', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(FailingWorkflow::class)->run();

    // Stop once the first step is scheduled.
    workOneJob();

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 0)->firstOrFail();

    // Mid-native-retries after a deploy removed retryOnSignal(): the row carries the
    // policy it was scheduled with and the queue still owes an attempt. An early replay
    // must not read that as a final failure while a job is still coming.
    $step->status = ActionStatus::Failed;
    $step->queue_attempts_exhausted = false;
    $step->retry_signal = 'balance-refilled';
    $step->save();

    DB::connection('testing')->table('jobs')->delete();

    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);

    expect(SagaFlow::findRun($run->id)->status)->not->toBe(FlowStatus::Failed);
});

it('keeps the cap when a step adopts a retry policy it was not scheduled with', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.actions.retry_on_signal.max_retries', 1);
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-q30')->run();

    // Drive until the retry step exists but has not failed yet, then strip the policy
    // off the row: this is a row an older deploy scheduled, before retryOnSignal() was
    // added to the workflow.
    workUntil($run, 1, fn (?ActionRun $step): bool => $step !== null);

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 1)->firstOrFail();
    $step->retry_signal = null;
    $step->retry_signal_max_attempts = null;
    $step->save();

    drainQueue();

    $parked = SagaFlow::findRun($run->id)->actions()->where('sequence', 1)->firstOrFail();

    // Parking is where the row adopts the policy, so it has to write the cap it decided
    // with: from here on the budget is read off the row, and an empty column would
    // silently mean unbounded.
    expect($parked->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($parked->retry_signal)->toBe('balance-refilled')
        ->and($parked->retry_signal_max_attempts)->toBe(1);
});

it('does not reopen an optional step that already published its give-up', function () {
    useDatabaseQueue();
    FlakyPaymentAction::reset(failures: 99);

    $run = SagaFlow::create(OptionalRetryOnSignalWorkflow::class)
        ->withArguments('order-q31', 3)
        ->run();
    drainQueue();

    $step = SagaFlow::findRun($run->id)->actions()->where('sequence', 0)->firstOrFail();

    // The state a deploy that ADDED retryOnSignal() leaves: the hook finalised the row
    // while it carried no policy, so listeners already have OptionalActionFailed.
    $step->status = ActionStatus::OptionalFailed;
    $step->retry_signal = null;
    $step->retry_signal_max_attempts = null;
    $step->save();

    DB::connection('testing')->table('jobs')->delete();

    ResumeWorkflowJob::dispatch($run->id);
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->actions()->where('sequence', 0)->first()->status)
        ->toBe(ActionStatus::OptionalFailed);
});

it('reaches the same retry policy decision through the queue as inline', function () {
    useDatabaseQueue();
    DeclinableChargeAction::reset(failures: 99, code: 422);
    RecordingRetryPolicy::$refuseCode = 422;

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-q-policy')->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    // The gate lives on the replay seam, which both transports share — but only the
    // queued path lets the step burn its native attempts first, so the policy must
    // still be asked exactly once, and only after the queue has given up.
    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($final->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::Failed)
        ->and($final->signals()->count())->toBe(0)
        ->and(RecordingRetryPolicy::calls())->toBe(1)
        ->and(RecordingRetryPolicy::last()->cyclesSpent)->toBe(0)
        ->and(CompensationLog::all())->toBe(['undo:created']);
});

it('parks through the queue when the policy allows the failure', function () {
    useDatabaseQueue();
    DeclinableChargeAction::reset(failures: 99, code: 503);
    RecordingRetryPolicy::$refuseCode = 422;

    $run = SagaFlow::create(RetryPolicyWorkflow::class)->withArguments('order-q-park')->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Waiting)
        ->and($final->actions()->where('sequence', 1)->first()->status)->toBe(ActionStatus::AwaitingRetry)
        ->and(RecordingRetryPolicy::calls())->toBe(1)
        ->and(RecordingRetryPolicy::last()->failure->code)->toBe(503)
        ->and(CompensationLog::all())->toBe([]);
});
