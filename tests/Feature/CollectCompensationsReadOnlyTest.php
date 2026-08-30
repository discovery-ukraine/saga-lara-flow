<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CaughtTimeoutThenParallelWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ManualCompensateWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalRetryCompensatedWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentOfVanishingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SelfCancellingThenThrowingWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\VanishedArgumentWorkflow;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;

/**
 * collectCompensations() is the read half of a rollback: it replays handle() only to
 * learn what has to be undone. Two things used to break that. It wrote — an optional
 * step whose retry budget was spent was settled by the replay that was only supposed
 * to look at it. And it decided by catching, which cannot tell the frontier it was
 * looking for from a fault it was not: any throw ended the stack, and compensate()
 * rolled back the truncated result and reported a complete unwind.
 *
 * Surfacing that fault instead has a blast radius of its own, because compensations are
 * planned from three places, not one. The two the operator did not ask for — the
 * expiration sweep and a parent closing its child — have to absorb it where it lands.
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

it('does not schedule or dispatch a parallel block it has only reached to read', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(CaughtTimeoutThenParallelWorkflow::class)->run();
    drainQueue();

    // The monitor times the wait out, so the next replay walks past the catch. The
    // resume job has not run yet, which is the window an operator's rollback lands in.
    app(FlowMonitor::class)->sweep();

    $rows = ActionRun::query()->count();
    $jobs = DB::connection('testing')->table('jobs')->count();

    $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    expect($entries)->toHaveCount(1)
        ->and(ActionRun::query()->count())->toBe($rows)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe($jobs)
        ->and(DB::connection('testing')->table('job_batches')->count())->toBe(0);
});

it('steps over a run whose rollback it cannot plan and journals it', function () {
    useDatabaseQueue();
    logToFile($path = sys_get_temp_dir().'/saga-expiry-'.bin2hex(random_bytes(6)).'.log');

    // Oldest deadline, so dueForExpiration() hands this one back first, every sweep.
    $stuck = SagaFlow::create(VanishedArgumentWorkflow::class)->run();
    drainQueue();
    $stuck = SagaFlow::findRun($stuck->id);
    $stuck->expires_at = now()->subHours(2);
    $stuck->save();

    $healthy = SagaFlow::create(SignalOnlyWorkflow::class)->run();
    drainQueue();
    $healthy = SagaFlow::findRun($healthy->id);
    $healthy->expires_at = now()->subMinute();
    $healthy->save();

    VanishedArgumentWorkflow::$label = null;

    // The sweep reads oldest first and runs before the signal and action passes, so a
    // throw let out here would cost every run behind this one and both passes below.
    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1)
        ->and(SagaFlow::findRun($healthy->id)->status)->toBe(FlowStatus::Expired)
        ->and(SagaFlow::findRun($stuck->id)->status)->toBe(FlowStatus::Waiting);

    $log = is_file($path) ? (string) file_get_contents($path) : '';

    expect($log)->toContain(AnomalyLog::REASON_EXPIRY_FAILED)
        ->and($log)->toContain($stuck->id);
});

it('does not strand a child whose rollback it could not plan', function () {
    config()->set('saga-lara-flow.queue.after_commit', false);

    $run = SagaFlow::create(ParentOfVanishingChildWorkflow::class)->runSync();
    $child = $run->children()->first()->child;

    expect($child->status)->toBe(FlowStatus::Waiting);

    VanishedArgumentWorkflow::$label = null;

    expect(fn () => SagaFlow::loadFlow($run->id)->compensate())
        ->toThrow(RuntimeException::class, 'the order this run was built from is gone');

    // Cancelling is the one state nothing recovers: the monitor and the doctor both
    // pass over it. The child is left where the close found it, for a retry to close.
    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Waiting);
});

it('fences a child before planning the rollback it will act on', function () {
    config()->set('saga-lara-flow.queue.after_commit', false);

    $run = SagaFlow::create(ParentOfVanishingChildWorkflow::class)->runSync();
    $child = SagaFlow::findRun($run->children()->first()->child_flow_run_id);

    app(StateMachine::class)->transition($child, FlowStatus::Cancelling);

    // This is what makes a plan made after the transition final, and a plan made
    // before it a guess: nothing can drive a Cancelling run, so no further step can
    // complete under one. The close plans on this side of the fence for that reason
    // — the transition's own guard reads status alone, so a child resumed and
    // re-parked while an earlier plan was being made would still match it.
    expect(fn () => app(FlowExecutor::class)->drive(SagaFlow::findRun($child->id), RunMode::Queued))
        ->toThrow(InvalidTransitionException::class);
});

it('retries a child close that failed after it had taken control', function () {
    // A real queue: finalizing the child notifies the parent, which batches its own
    // rollback. Nothing drains it — the assertions here are about the child.
    useDatabaseQueue();

    $run = SagaFlow::create(ParentOfVanishingChildWorkflow::class)->runSync();
    $child = SagaFlow::findRun($run->children()->first()->child_flow_run_id);

    // The window the two plans leave: the first succeeded, the child was fenced, and
    // the second met a world that had moved. The child is Cancelling and owned.
    app(StateMachine::class)->transition($child, FlowStatus::Cancelling);
    VanishedArgumentWorkflow::$label = null;

    expect(fn () => dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true)))
        ->toThrow(RuntimeException::class)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelling);

    // Cancelling has no way back — allowedFrom() lists only terminal states — so the
    // child sits there until the replay can read what it could not. It is not stuck
    // for good, though: the retry recovers it once the cause is fixed.
    VanishedArgumentWorkflow::reset();

    dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true));

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('surfaces a sweep failure that already took the run into cancelling', function () {
    // Sync driver and no job_batches table: the plan succeeds, the run is taken to
    // Cancelling, and dispatching the rollback batch is what throws.
    config()->set('saga-lara-flow.queue.after_commit', false);
    logToFile($path = sys_get_temp_dir().'/saga-sweep-'.bin2hex(random_bytes(6)).'.log');

    $run = SagaFlow::create(ManualCompensateWorkflow::class)->runSync();
    $run = SagaFlow::findRun($run->id);
    $run->expires_at = now()->subMinute();
    $run->save();

    expect(fn () => app(FlowMonitor::class)->sweep())->toThrow(QueryException::class)
        ->and(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Cancelling);

    // Not journalled as a plan that could not be made, because that is not what it
    // was, and not absorbed either: nothing returns to a run left mid-cancellation.
    $log = is_file($path) ? (string) file_get_contents($path) : '';

    expect($log)->not->toContain(AnomalyLog::REASON_EXPIRY_FAILED);

    // It cannot wedge the sweep the way a planning failure would: Cancelling is not
    // a candidate status, so the run is gone from the batch on its own.
    expect(app(FlowMonitor::class)->sweep())->toBe(['runs' => 0, 'signals' => 0, 'actions' => 0]);
});

it('does not abort the sweep over a run somebody else was moving meanwhile', function (FlowStatus $moveTo) {
    useDatabaseQueue();
    logToFile($path = sys_get_temp_dir().'/saga-race-'.bin2hex(random_bytes(6)).'.log');
    SelfCancellingThenThrowingWorkflow::reset();

    $run = SagaFlow::create(SelfCancellingThenThrowingWorkflow::class)->run();
    drainQueue();
    $run = SagaFlow::findRun($run->id);
    $run->expires_at = now()->subMinute();
    $run->save();

    // The replay moves the run and then faults — one process doing what two would.
    // Cancelling is the operator's own compensate(), so the sweep cannot read that
    // status as evidence of its own doing; what it goes on is the type of the throw.
    SelfCancellingThenThrowingWorkflow::$moveTo = $moveTo;

    expect(app(FlowMonitor::class)->sweep())->toBe(['runs' => 0, 'signals' => 0, 'actions' => 0])
        ->and(SagaFlow::findRun($run->id)->status)->toBe($moveTo);

    $log = is_file($path) ? (string) file_get_contents($path) : '';

    expect($log)->toContain(AnomalyLog::REASON_EXPIRY_FAILED)
        ->and($log)->toContain($run->id);

    SelfCancellingThenThrowingWorkflow::reset();
})->with([
    'finished outright' => FlowStatus::Cancelled,
    'taken by a manual compensate()' => FlowStatus::Cancelling,
]);
