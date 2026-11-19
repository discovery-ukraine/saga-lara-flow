<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CompensationUnfinishedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\States\FlowStateMachine;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RaceOnTransition;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Support\Facades\DB;

/**
 * Whoever moved a run into Cancelling owns its rollback, and the run lands where that owner
 * intended. A parent's close, an operator's compensate() and a retry of the close job itself
 * leave a rollback with another owner alone; cancel() is the one way to cut it short, and it
 * says so on the run; a kick takes a stalled one over as the doctor would.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    RaceOnTransition::reset();
    DiesEnteringCancelling::$armed = false;
    KickedMidUndoAction::$runId = null;
    ReportedTransitions::$seen = [];
});

final class ReportedTransitions
{
    /** @var list<string> */
    public static array $seen = [];
}

final class FailsAfterItsStepWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'c')->compensateWith(UndoAction::class, 'c')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

final class ParentOfFailingChildWorkflow extends Workflow
{
    public function handle(bool $ownStep = true): void
    {
        if ($ownStep) {
            $this->action(MakeValueAction::class, 'p')->compensateWith(UndoAction::class, 'p')->run();
        }

        $this->child(FailsAfterItsStepWorkflow::class)->closePolicy(ChildClosePolicy::Cancel)->run();
        $this->awaitSignal('never');
    }
}

final class TwoUndoableStepsWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->awaitSignal('never');
    }
}

final class FailsAfterTwoStepsWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

/**
 * An undo during which an operator kicks the run it belongs to.
 */
final class KickedMidUndoAction extends Action
{
    public static ?string $runId = null;

    public function handle(string $label): void
    {
        CompensationLog::record('undo:'.$label);

        if (self::$runId !== null) {
            $id = self::$runId;
            self::$runId = null;

            app(FlowDoctor::class)->kick(FlowRun::query()->findOrFail($id));
        }
    }
}

final class KickedWhileUndoingWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(KickedMidUndoAction::class, 'b')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

/**
 * A process that dies right after its move into Cancelling has committed.
 */
final class DiesEnteringCancelling extends FlowStateMachine
{
    public static bool $armed = false;

    public function transition(FlowRun $run, FlowStatus $to): FlowRun
    {
        $moved = parent::transition($run, $to);

        if (self::$armed && $to === FlowStatus::Cancelling) {
            self::$armed = false;

            throw new RuntimeException('the process died');
        }

        return $moved;
    }
}

/**
 * Drive the parent until its child has failed on its own and queued its rollback.
 *
 * @return array{0: FlowRun, 1: FlowRun}
 */
function childRollingItselfBack(bool $ownStep): array
{
    $parent = SagaFlow::create(ParentOfFailingChildWorkflow::class)
        ->withArguments($ownStep)
        ->expiresAt(now()->addSeconds(30))
        ->run();

    for ($tick = 0; $tick < 20; $tick++) {
        $child = FlowRun::query()->where('parent_id', $parent->id)->first();

        if ($child?->status === FlowStatus::Cancelling) {
            break;
        }

        workOneJob();
    }

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($child->status)->toBe(FlowStatus::Cancelling)
        ->and($child->rollback_target)->toBe(FlowStatus::Failed);

    return [$parent, $child];
}

/**
 * Expire a run with two undoable steps; level b's job is left on the queue.
 */
function expiringTwoStepRun(): FlowRun
{
    $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    test()->travel(60)->seconds();

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1);

    return FlowRun::query()->findOrFail($run->id);
}

it('leaves a child rolling itself back to that rollback when its parent expires', function (): void {
    foreach ([true, false] as $ownStep) {
        CompensationLog::reset();
        useDatabaseQueue();
        logToFile($log = sys_get_temp_dir().'/saga-owner-'.bin2hex(random_bytes(6)).'.log');

        [$parent, $child] = childRollingItselfBack($ownStep);
        $childsOwn = loseJobs('RunCompensationJob');

        $this->travel(60)->seconds();
        app(FlowMonitor::class)->sweep();
        drainQueue();

        // The close came first and left the child to the rollback it found.
        expect(FlowRun::query()->findOrFail($parent->id)->status)->toBe(FlowStatus::Expired)
            ->and(FlowRun::query()->findOrFail($child->id)->status)->toBe(FlowStatus::Cancelling)
            ->and(FlowRun::query()->findOrFail($child->id)->rollback_token)->toBe($child->rollback_token)
            ->and(CompensationLog::all())->toBe($ownStep ? ['undo:p'] : []);

        bringJobsBack($childsOwn);
        drainQueue();

        $landed = FlowRun::query()->findOrFail($child->id);

        expect($landed->status)->toBe(FlowStatus::Failed)
            ->and($landed->exception['class'] ?? null)->toBe(ActionFailedException::class)
            ->and(CompensationLog::all())->toBe($ownStep ? ['undo:p', 'undo:c'] : ['undo:c'])
            ->and(compensationTrail($child->id))->toBe(['c:completed'])
            ->and(FlowChild::query()->where('child_flow_run_id', $child->id)->firstOrFail()->status)
            ->toBe(ChildStatus::Failed)
            ->and((string) @file_get_contents($log))->not->toContain(AnomalyLog::REASON_CLAIM_LOST);

        $this->travelBack();
    }
});

it('leaves a child rolling itself back to that rollback when its parent is cancelled', function (): void {
    useDatabaseQueue();

    [$parent, $child] = childRollingItselfBack(true);
    $childsOwn = loseJobs('RunCompensationJob');

    // cancel() closes the children without compensation: the close has nothing of its own
    // to undo, and must not take the child's undo away from it either.
    SagaFlow::loadFlow($parent->id)->cancel();
    drainQueue();

    expect(FlowRun::query()->findOrFail($child->id)->status)->toBe(FlowStatus::Cancelling);

    bringJobsBack($childsOwn);
    drainQueue();

    expect(FlowRun::query()->findOrFail($child->id)->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c'])
        ->and(compensationTrail($child->id))->toBe(['c:completed']);
});

it('leaves a child whose undo a live worker holds to its own rollback', function (): void {
    useDatabaseQueue();

    [$parent, $child] = childRollingItselfBack(true);
    loseJobs('RunCompensationJob');

    CompensationRun::query()->where('flow_run_id', $child->id)
        ->update(['status' => CompensationStatus::Running, 'started_at' => now()]);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    $child = FlowRun::query()->findOrFail($child->id);

    expect(FlowRun::query()->findOrFail($parent->id)->status)->toBe(FlowStatus::Expired)
        ->and($child->status)->toBe(FlowStatus::Cancelling)
        ->and($child->rollback_target)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:p']);
});

it('carries a close on when the same job is tried again, and leaves it to that job otherwise', function (): void {
    foreach ([true, false] as $withCompensation) {
        CompensationLog::reset();
        useDatabaseQueue();

        $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->run();
        drainQueue();

        app()->bind(StateMachine::class, DiesEnteringCancelling::class);
        DiesEnteringCancelling::$armed = true;

        $close = new CancelChildWorkflowJob($run->id, FlowStatus::Cancelled, $withCompensation);

        expect(fn () => dispatch_sync($close))->toThrow(RuntimeException::class, 'the process died');

        app()->bind(StateMachine::class, FlowStateMachine::class);

        expect(FlowRun::query()->findOrFail($run->id)->rollback_token)->toBe($close->rollbackToken);

        // Another close job finds a rollback it does not own.
        dispatch_sync(new CancelChildWorkflowJob($run->id, FlowStatus::Cancelled, $withCompensation));

        expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling)
            ->and(FlowRun::query()->findOrFail($run->id)->rollback_token)->toBe($close->rollbackToken)
            ->and(CompensationLog::all())->toBe([]);

        // The same payload, delivered again. It finds the child fenced under its own token and
        // writes no second move: a move guarded by status alone could put that token back over
        // an owner that took the rollback over in between.
        app()->bind(StateMachine::class, RaceOnTransition::class);
        RaceOnTransition::$race = fn (): null => null;

        dispatch_sync(unserialize(serialize($close)));

        app()->bind(StateMachine::class, FlowStateMachine::class);

        expect(RaceOnTransition::$races)->toBe(0)
            ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelled)
            ->and(CompensationLog::all())->toBe($withCompensation ? ['undo:b', 'undo:a'] : []);
    }
});

it('leaves a rollback with an owner alone from a close job queued before jobs carried one', function (): void {
    useDatabaseQueue();

    $run = expiringTwoStepRun();

    $payload = preg_replace('/s:13:"rollbackToken";s:26:"[0-9A-Z]{26}";/', '', serialize(
        new CancelChildWorkflowJob($run->id, FlowStatus::Cancelled, true),
    ), 1, $removed);
    $payload = preg_replace_callback('/^(O:\d+:"[^"]+":)(\d+)/', fn (array $m): string => $m[1].($m[2] - 1), $payload);

    $legacy = unserialize($payload);

    expect($removed)->toBe(1)
        ->and($legacy->rollbackToken)->toBeNull();

    dispatch_sync($legacy);

    expect(FlowRun::query()->findOrFail($run->id)->rollback_token)->toBe($run->rollback_token)
        ->and(CompensationLog::all())->toBe([]);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves a run already rolling back to its owner when compensate() is called', function (): void {
    useDatabaseQueue();

    $run = expiringTwoStepRun();

    $returned = SagaFlow::loadFlow($run->id)->compensate();

    expect($returned->status)->toBe(FlowStatus::Cancelling)
        ->and($returned->rollback_token)->toBe($run->rollback_token)
        ->and(CompensationLog::all())->toBe([]);

    drainQueue();

    $landed = FlowRun::query()->findOrFail($run->id);

    expect($landed->status)->toBe(FlowStatus::Expired)
        ->and($landed->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a'])
        ->and(compensationTrail($run->id))->toBe(['b:completed', 'a:completed']);
});

it('leaves a rollback that started while compensate() planned to its owner', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    $handle = SagaFlow::loadFlow($run->id);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    $returned = $handle->compensate();

    expect($returned->status)->toBe(FlowStatus::Cancelling)
        ->and($returned->rollback_target)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toBe([]);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('cuts a stalled rollback short on cancel() and records what it left undone', function (): void {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/saga-owner-'.bin2hex(random_bytes(6)).'.log');
    app(ExceptionHandler::class)->reportable(function (InvalidTransitionException $e): void {
        ReportedTransitions::$seen[] = $e->getMessage();
    });

    $run = expiringTwoStepRun();
    workOneJob();
    $lost = loseJobs('RunCompensationJob');

    $cancelled = SagaFlow::loadFlow($run->id)->cancel();

    expect($cancelled->status)->toBe(FlowStatus::Cancelled)
        ->and($cancelled->rollback_token)->not->toBe($run->rollback_token)
        ->and($cancelled->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and($cancelled->exception['compensation']['class'] ?? null)->toBe(CompensationUnfinishedException::class)
        ->and($cancelled->exception['compensation']['message'] ?? '')->toContain('sequence 1')
        ->and(FlowRun::query()->findOrFail($run->id)->exception)->toEqual($cancelled->exception)
        ->and((string) @file_get_contents($log))->toContain(AnomalyLog::REASON_ROLLBACK_ABANDONED);

    // The lost job turns up after all: the rollback is no longer its owner's to carry on.
    bringJobsBack($lost);
    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b'])
        ->and(compensationTrail($run->id))->toBe(['b:completed', 'a:pending'])
        ->and(ReportedTransitions::$seen)->toBe([]);
});

it('records a rollback cut short before any compensation was registered', function (): void {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/saga-owner-'.bin2hex(random_bytes(6)).'.log');
    app()->bind(StateMachine::class, DiesEnteringCancelling::class);

    $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    DiesEnteringCancelling::$armed = true;
    $this->travel(60)->seconds();

    expect(fn () => app(FlowMonitor::class)->sweep())->toThrow(RuntimeException::class, 'the process died')
        ->and(compensationTrail($run->id))->toBe([]);

    $cancelled = SagaFlow::loadFlow($run->id)->cancel();

    expect($cancelled->status)->toBe(FlowStatus::Cancelled)
        ->and($cancelled->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and($cancelled->exception['compensation']['class'] ?? null)->toBe(CompensationUnfinishedException::class)
        ->and($cancelled->exception['compensation']['message'] ?? '')->toContain('was cancelled before it finished')
        ->and((string) @file_get_contents($log))->toContain(AnomalyLog::REASON_ROLLBACK_ABANDONED);
});

it('takes a stalled rollback over on a kick once the doctor has given up on it', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = SagaFlow::create(FailsAfterTwoStepsWorkflow::class)->run();

    for ($tick = 0; $tick < 20; $tick++) {
        if (FlowRun::query()->findOrFail($run->id)->status === FlowStatus::Cancelling) {
            break;
        }

        workOneJob();
    }

    loseJobs('RunCompensationJob');

    $max = (int) config('saga-lara-flow.repair.max_attempts');
    FlowRun::query()->whereKey($run->id)->update(['repair_attempts' => $max]);

    $this->travel(10)->minutes();

    expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(0);

    $owner = FlowRun::query()->findOrFail($run->id)->rollback_token;

    $this->artisan('saga-flow:kick', ['run' => $run->id])
        ->expectsOutputToContain("Flow run [{$run->id}] rollback taken over and re-driven.")
        ->assertSuccessful();

    $kicked = FlowRun::query()->findOrFail($run->id);

    expect($kicked->rollback_token)->not->toBe($owner)
        ->and($kicked->repair_attempts)->toBe(0);

    drainQueue();

    $landed = FlowRun::query()->findOrFail($run->id);

    expect($landed->status)->toBe(FlowStatus::Failed)
        ->and($landed->exception['class'] ?? null)->toBe(ActionFailedException::class)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves a rollback that undoes nothing to its owner on a kick', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->run();
    drainQueue();

    app()->bind(StateMachine::class, DiesEnteringCancelling::class);
    DiesEnteringCancelling::$armed = true;

    expect(fn () => dispatch_sync(new CancelChildWorkflowJob($run->id, FlowStatus::Cancelled, false)))
        ->toThrow(RuntimeException::class, 'the process died');

    app()->bind(StateMachine::class, FlowStateMachine::class);

    $owner = FlowRun::query()->findOrFail($run->id)->rollback_token;

    $this->artisan('saga-flow:kick', ['run' => $run->id])
        ->expectsOutputToContain("Flow run [{$run->id}] is cancelling; nothing to re-drive.")
        ->assertSuccessful();

    expect(FlowRun::query()->findOrFail($run->id)->rollback_token)->toBe($owner)
        ->and(CompensationLog::all())->toBe([]);
});

it('leaves a rollback that is still moving to its owner on a kick', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(KickedWhileUndoingWorkflow::class)->run();

    for ($tick = 0; $tick < 20; $tick++) {
        if (FlowRun::query()->findOrFail($run->id)->status === FlowStatus::Cancelling) {
            break;
        }

        workOneJob();
    }

    $owner = FlowRun::query()->findOrFail($run->id)->rollback_token;

    // The kick lands while a live worker is inside the undo of b.
    KickedMidUndoAction::$runId = $run->id;
    drainQueue();

    $landed = FlowRun::query()->findOrFail($run->id);

    expect(KickedMidUndoAction::$runId)->toBeNull()
        ->and($landed->status)->toBe(FlowStatus::Failed)
        ->and($landed->rollback_token)->toBe($owner)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('carries a close on when a payload queued before jobs carried a token is delivered again', function (): void {
    foreach ([true, false] as $withCompensation) {
        CompensationLog::reset();
        useDatabaseQueue();

        $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->run();
        drainQueue();

        CancelChildWorkflowJob::dispatch($run->id, FlowStatus::Cancelled, $withCompensation);

        // Rewrite the queued payload into the shape a job queued before the upgrade has.
        $row = DB::connection('testing')->table('jobs')->where('payload', 'like', '%CancelChildWorkflowJob%')->sole();
        $payload = json_decode((string) $row->payload, true);
        $command = preg_replace('/s:13:"rollbackToken";s:26:"[0-9A-Z]{26}";/', '', $payload['data']['command'], 1, $removed);
        $payload['data']['command'] = preg_replace_callback(
            '/^(O:\d+:"[^"]+":)(\d+)/',
            fn (array $m): string => $m[1].($m[2] - 1),
            (string) $command,
        );
        DB::connection('testing')->table('jobs')->where('id', $row->id)->update(['payload' => json_encode($payload)]);

        expect($removed)->toBe(1);

        $legacy = loseJobs('CancelChildWorkflowJob');
        bringJobsBack($legacy);

        app()->bind(StateMachine::class, DiesEnteringCancelling::class);
        DiesEnteringCancelling::$armed = true;
        workOneJob();
        app()->bind(StateMachine::class, FlowStateMachine::class);

        expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling)
            ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);

        // The same payload, delivered again.
        bringJobsBack($legacy);
        drainQueue();

        expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelled)
            ->and(CompensationLog::all())->toBe($withCompensation ? ['undo:b', 'undo:a'] : []);
    }
});

it('says so when a kick takes over a rollback whose compensation a worker claimed a moment before', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(FailsAfterTwoStepsWorkflow::class)->run();

    for ($tick = 0; $tick < 20; $tick++) {
        if (FlowRun::query()->findOrFail($run->id)->status === FlowStatus::Cancelling) {
            break;
        }

        workOneJob();
    }

    loseJobs('RunCompensationJob');
    $this->travel(10)->minutes();

    // A late job of the owner claims b between the kick's look at the rollback and its takeover.
    $armed = true;
    DB::connection('testing')->listen(function (QueryExecuted $query) use (&$armed, $run): void {
        if ($armed && str_contains($query->sql, 'exists') && str_contains($query->sql, 'compensation_runs')) {
            $armed = false;

            CompensationRun::query()->where('flow_run_id', $run->id)
                ->update(['status' => CompensationStatus::Running, 'started_at' => now()]);
        }
    });

    $this->artisan('saga-flow:kick', ['run' => $run->id])
        ->expectsOutputToContain('a compensation a worker still holds keeps it waiting')
        ->assertSuccessful();

    expect($armed)->toBeFalse()
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);

    // That worker finishes b; once nothing has moved the rollback for the grace window, a
    // second kick carries it on.
    CompensationRun::query()->where('flow_run_id', $run->id)
        ->update(['status' => CompensationStatus::Completed, 'finished_at' => now()]);
    $this->travel(61)->seconds();

    $this->artisan('saga-flow:kick', ['run' => $run->id])
        ->expectsOutputToContain("Flow run [{$run->id}] rollback taken over and re-driven.")
        ->assertSuccessful();

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:a']);
});

it('takes a rollback from its owner before reading what it left on cancel()', function (): void {
    useDatabaseQueue();
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $run = expiringTwoStepRun();
    workOneJob();
    $lost = loseJobs('RunCompensationJob');

    // The owner's job for a arrives after what is left was read, before the run lands.
    RaceOnTransition::$on = FlowStatus::Cancelled;
    RaceOnTransition::$race = function () use ($lost): void {
        bringJobsBack($lost);
        workOneJob();
    };

    $cancelled = SagaFlow::loadFlow($run->id)->cancel();

    expect(RaceOnTransition::$races)->toBe(1)
        ->and($cancelled->exception['compensation']['message'] ?? '')->toContain('sequence 1')
        ->and(compensationTrail($run->id))->toBe(['b:completed', 'a:pending'])
        ->and(CompensationLog::all())->toBe(['undo:b']);
});

it('cuts a rollback short through a handle loaded before it began', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    $handle = SagaFlow::loadFlow($run->id);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    $cancelled = $handle->cancel();

    expect($cancelled->status)->toBe(FlowStatus::Cancelled)
        ->and($cancelled->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and($cancelled->exception['compensation']['class'] ?? null)->toBe(CompensationUnfinishedException::class);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe([]);
});

it('does not report a takeover for a run that began rolling back on its own while it was kicked', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    // The command reads the run Waiting; the sweep expires it before the kick looks again.
    $armed = true;
    DB::connection('testing')->listen(function (QueryExecuted $query) use (&$armed): void {
        if ($armed && str_starts_with($query->sql, 'select') && str_contains($query->sql, 'flow_runs')) {
            $armed = false;

            test()->travel(60)->seconds();
            app(FlowMonitor::class)->sweep();
        }
    });

    $this->artisan('saga-flow:kick', ['run' => $run->id])
        ->expectsOutputToContain("Flow run [{$run->id}] is cancelling; nothing to re-drive.")
        ->doesntExpectOutputToContain('taken over')
        ->assertSuccessful();

    expect($armed)->toBeFalse()
        ->and(FlowRun::query()->findOrFail($run->id)->rollback_target)->toBe(FlowStatus::Expired);
});

it('reads what a cancelled rollback left in one go', function (): void {
    useDatabaseQueue();

    $run = expiringTwoStepRun();
    loseJobs('RunCompensationJob');

    // A worker holds b, and its undo fails the moment after cancel() reads what is left.
    CompensationRun::query()->where('flow_run_id', $run->id)
        ->update(['status' => CompensationStatus::Running, 'started_at' => now()]);

    $armed = true;
    DB::connection('testing')->listen(function (QueryExecuted $query) use (&$armed, $run): void {
        if ($armed && str_starts_with($query->sql, 'select') && str_contains($query->sql, 'compensation_runs')) {
            $armed = false;

            CompensationRun::query()->where('flow_run_id', $run->id)
                ->update(['status' => CompensationStatus::Failed, 'finished_at' => now()]);
        }
    });

    $cancelled = SagaFlow::loadFlow($run->id)->cancel();

    expect($armed)->toBeFalse()
        ->and($cancelled->exception['compensation']['class'] ?? null)->toBe(CompensationUnfinishedException::class)
        ->and($cancelled->exception['compensation']['message'] ?? '')->toContain('status: running');
});

it('leaves a run that went into Cancelling before its intent was recorded alone on a kick', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoUndoableStepsWorkflow::class)->run();
    drainQueue();

    // Moved before the ownership columns existed: no owner, no target.
    app(StateMachine::class)->transition(FlowRun::query()->findOrFail($run->id), FlowStatus::Cancelling);

    $this->travel(10)->minutes();

    $this->artisan('saga-flow:kick', ['run' => $run->id])
        ->expectsOutputToContain("Flow run [{$run->id}] is cancelling; nothing to re-drive.")
        ->assertSuccessful();

    expect(FlowRun::query()->findOrFail($run->id)->rollback_token)->toBeNull()
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0)
        ->and(CompensationLog::all())->toBe([]);
});
