<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\CompensationRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\States\FlowStateMachine;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentCancelPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * A run in Cancelling is one the sweep, the doctor and drive() all pass over, so a
 * rollback whose owner stopped — a process that died after taking the run, a level whose
 * job was lost — used to stay there for good. The intent written with the move says where
 * the rollback lands; once nothing has moved it for the grace window, the monitor carries
 * an expiry to the end and the doctor everything else, reusing every compensation the
 * owner it was taken from had already settled.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    DiesAfterTakingTheRun::$armed = false;
    DiesBeforeLanding::$runId = null;
    TakenOverMidUndoAction::$resumed = 0;
    StalledSagaWorkflow::$fault = null;
    StalledFailingSagaWorkflow::$onReplay = null;
});

final class StalledSagaWorkflow extends Workflow
{
    public static ?string $fault = null;

    public function handle(): void
    {
        if (self::$fault !== null) {
            throw new RuntimeException(self::$fault);
        }

        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->awaitSignal('never');
    }
}

final class StalledFailingSagaWorkflow extends Workflow
{
    /** Runs at the top of every replay, the doctor's planning one included. */
    public static ?Closure $onReplay = null;

    public function handle(): void
    {
        if (self::$onReplay !== null) {
            (self::$onReplay)();
        }

        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

/** A process that dies right before one run lands terminal. */
final class DiesBeforeLanding extends FlowStateMachine
{
    public static ?string $runId = null;

    public function transition(FlowRun $run, FlowStatus $to): FlowRun
    {
        if ($run->id === self::$runId && $to->isTerminal()) {
            self::$runId = null;

            throw new RuntimeException('the process died');
        }

        return parent::transition($run, $to);
    }
}

/**
 * An undo slow enough that its reclaim window and the grace window both pass while it
 * runs, and the doctor resumes the rollback from underneath it.
 */
final class TakenOverMidUndoAction extends Action
{
    public static int $resumed = 0;

    /**
     * @return array{undone: string}
     */
    public function handle(string $label): array
    {
        CompensationLog::record('undo:'.$label);

        test()->travel(5)->minutes();

        self::$resumed += app(FlowDoctor::class)->repair()->resumedRollbacks;

        return ['undone' => $label];
    }
}

final class TakenOverMidUndoWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(TakenOverMidUndoAction::class, 'b')->run();
        $this->awaitSignal('never');
    }
}

/**
 * A parallel block rolled back as one level, whose first undo is the slow one: the
 * second is reached by the inline owner only after the rollback was taken over.
 */
final class TakenOverMidLevelWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->parallel()
            ->action(MakeValueAction::class, 'y')
            ->compensateWith(UndoAction::class, 'y')
            ->action(MakeValueAction::class, 'x')
            ->compensateWith(TakenOverMidUndoAction::class, 'x')
            ->run();

        $this->awaitSignal('never');
    }
}

/** A process that dies once its move into Cancelling has committed. */
final class DiesAfterTakingTheRun extends FlowStateMachine
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
 * An overdue two-step saga whose expiry has been taken and whose first level is queued.
 */
function stalledExpiry(): FlowRun
{
    $run = SagaFlow::create(StalledSagaWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    test()->travel(60)->seconds();

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1);

    return FlowRun::query()->findOrFail($run->id);
}

/**
 * Take the queued jobs of one class off the queue, as a worker dying with them would.
 *
 * @return list<array<string, mixed>>
 */
function loseJobs(string $class): array
{
    $rows = DB::connection('testing')->table('jobs')->where('payload', 'like', '%'.$class.'%')->get();

    DB::connection('testing')->table('jobs')->whereIn('id', $rows->pluck('id'))->delete();

    return $rows->map(fn (object $row): array => array_diff_key((array) $row, ['id' => true]))->all();
}

/**
 * @param  list<array<string, mixed>>  $rows
 */
function bringJobsBack(array $rows): void
{
    foreach ($rows as $row) {
        DB::connection('testing')->table('jobs')->insert($row);
    }
}

/**
 * @return list<string>
 */
function compensationTrail(string $runId): array
{
    return CompensationRun::query()->where('flow_run_id', $runId)->orderBy('sequence')->get()
        ->map(fn (CompensationRun $row): string => $row->arguments[0].':'.$row->status->value)
        ->all();
}

it('carries an expiry to the end once the process that took the run died', function (): void {
    useDatabaseQueue();
    app()->bind(StateMachine::class, DiesAfterTakingTheRun::class);

    $run = SagaFlow::create(StalledSagaWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    DiesAfterTakingTheRun::$armed = true;
    $this->travel(60)->seconds();

    expect(fn () => app(FlowMonitor::class)->sweep())->toThrow(RuntimeException::class, 'the process died')
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(compensationTrail($run->id))->toBe([]);

    // Inside the grace window the rollback may still have an owner.
    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(0);

    $this->travel(61)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(1);

    drainQueue();

    $expired = FlowRun::query()->findOrFail($run->id);

    expect($expired->status)->toBe(FlowStatus::Expired)
        ->and($expired->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('resumes a rollback whose level job was lost without undoing a settled step again', function (): void {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/stalled-level-'.uniqid().'.log');

    $run = stalledExpiry();

    $this->travel(50)->seconds();
    workOneJob();
    loseJobs('RunCompensationJob');

    expect(compensationTrail($run->id))->toBe(['b:completed', 'a:pending']);

    // The run has not moved for the grace window, but its rollback has: the second level
    // was registered 11 seconds ago.
    $this->travel(11)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(0);

    $this->travel(50)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(1);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a'])
        ->and(compensationTrail($run->id))->toBe(['b:completed', 'a:completed'])
        // The settled level is passed over, not sent again to lose its claim.
        ->and((string) @file_get_contents($log))->not->toContain(AnomalyLog::REASON_CLAIM_LOST);
});

it('reuses the undo a close job ran before it died, when the job is tried again', function (): void {
    useDatabaseQueue();

    $parent = SagaFlow::create(ParentCancelPolicyWorkflow::class)->run();
    drainQueue();

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    app()->bind(StateMachine::class, DiesBeforeLanding::class);
    DiesBeforeLanding::$runId = $child->id;

    SagaFlow::loadFlow($parent->id)->compensate();

    // The job's own retry: the first attempt undid the child's step and died before the
    // child landed.
    drainQueue();

    expect(FlowRun::query()->findOrFail($child->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child']);

    CancelChildWorkflowJob::dispatch($child->id, FlowStatus::Cancelled, true);
    drainQueue();

    expect(FlowRun::query()->findOrFail($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child'])
        ->and(compensationTrail($child->id))->toBe(['child:completed']);
});

it('leaves an inline rollback taken over mid-undo to the recoverer that took it', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.sagas.reclaim.stale_running.enabled', true);
    config()->set('saga-lara-flow.sagas.reclaim.stale_running.after_seconds', 60);

    $run = SagaFlow::create(TakenOverMidUndoWorkflow::class)->run();
    drainQueue();

    // The first undo outlasts its reclaim window and the grace window, and the doctor
    // takes the rollback over while it is still running.
    $cancelled = SagaFlow::loadFlow($run->id)->compensate();

    expect(TakenOverMidUndoAction::$resumed)->toBe(1)
        ->and($cancelled->status)->toBe(FlowStatus::Cancelling)
        ->and(CompensationLog::all())->toBe(['undo:b']);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('carries a rollback to the end on a queue that stays further behind than the grace window', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();

    // Every job waits 61 s, so each pass of the sweep comes round before the oldest job
    // reaches a worker. Taking the rollback over on every one of them would leave each
    // owner's jobs stale by the time they ran.
    for ($pass = 0; $pass < 12 && FlowRun::query()->findOrFail($run->id)->status === FlowStatus::Cancelling; $pass++) {
        $this->travel(61)->seconds();
        app(FlowMonitor::class)->sweep();
        workOneJob();
    }

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves a compensation a worker may still be running until its reclaim window passes', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();

    // The worker took the row and died with it, and nothing marks it reclaimable.
    loseJobs('RunCompensationJob');
    CompensationRun::query()->where('flow_run_id', $run->id)->update([
        'status' => CompensationStatus::Running,
        'attempts' => 1,
        'started_at' => now(),
    ]);

    $this->travel(10)->minutes();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(0);

    // Written without touching updated_at, which would read as the row moving again.
    CompensationRun::query()->toBase()->where('flow_run_id', $run->id)->update(['reclaim_stale_at' => now()->subSecond()]);

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(1);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('lets the late job of the owner it was taken from claim nothing, and stops that owner', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();

    // The first level's job is only late, not lost.
    $late = loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(1);

    $resumed = loseJobs('RunCompensationJob');

    // The late job runs first, under the owner it was sent by.
    bringJobsBack($late);
    workOneJob();

    // Its continuation leaves the level to the owner that took it, rather than judging
    // the row it never ran as unfinished and landing the run.
    expect(CompensationLog::all())->toBe([])
        ->and(compensationTrail($run->id))->toBe(['b:pending'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);

    bringJobsBack($resumed);
    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(FlowRun::query()->findOrFail($run->id)->exception['compensation'] ?? null)->toBeNull()
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves the rest of a level to the recoverer that took the rollback over mid-level', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.sagas.reclaim.stale_running.enabled', true);
    config()->set('saga-lara-flow.sagas.reclaim.stale_running.after_seconds', 60);

    $run = SagaFlow::create(TakenOverMidLevelWorkflow::class)->run();
    drainQueue();

    $cancelled = SagaFlow::loadFlow($run->id)->compensate();

    // The inline owner finished the undo it was running, and claimed nothing after it.
    expect(TakenOverMidUndoAction::$resumed)->toBe(1)
        ->and($cancelled->status)->toBe(FlowStatus::Cancelling)
        ->and(CompensationLog::all())->toBe(['undo:x']);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:x', 'undo:y']);
});

it('leaves a level whose row a live worker took meanwhile, and takes the rollback over again once it settles', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();
    loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    $scanned = FlowRun::query()->findOrFail($run->id);

    // Between the scan and the takeover, the late job of the earlier owner claims the row.
    $b = CompensationRun::query()->where('flow_run_id', $run->id)->where('step_sequence', 1)->firstOrFail();

    expect(app(CompensationRecorder::class)->startCompensation($b, $scanned->rollback_token))->toBeTrue()
        ->and(app(FlowExecutor::class)->resumeRollback($scanned))->toBeTrue()
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0)
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling);

    // That undo finishes; nothing has moved the rollback since, so it is taken over again.
    expect(app(CompensationRecorder::class)->completeCompensation($b->fresh(), ['undone' => 'b']))->toBeTrue();

    $this->travel(61)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(1);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(FlowRun::query()->findOrFail($run->id)->exception['compensation'] ?? null)->toBeNull()
        ->and(CompensationLog::all())->toBe(['undo:a'])
        ->and(compensationTrail($run->id))->toBe(['b:completed', 'a:completed']);
});

it('lets one of two recoverers take a stalled rollback', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();
    loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    $first = FlowRun::query()->findOrFail($run->id);
    $second = FlowRun::query()->findOrFail($run->id);

    expect(app(FlowExecutor::class)->resumeRollback($first))->toBeTrue()
        ->and(app(FlowExecutor::class)->resumeRollback($second))->toBeFalse();

    drainQueue();

    expect(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('does not take over a rollback that landed since it was read', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();
    $late = loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    $scanned = FlowRun::query()->findOrFail($run->id);

    // The late job turns up after all and the rollback lands under its own owner.
    bringJobsBack($late);
    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(app(FlowExecutor::class)->resumeRollback($scanned))->toBeFalse()
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('lets one of two recoverers take it within the same second', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();
    loseJobs('RunCompensationJob');

    $this->freezeTime();

    $first = FlowRun::query()->findOrFail($run->id);
    $second = FlowRun::query()->findOrFail($run->id);

    expect(app(FlowExecutor::class)->resumeRollback($first))->toBeTrue()
        ->and(app(FlowExecutor::class)->resumeRollback($second))->toBeFalse()
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);
});

it('holds off a stalled expiry it cannot plan, and resumes it once it can', function (): void {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/stalled-rollback-'.uniqid().'.log');

    $run = stalledExpiry();
    loseJobs('RunCompensationJob');

    StalledSagaWorkflow::$fault = 'the record this argument reads is gone';
    $this->travel(61)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(0);

    $held = FlowRun::query()->findOrFail($run->id);

    expect($held->status)->toBe(FlowStatus::Cancelling)
        ->and($held->expiry_attempts)->toBe(1)
        ->and(substr_count((string) @file_get_contents($log), '"reason":"'.AnomalyLog::REASON_ROLLBACK_RESUME_FAILED.'"'))->toBe(1);

    StalledSagaWorkflow::$fault = null;

    $this->travel(61)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(1);

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves a rollback to Failed to the doctor, which carries it to the end with its cause', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = SagaFlow::create(StalledFailingSagaWorkflow::class)->run();

    for ($tick = 0; $tick < 20 && FlowRun::query()->findOrFail($run->id)->status !== FlowStatus::Cancelling; $tick++) {
        workOneJob();
    }

    loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(0);

    $report = app(FlowDoctor::class)->repair();

    expect($report->resumedRollbacks)->toBe(1)
        ->and(FlowRun::query()->findOrFail($run->id)->repair_attempts)->toBe(1);

    drainQueue();

    $failed = FlowRun::query()->findOrFail($run->id);

    expect($failed->status)->toBe(FlowStatus::Failed)
        ->and($failed->rollback_target)->toBe(FlowStatus::Failed)
        ->and($failed->exception['message'] ?? null)->not->toBeNull()
        ->and($failed->exception['message'] ?? null)->toBe($failed->rollback_cause['message'] ?? 'missing')
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('counts a takeover against the budget the run holds, not the one it was scanned with', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = SagaFlow::create(StalledFailingSagaWorkflow::class)->run();

    for ($tick = 0; $tick < 20 && FlowRun::query()->findOrFail($run->id)->status !== FlowStatus::Cancelling; $tick++) {
        workOneJob();
    }

    loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    // Another doctor counts takeovers of its own while this one is still replaying.
    StalledFailingSagaWorkflow::$onReplay = function () use ($run): void {
        FlowRun::query()->toBase()->where('id', $run->id)->update(['repair_attempts' => 4]);
    };

    $this->freezeTime();

    expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(1);

    $counted = FlowRun::query()->findOrFail($run->id);

    // The fifth attempt waits the fifth window: 10 s doubled four times.
    expect($counted->repair_attempts)->toBe(5)
        ->and($counted->repair_available_at?->toDateTimeString())->toBe(now()->addSeconds(160)->toDateTimeString());
});

it('leaves a stalled expiry to the monitor', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = stalledExpiry();
    loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(0)
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling);
});

it('decides from the writer that a rollback stalled', function (): void {
    useDatabaseQueue();

    $run = stalledExpiry();
    loseJobs('RunCompensationJob');

    $this->travel(61)->seconds();

    // A replica that has seen nothing at all: a scan reading it fails rather than
    // missing a compensation that moved since it last caught up.
    DB::connection('testing')->setReadPdo(new PDO('sqlite::memory:'));

    expect(collect(app(FlowRepository::class)->dueForRollbackRecovery(10, 60))->pluck('id')->all())->toBe([$run->id])
        ->and(collect(app(FlowRepository::class)->dueForRollbackRepair(10, 60, 10))->all())->toBe([]);
});

it('leaves a run in Cancelling that carries no intent', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);

    $run = SagaFlow::create(StalledSagaWorkflow::class)->run();
    drainQueue();

    // A rollback taken before its intent was written down: where it lands is not on record.
    FlowRun::query()->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);

    $this->travel(10)->minutes();

    expect(app(FlowMonitor::class)->sweep()['rollbacks'])->toBe(0)
        ->and(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(0)
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling);
});
