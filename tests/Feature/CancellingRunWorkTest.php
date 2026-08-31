<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionRetried;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * A run rolling back plans the stack it will undo once. Work that starts after that
 * plan finishes outside it: its compensation is in no stack, never runs, and the run
 * lands terminal reporting a complete unwind over a step that is still applied.
 *
 * Not finished is therefore the wrong question at the seams where work BEGINS, and the
 * right one everywhere a row already started is settled or written down. The two are
 * separate predicates because a rollback needs the second to keep working while the
 * first is closed.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    RetriedPaymentAction::$calls = 0;
    CancelMidStepAction::$calls = 0;
    CancelMidStepAction::$runId = null;
    FlakyPaymentAction::reset(5);
});

/**
 * Fails its first native attempt and succeeds on the next, so the queue owes the row
 * another try while it sits Failed — the one status terminal settlement leaves exactly
 * as it found it, and therefore the window that needs no race to reach.
 */
final class RetriedPaymentAction extends Action
{
    public int $tries = 3;

    public static int $calls = 0;

    /**
     * @return array{charged: string}
     */
    public function handle(string $orderId): array
    {
        self::$calls++;

        if (self::$calls === 1) {
            throw new RuntimeException('insufficient balance');
        }

        return ['charged' => $orderId];
    }
}

/**
 * Moves its own run into Cancelling from inside handle(), so the rollback begins while
 * this step is executing: the claim is already won, and everything written afterwards
 * is a record of work that really ran.
 */
final class CancelMidStepAction extends Action
{
    public static int $calls = 0;

    public static ?string $runId = null;

    /**
     * @return array{charged: string}
     */
    public function handle(string $orderId): array
    {
        self::$calls++;

        app(StateMachine::class)->transition(
            FlowRun::query()->findOrFail((string) self::$runId),
            FlowStatus::Cancelling,
        );

        throw new RuntimeException('insufficient balance');
    }
}

final class RetriedPaymentWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(RetriedPaymentAction::class, 'order-1')->compensateWith(UndoAction::class, 'p')->run();
        $this->awaitSignal('go');
    }
}

final class CancelMidStepWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(CancelMidStepAction::class, 'order-1')->compensateWith(UndoAction::class, 'p')->run();
    }
}

/**
 * Drive the run until the payment step has failed its first native attempt and the
 * queue still owes it another, then hand back that step.
 */
function stepFailedBetweenTries(string $flowRunId): ActionRun
{
    for ($tick = 0; $tick < 12; $tick++) {
        $step = ActionRun::query()->where('flow_run_id', $flowRunId)->where('sequence', 1)->first();

        if ($step?->status === ActionStatus::Failed || DB::connection('testing')->table('jobs')->count() === 0) {
            break;
        }

        workOneJob();
    }

    $step = ActionRun::query()->where('flow_run_id', $flowRunId)->where('sequence', 1)->firstOrFail();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);

    return $step;
}

/**
 * A step written straight into the database under a run in the given status, aged so
 * the doctor's grace period has passed. Older rows sort first — which is what puts one
 * at the head of every batch.
 *
 * @param  array<string, mixed>  $attributes
 */
function repairCandidate(
    FlowStatus $runStatus,
    int $ageMinutes,
    ActionStatus $stepStatus = ActionStatus::Pending,
    array $attributes = [],
): ActionRun {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => $runStatus,
        'arguments' => [],
    ]);

    $step = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => $stepStatus,
        'attempts' => 0,
        ...$attributes,
    ]);

    ActionRun::query()->whereKey($step->id)->update(['created_at' => now()->subMinutes($ageMinutes)]);

    return $step->fresh();
}

it('does not run a step whose job outlived the start of the rollback', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(RetriedPaymentWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    stepFailedBetweenTries($run->id);

    // The sweep expires the run, which plans the rollback and moves it to Cancelling.
    $this->travel(60)->seconds();

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1)
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling);

    // The native retry the queue still owed now arrives, under a run rolling back.
    drainQueue();

    $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 1)->firstOrFail();

    expect(RetriedPaymentAction::$calls)->toBe(1)
        ->and($step->status)->toBe(ActionStatus::Failed)
        ->and($step->result)->toBeNull()
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired);
});

it('journals the refused claim and leaves the job nothing to retry', function (): void {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/cancelling-claim-'.uniqid().'.log');

    $run = SagaFlow::create(RetriedPaymentWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    stepFailedBetweenTries($run->id);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    $lines = (string) @file_get_contents($log);

    // One refusal, named, and no queue failure behind it: a lost claim means somebody
    // else owns the row, which is not this job's business to retry.
    expect(substr_count($lines, '"reason":"claim_lost"'))->toBe(1)
        ->and($lines)->toContain('"entity":"action"')
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

it('settles a step it refused to start rather than leaving it open', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(RetriedPaymentWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    // Stop while the payment step is still Pending and its first job is queued.
    for ($tick = 0; $tick < 12; $tick++) {
        $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 1)->first();

        if ($step?->status === ActionStatus::Pending || DB::connection('testing')->table('jobs')->count() === 0) {
            break;
        }

        workOneJob();
    }

    expect(ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 1)->firstOrFail()->status)
        ->toBe(ActionStatus::Pending);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 1)->firstOrFail();

    expect(RetriedPaymentAction::$calls)->toBe(0)
        ->and($step->status)->toBe(ActionStatus::Cancelled)
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired);
});

it('still runs the compensations of the rollback that closed the run', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(RetriedPaymentWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    stepFailedBetweenTries($run->id);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    // The compensations run under Cancelling, which is exactly the status the claim is
    // now fenced against: they are claimed on their own rows, never on the run's state.
    expect(CompensationLog::all())->toBe(['undo:a'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired);
});

it('records that the queue is finished with a step the cancel caught mid-execution', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(CancelMidStepWorkflow::class)->run();
    CancelMidStepAction::$runId = $run->id;

    drainQueue();

    $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 1)->firstOrFail();

    // The claim was won before the rollback began, so what follows is bookkeeping about
    // work that really ran. Refusing it would tell the retry seam the queue still owes
    // an attempt nobody will ever send.
    expect(CancelMidStepAction::$calls)->toBe(1)
        ->and($step->status)->toBe(ActionStatus::Failed)
        ->and($step->queue_attempts_exhausted)->toBeTrue()
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling);
});

it('does not let a rolling-back run hold the head of the repair batch', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.repair.batch_size', 1);
    config()->set('saga-lara-flow.repair.redispatch_lost_actions', true);

    // Older, so it sorts first; its rule refuses it after it has taken the slot and
    // before anything holds it off, so its window never moves.
    $rollingBack = repairCandidate(FlowStatus::Cancelling, 30);
    $stuck = repairCandidate(FlowStatus::Waiting, 10);

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(1)
        ->and($report->skipped)->toBe(0)
        ->and($stuck->fresh()->repair_attempts)->toBe(1)
        ->and($rollingBack->fresh()->repair_attempts)->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);
});

it('still expires an overdue step under a run that is rolling back', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(RetriedPaymentWorkflow::class)->run();
    $step = stepFailedBetweenTries($run->id);

    // A rollback long enough for the step's own deadline to pass under it. Settling
    // that step is not starting work, and leaving it unresolved for the length of the
    // rollback would be the sweep failing at the one thing it is for.
    app(StateMachine::class)->transition(FlowRun::query()->findOrFail($run->id), FlowStatus::Cancelling);
    $step->newQuery()->toBase()->where('id', $step->id)->update([
        'status' => ActionStatus::Pending->value,
        'expires_at' => now()->subMinute(),
    ]);

    $report = app(FlowMonitor::class)->sweep();

    expect($report['actions'])->toBe(1)
        ->and($step->fresh()->status)->toBe(ActionStatus::Expired)
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling);
});

it('refuses a repair candidate the rollback claimed after the batch was read', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.repair.redispatch_lost_actions', true);

    $stuck = repairCandidate(FlowStatus::Waiting, 10);
    $table = (new FlowRun)->getTable();

    // The rollback starts in the gap the batch leaves open: between the scan that read
    // this row and the lock the rule takes on it. One process, staged on the scan's own
    // query, because the check under the lock exists for exactly this interleaving.
    DB::listen(function ($query) use ($stuck, $table): void {
        if (str_contains($query->sql, 'action_runs') && str_contains($query->sql, 'created_at')) {
            DB::connection('testing')->table($table)
                ->where('id', $stuck->flow_run_id)
                ->update(['status' => FlowStatus::Cancelling->value]);
        }
    });

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(0)
        ->and($report->skipped)->toBe(1)
        ->and($stuck->fresh()->repair_attempts)->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

it('does not spend a retry cycle on a run the rollback claimed mid-replay', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->run();
    drainQueue();

    $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 1)->firstOrFail();

    expect($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal_attempts)->toBe(0);

    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    // The rollback commits while this replay is under way. Staged on the seam's own
    // read of the wait-signal: that happens outside any transaction and before the one
    // the retry writes in, so the row the fence reads really does say Cancelling.
    $staged = false;
    FlowSignal::retrieved(function () use ($run, &$staged): void {
        if ($staged) {
            return;
        }

        $staged = true;

        app(StateMachine::class)->transition(
            FlowRun::query()->findOrFail($run->id),
            FlowStatus::Cancelling,
        );
    });

    $announced = 0;
    Event::listen(ActionRetried::class, function () use (&$announced): void {
        $announced++;
    });

    drainQueue();

    $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 1)->firstOrFail();
    $signal = FlowSignal::query()->where('flow_run_id', $run->id)->orderByDesc('id')->firstOrFail();

    // A retry spends the delivery and a unit of the budget and then sends a job. None of
    // that may happen once the run is rolling back: the step it would restart lands
    // outside the stack that rollback already planned.
    expect($staged)->toBeTrue()
        ->and($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal_attempts)->toBe(0)
        ->and($signal->status->value)->toBe('received')
        ->and($announced)->toBe(0)
        ->and(FlakyPaymentAction::$calls)->toBe(1);
});

it('does not let a rolling-back run hold the head of the stale-running batch', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.repair.batch_size', 1);
    config()->set('saga-lara-flow.repair.redispatch_stale_running_actions', true);

    // R3 orders by reclaim_stale_at, so the one that went stale first leads the page.
    $rollingBack = repairCandidate(FlowStatus::Cancelling, 30, ActionStatus::Running, [
        'reclaim_stale_at' => now()->subMinutes(30),
    ]);
    $stuck = repairCandidate(FlowStatus::Running, 10, ActionStatus::Running, [
        'reclaim_stale_at' => now()->subMinutes(10),
    ]);

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(1)
        ->and($report->skipped)->toBe(0)
        ->and($stuck->fresh()->repair_attempts)->toBe(1)
        ->and($rollingBack->fresh()->repair_attempts)->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);
});

it('refuses a stale-running candidate the rollback claimed after the batch was read', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.repair.redispatch_stale_running_actions', true);

    $stuck = repairCandidate(FlowStatus::Running, 10, ActionStatus::Running, [
        'reclaim_stale_at' => now()->subMinutes(10),
    ]);
    $table = (new FlowRun)->getTable();

    DB::listen(function ($query) use ($stuck, $table): void {
        if (str_contains($query->sql, 'action_runs') && str_contains($query->sql, 'reclaim_stale_at')) {
            DB::connection('testing')->table($table)
                ->where('id', $stuck->flow_run_id)
                ->update(['status' => FlowStatus::Cancelling->value]);
        }
    });

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(0)
        ->and($report->skipped)->toBe(1)
        ->and($stuck->fresh()->repair_attempts)->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});
