<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RaceOnTransition;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * A rollback is planned before the run is moved to Cancelling, and the plan describes
 * the run as it was when the replay read it. A step whose owed attempt lands in that
 * gap completes legitimately — the claim fence asks whether the run may still start
 * work, and it may — and lands outside a plan already drawn.
 *
 * The plan acted on is therefore made a second time, with the run already fenced.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    RaceOnTransition::reset();
    PlanGapPaymentAction::$calls = 0;
    PlanGapPaymentWorkflow::$fault = null;
});

/**
 * Fails its first native attempt and succeeds on the next, so the queue owes the row
 * another try while it sits Failed — the one window that needs no race to reach.
 */
final class PlanGapPaymentAction extends Action
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

final class PlanGapPaymentWorkflow extends Workflow
{
    /** A fault armed after the first plan, so only the second one meets it. */
    public static ?string $fault = null;

    public function handle(): void
    {
        if (self::$fault !== null) {
            throw new RuntimeException(self::$fault);
        }

        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(PlanGapPaymentAction::class, 'order-1')->compensateWith(UndoAction::class, 'p')->run();
        $this->awaitSignal('go');
    }
}

/**
 * The payment step registers a compensation even for its own failure, so the first plan
 * holds an entry a later one loses the moment an attempt claims that row.
 */
final class PlanGapSelfFailureWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->action(PlanGapPaymentAction::class, 'order-1')
            ->compensateStepOnSelfFailure()
            ->compensateWith(UndoAction::class, 'p')
            ->run();
        $this->awaitSignal('go');
    }
}

/**
 * Drive the run until the payment step has failed its first native attempt and the
 * queue still owes it another, then hand back that step.
 */
function planGapStepAwaitingItsLastTry(string $flowRunId, int $sequence): ActionRun
{
    for ($tick = 0; $tick < 12; $tick++) {
        $step = ActionRun::query()->where('flow_run_id', $flowRunId)->where('sequence', $sequence)->first();

        if ($step?->status === ActionStatus::Failed || DB::connection('testing')->table('jobs')->count() === 0) {
            break;
        }

        workOneJob();
    }

    $step = ActionRun::query()->where('flow_run_id', $flowRunId)->where('sequence', $sequence)->firstOrFail();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);

    return $step;
}

/**
 * Let the attempt the queue still owed land where a plan already drawn cannot see it:
 * after the caller planned, before it took control of the run.
 */
function planGapRaceOn(ActionRun $step, FlowStatus $status = FlowStatus::Cancelling): void
{
    RaceOnTransition::$on = $status;
    RaceOnTransition::$race = function () use ($step): void {
        app(ActionDispatcher::class)->execute($step->fresh());
    };
}

it('compensates a step that completed while the expiry was being planned', function (): void {
    useDatabaseQueue();
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $run = SagaFlow::create(PlanGapPaymentWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    planGapRaceOn(planGapStepAwaitingItsLastTry($run->id, 1));

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    expect(RaceOnTransition::$races)->toBe(1)
        ->and(PlanGapPaymentAction::$calls)->toBe(2)
        ->and(CompensationLog::all())->toBe(['undo:p', 'undo:a'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired);
});

it('compensates a step that completed while a manual rollback was being planned', function (): void {
    useDatabaseQueue();
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $run = SagaFlow::create(PlanGapPaymentWorkflow::class)->run();

    planGapRaceOn(planGapStepAwaitingItsLastTry($run->id, 1));

    SagaFlow::loadFlow($run->id)->compensate();

    expect(RaceOnTransition::$races)->toBe(1)
        ->and(CompensationLog::all())->toBe(['undo:p', 'undo:a'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelled);
});

it('keeps the plan it holds when the second one drops an entry from it', function (): void {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/plan-gap-short-'.uniqid().'.log');
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $run = SagaFlow::create(PlanGapSelfFailureWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    $step = planGapStepAwaitingItsLastTry($run->id, 1);

    // The owed attempt claims the row in the gap and gets no further, so the second
    // replay reads a step that is Running where the first read one that had failed.
    RaceOnTransition::$race = function () use ($step): void {
        expect(app(ActionRecorder::class)->startAction($step->fresh()))->toBeTrue();
    };

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    $lines = (string) @file_get_contents($log);

    expect(CompensationLog::all())->toBe(['undo:p', 'undo:a'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(substr_count($lines, '"reason":"replan_incomplete"'))->toBe(1)
        ->and($lines)->toContain('"dropped_sequences":[1]');
});

it('rolls back with the plan it holds when the second one throws', function (): void {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/plan-gap-'.uniqid().'.log');
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $run = SagaFlow::create(PlanGapPaymentWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    planGapStepAwaitingItsLastTry($run->id, 1);

    RaceOnTransition::$race = function (): void {
        PlanGapPaymentWorkflow::$fault = 'the record this argument reads is gone';
    };

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    $lines = (string) @file_get_contents($log);

    // Control was already taken, so the plan in hand is acted on rather than surfaced:
    // shorter than the truth by whatever landed in the gap, but never shorter than what
    // the caller would have unwound without a second plan at all.
    expect(CompensationLog::all())->toBe(['undo:a'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired)
        ->and(substr_count($lines, '"reason":"replan_failed"'))->toBe(1)
        ->and($lines)->toContain('"entity":"flow"');
});
