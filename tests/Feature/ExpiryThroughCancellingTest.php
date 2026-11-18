<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RaceOnTransition;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * An expiring run with nothing to undo takes the same road as one with something: into
 * Cancelling, a second plan, and only then Expired. The plan drawn before the move can be
 * empty and still be wrong — a step whose owed attempt lands between that plan and the
 * terminal write completes, and nothing undoes it under a run reported expired.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    RaceOnTransition::reset();
    OnlyStepPaymentAction::$calls = 0;
});

/**
 * Fails its first native attempt and succeeds on the next, so the queue owes the row
 * another try while it sits Failed and the plan has nothing to undo.
 */
final class OnlyStepPaymentAction extends Action
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

final class OnlyStepWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(OnlyStepPaymentAction::class, 'order-1')->compensateWith(UndoAction::class, 'p')->run();
        $this->awaitSignal('go');
    }
}

final class NothingToUndoWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'x')->run();
        $this->awaitSignal('go');
    }
}

it('compensates a step that completed while an empty expiry plan was being drawn', function (): void {
    useDatabaseQueue();
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $run = SagaFlow::create(OnlyStepWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    for ($tick = 0; $tick < 12; $tick++) {
        $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 0)->first();

        if ($step?->status === ActionStatus::Failed) {
            break;
        }

        workOneJob();
    }

    $step = ActionRun::query()->where('flow_run_id', $run->id)->where('sequence', 0)->firstOrFail();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);

    // The owed attempt lands after the empty plan and before the run is taken.
    RaceOnTransition::$race = function () use ($step): void {
        app(ActionDispatcher::class)->execute($step->fresh());
    };

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    expect(RaceOnTransition::$races)->toBe(1)
        ->and($step->fresh()->status)->toBe(ActionStatus::Completed)
        ->and(CompensationLog::all())->toBe(['undo:p'])
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired);
});

it('expires a run with nothing to undo through Cancelling, with the expiry as its intent', function (): void {
    useDatabaseQueue();

    $seen = [];

    RaceOnTransition::$on = FlowStatus::Expired;
    RaceOnTransition::$race = function (FlowRun $moving) use (&$seen): void {
        $seen[] = FlowRun::query()->findOrFail($moving->id)->status;
    };
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $run = SagaFlow::create(NothingToUndoWorkflow::class)->expiresAt(now()->addSeconds(30))->run();
    drainQueue();

    $this->travel(60)->seconds();

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1);

    $expired = FlowRun::query()->findOrFail($run->id);

    expect($seen)->toBe([FlowStatus::Cancelling])
        ->and($expired->status)->toBe(FlowStatus::Expired)
        ->and($expired->rollback_target)->toBe(FlowStatus::Expired)
        ->and($expired->rollback_token)->not->toBeNull()
        ->and($expired->rollback_cause['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and($expired->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and($expired->compensations()->count())->toBe(0)
        ->and(CompensationLog::all())->toBe([]);
});
