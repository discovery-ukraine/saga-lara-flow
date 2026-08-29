<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationStepStarted;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionClaimFailedException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CountedActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailedStepWithCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * ActionRecorder::startAction() claims the row, records action.started and fires the
 * ActionStarted event in one transaction, so a listener cannot leave the row Running
 * with nothing executing it. That listener is the one place a caller's code runs inside
 * a transaction the engine opened, and the drivers do not agree about what a failed
 * statement does to one: PostgreSQL aborts the whole transaction and turns the eventual
 * COMMIT into a rollback, reporting success either way, while SQLite and MySQL let the
 * caller carry on.
 *
 * A listener that throws is already handled — the claim rolls back with it. The shape
 * that gets past the guarantee is one that runs a failing query and SWALLOWS it, which
 * is why the claim is read back after the transaction closes rather than trusted to the
 * commit that reported success.
 *
 * The tests below say the same thing on every driver: whether the claim held is the
 * driver's business, but the step body and the event that announces it agree with it.
 */
beforeEach(function () {
    FlakyPaymentAction::reset();
    CompensationLog::reset();
});

function swallowAQueryFailureOnStart(): void
{
    Event::listen(ActionStarted::class, function (): void {
        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // A listener that eats its own failure — broken, but nothing stops it.
        }
    });
}

function claimIsOnRecord(FlowRun $run): bool
{
    return $run->events()->where('type', FlowEventType::ActionStarted)->exists();
}

it('never records a claim without its event', function () {
    swallowAQueryFailureOnStart();

    $run = SagaFlow::create(CountedActionWorkflow::class)->runSync();

    $step = $run->actions()->first();
    $started = claimIsOnRecord($run);

    // Both halves of the claim or neither: the row's own marks and the event that
    // announces them are written in the same transaction and cannot come apart.
    //
    // `attempts` is what says whether the row was marked, because only the claim's own
    // UPDATE increments it. The status does not: a run that ends loudly because its
    // claim was discarded settles its open steps on the way out, so an unclaimed step
    // is Cancelled by then rather than still Pending.
    expect($started)->toBe($step->attempts > 0)
        ->and($step->attempts)->toBe($started ? 1 : 0);
});

it('does not execute a step it failed to claim', function () {
    swallowAQueryFailureOnStart();

    $run = SagaFlow::create(CountedActionWorkflow::class)->runSync();

    // The half that at-least-once rests on. A body that ran under a claim the database
    // never kept is a charge nobody can attribute, and the next replay makes a second
    // one.
    expect(FlakyPaymentAction::$calls)->toBe(claimIsOnRecord($run) ? 1 : 0);
});

it('journals a claim its own transaction discarded', function () {
    $path = sys_get_temp_dir().'/saga-anomaly-'.uniqid().'.log';
    logToFile($path);

    swallowAQueryFailureOnStart();

    $run = SagaFlow::create(CountedActionWorkflow::class)->runSync();

    $step = $run->actions()->first();
    $started = claimIsOnRecord($run);

    // No anomaly means no file at all: the channel writes lazily, and a driver where
    // the claim held has nothing to journal.
    $log = is_file($path) ? (string) file_get_contents($path) : '';

    // Nothing fails the job, so this line is the only place the cause is written down:
    // without it an operator sees a run that stopped for no stated reason. Which of the
    // two outcomes a driver produces is its own business — but they are the only two,
    // and the journal accounts for the one that lost the claim.
    expect(str_contains($log, AnomalyLog::REASON_CLAIM_NOT_COMMITTED))->toBe(! $started)
        ->and(str_contains($log, $step->id))->toBe(! $started);
});

it('does not execute an undo it failed to claim', function () {
    Event::listen(CompensationStepStarted::class, function (): void {
        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // The same broken listener, on the rollback half of the engine.
        }
    });

    $failure = null;

    try {
        SagaFlow::create(FailedStepWithCompensationWorkflow::class)->runSync();
    } catch (ActionClaimFailedException $e) {
        $failure = $e;
    }

    $undo = CompensationRun::query()->orderBy('sequence')->first();

    // An undo that ran under a claim the database never kept is worse than a step that
    // did: the row says the rollback never happened, so a later pass runs it again and
    // one charge is refunded twice.
    expect(CompensationLog::all())->toHaveCount($undo->attempts)
        // And a rollback that cannot record its undo stops loudly instead of reporting
        // itself finished. Sync compensation claims a row it created in the same call,
        // so a claim it does not end up holding is a broken invariant, not a race.
        ->and($failure !== null)->toBe($undo->attempts === 0);
});

/**
 * The listener is not the only caller code inside that transaction, and the fix has to
 * cover the rest of it. Moving ActionStarted to ShouldDispatchAfterCommit would make
 * every test above green and leave this one red: the action.started row is written
 * inside the transaction whatever the event does, and a model observer on it runs
 * there too.
 */
it('never records a claim without its event, poisoned through a model observer', function () {
    FlowEvent::created(function (): void {
        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // No listener anywhere; the engine's own insert carries the failure in.
        }
    });

    $run = SagaFlow::create(CountedActionWorkflow::class)->runSync();

    $step = $run->actions()->first();

    expect(FlakyPaymentAction::$calls)->toBe($step->attempts)
        ->and($step->attempts)->toBe(claimIsOnRecord($run) ? 1 : 0);
});
