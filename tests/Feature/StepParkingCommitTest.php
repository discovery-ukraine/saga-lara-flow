<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionAwaitingRetry;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaBuilder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaFlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RetryOnSignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ScopedFlowEvent;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * Parking a failed step on its retry signal writes the wait-signal, the step's move to
 * AwaitingRetry and its action.awaiting_retry row in one transaction. A model observer on
 * that row runs inside it, and on PostgreSQL a failing statement the observer swallows
 * aborts the transaction: the COMMIT that follows reports success and rolls back. SQLite
 * and MySQL let the transaction carry on, so there the parking holds.
 *
 * The tests say the same thing on every driver — whether the parking held is the
 * driver's business, but the announcement, the anomaly journal and the run's way out
 * agree with it. The reference shape is TransactionIntegrityTest.
 */
beforeEach(function () {
    FlakyPaymentAction::reset(failures: 99);
    CompensationLog::reset();
    LaggingReplicaBuilder::reset();
    ScopedFlowEvent::$hiddenType = null;
});

afterEach(function () {
    LaggingReplicaBuilder::reset();
    ScopedFlowEvent::$hiddenType = null;
});

/**
 * Poison the next $times action.awaiting_retry rows with a failure the observer swallows.
 */
function swallowAQueryFailureOnParking(int $times = PHP_INT_MAX): void
{
    FlowEvent::created(function (FlowEvent $event) use (&$times): void {
        if ($event->type !== FlowEventType::ActionAwaitingRetry || $times <= 0) {
            return;
        }

        $times--;

        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // An observer that eats its own failure — broken, but nothing stops it.
        }
    });
}

function parkedStep(FlowRun $run): ?ActionRun
{
    return $run->actions()->where('sequence', 1)->where('status', ActionStatus::AwaitingRetry)->first();
}

it('announces a parking only once it is on record', function () {
    swallowAQueryFailureOnParking();

    $seen = [];
    Event::listen(ActionAwaitingRetry::class, function (ActionAwaitingRetry $event) use (&$seen): void {
        $seen[] = ActionRun::query()->useWritePdo()->whereKey($event->actionRun->id)->value('status');
    });

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->runSync();

    $parked = parkedStep($run) !== null;

    // One announcement per parking that holds, none for one the database discarded, and
    // a listener that reads the step finds it where the event says it is.
    expect($seen)->toBe($parked ? [ActionStatus::AwaitingRetry] : [])
        ->and($run->signals()->where('status', SignalStatus::Waiting)->count())->toBe($parked ? 1 : 0);
});

it('journals a parking its own transaction discarded', function () {
    $path = sys_get_temp_dir().'/saga-anomaly-'.uniqid().'.log';
    logToFile($path);

    swallowAQueryFailureOnParking();

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->runSync();

    $parked = parkedStep($run) !== null;
    $step = $run->actions()->where('sequence', 1)->first();

    // Nothing fails the run, so this line is the only place the cause is written down.
    $log = is_file($path) ? (string) file_get_contents($path) : '';

    expect(str_contains($log, AnomalyLog::REASON_CLAIM_NOT_COMMITTED))->toBe(! $parked)
        ->and(str_contains($log, $step->id))->toBe(! $parked);
});

it('parks again once the run whose parking was discarded is woken', function (string $mode, string $waker) {
    useDatabaseQueue();
    swallowAQueryFailureOnParking(times: 1);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1');
    $run = $mode === 'sync' ? $run->runSync() : $run->run();
    drainQueue();

    if ($waker === 'doctor') {
        config()->set('saga-lara-flow.repair.enabled', true);
        config('saga-lara-flow.models.flow_run')::query()
            ->whereKey($run->id)
            ->update(['updated_at' => now()->subMinutes(2)]);

        app(FlowDoctor::class)->repair();
    } else {
        SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    }

    drainQueue();

    // A discarded parking leaves the step Failed with no wait recorded. Whatever wakes
    // the run next — the doctor's lost-resume pass, or the delivery the parking was
    // waiting for — has to bring the step back to a parking that holds.
    expect(parkedStep(SagaFlow::findRun($run->id)))->not->toBeNull()
        ->and($run->signals()->where('status', SignalStatus::Waiting)->count())->toBe(1);
})->with([['sync'], ['queued']])->with([['doctor'], ['signal']]);

it('parks again once a retried attempt whose parking was discarded is woken', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->runSync();

    // The retried attempt runs inline too, and fails again; its parking is the one
    // the database throws away.
    swallowAQueryFailureOnParking(times: 1);
    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Sync);

    config()->set('saga-lara-flow.repair.enabled', true);
    config('saga-lara-flow.models.flow_run')::query()
        ->whereKey($run->id)
        ->update(['updated_at' => now()->subMinutes(2)]);

    app(FlowDoctor::class)->repair();
    drainQueue();

    $step = parkedStep(SagaFlow::findRun($run->id));

    expect($step)->not->toBeNull()
        ->and($step->retry_signal_attempts)->toBe(1)
        ->and($run->signals()->where('status', SignalStatus::Waiting)->count())->toBe(1);
});

it('reads the parking back from the write connection', function () {
    $path = sys_get_temp_dir().'/saga-anomaly-'.uniqid().'.log';
    logToFile($path);

    config()->set('saga-lara-flow.models.flow_event', LaggingReplicaFlowEvent::class);

    // A replica that has not seen the parking's history row yet must not turn a
    // parking that holds into a lost one.
    LaggingReplicaBuilder::$noEvents = true;

    $announced = 0;
    Event::listen(ActionAwaitingRetry::class, function () use (&$announced): void {
        $announced++;
    });

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->runSync();

    LaggingReplicaBuilder::reset();

    expect(parkedStep($run))->not->toBeNull()
        ->and($announced)->toBe(1)
        ->and(is_file($path) ? (string) file_get_contents($path) : '')
        ->not->toContain(AnomalyLog::REASON_CLAIM_NOT_COMMITTED);
});

it('does not take a parking for lost when the step moves on before it is read back', function () {
    $path = sys_get_temp_dir().'/saga-anomaly-'.uniqid().'.log';
    logToFile($path);

    $announced = 0;
    Event::listen(ActionAwaitingRetry::class, function () use (&$announced): void {
        $announced++;
    });

    // Between the parking's commit and its read-back, another process delivers the
    // signal and spends the cycle: the step is rewound to Pending on its next cycle.
    $reached = beforeStatement('/^select exists\(select \* from [`"]?\w*flow_events[`"]? where/i', function (): void {
        ActionRun::query()
            ->where('sequence', 1)
            ->update(['status' => ActionStatus::Pending, 'retry_signal_attempts' => 1]);
    });

    SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->runSync();

    expect($reached())->toBeTrue()
        ->and($announced)->toBe(1)
        ->and(is_file($path) ? (string) file_get_contents($path) : '')
        ->not->toContain(AnomalyLog::REASON_CLAIM_NOT_COMMITTED);
});

it('reads the parking back past the event model\'s own scopes', function () {
    $path = sys_get_temp_dir().'/saga-anomaly-'.uniqid().'.log';
    logToFile($path);

    // A host's scope may hide history rows from its own reads; whether the parking was
    // written is a question about the row, not about what the scope lets through.
    config()->set('saga-lara-flow.models.flow_event', ScopedFlowEvent::class);
    ScopedFlowEvent::$hiddenType = FlowEventType::ActionAwaitingRetry;

    $announced = 0;
    Event::listen(ActionAwaitingRetry::class, function () use (&$announced): void {
        $announced++;
    });

    $run = SagaFlow::create(RetryOnSignalWorkflow::class)->withArguments('order-1')->runSync();

    expect(parkedStep($run))->not->toBeNull()
        ->and($announced)->toBe(1)
        ->and(is_file($path) ? (string) file_get_contents($path) : '')
        ->not->toContain(AnomalyLog::REASON_CLAIM_NOT_COMMITTED);
});
