<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalCancellingFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\NoAwaitingRetrySignalException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\FlowHandle;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\DeclinableChargeAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaActionRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaBuilder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaFlowChild;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaFlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\NamedRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ScopedFlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SelfSignallingRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Queue;

/**
 * signalRetry() wakes a run parked by retryOnSignal() without the caller naming the
 * signal: the operator selecting failed runs by id knows which runs to retry, not which
 * signal each one waits on. It resolves the name from the parked step or child and then
 * delivers exactly as signal() does.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    FlakyPaymentAction::reset();
    LaggingReplicaBuilder::reset();
    ScopedFlowSignal::$hiddenName = null;

    useDatabaseQueue();
});

/**
 * A run of NamedRetryWorkflow left parked on $signal, its first attempt failed.
 */
function parkedOn(string $signal, bool $child = false, ?string $hold = null): FlowRun
{
    FlakyPaymentAction::$failures++;

    $run = SagaFlow::create(NamedRetryWorkflow::class)->withArguments($signal, $child, $hold)->run();

    drainQueue();

    $parked = FlowRun::query()->findOrFail($run->id);

    expect($parked->status)->toBe(FlowStatus::Waiting);

    return $parked;
}

function signalRows(): int
{
    return DB::connection('testing')->table('saga_flow_signals')->count();
}

it('wakes each selected run on the signal it is parked on', function (): void {
    $balance = parkedOn('balance-refilled');
    $stock = parkedOn('stock-synced');
    $gateway = parkedOn('gateway-recovered');

    SagaFlow::query()
        ->whereAwaitingRetrySignal()
        ->signalable()
        ->whereId($balance->id, $stock->id)
        ->handles()
        ->each(fn (FlowHandle $handle) => $handle->signalRetry());

    drainQueue();

    expect(SagaFlow::findRun($balance->id)->status)->toBe(FlowStatus::Completed)
        ->and(SagaFlow::findRun($stock->id)->status)->toBe(FlowStatus::Completed)
        ->and(FlowSignal::query()->where('flow_run_id', $balance->id)->pluck('name')->unique()->all())
        ->toBe(['balance-refilled'])
        ->and(FlowSignal::query()->where('flow_run_id', $stock->id)->pluck('name')->unique()->all())
        ->toBe(['stock-synced']);

    // Left out of the selection, so left parked.
    $unselected = SagaFlow::findRun($gateway->id);

    expect($unselected->status)->toBe(FlowStatus::Waiting)
        ->and($unselected->actions()->where('status', ActionStatus::AwaitingRetry)->count())->toBe(1);
});

it('wakes a parent parked on a failed child', function (): void {
    $run = parkedOn('analysis-recovered', child: true);

    expect(FlowChild::query()->where('parent_flow_run_id', $run->id)->firstOrFail()->status)
        ->toBe(ChildStatus::AwaitingRetry);

    SagaFlow::loadFlow($run->id)->signalRetry();

    drainQueue();

    $link = FlowChild::query()->where('parent_flow_run_id', $run->id)->firstOrFail();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and($link->status)->toBe(ChildStatus::Completed)
        ->and($link->retry_signal_attempts)->toBe(1)
        ->and(FlowRun::query()->where('parent_id', $run->id)->count())->toBe(2);
});

it('records every parked signal before it wakes the run', function (): void {
    // On a sync connection a wake drives the run inline, to its end if nothing stops it.
    config()->set('queue.default', 'sync');

    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(NamedRetryWorkflow::class)->withArguments('balance-refilled')->run();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);

    // A workflow cannot reach two parks at once, since replay stops at the first one, so the
    // second, with its wait, is written by hand.
    ActionRun::query()
        ->where('flow_run_id', $run->id)
        ->where('status', ActionStatus::AwaitingRetry)
        ->firstOrFail()
        ->replicate()
        ->fill(['sequence' => 99, 'retry_signal' => 'stock-synced'])
        ->save();

    FlowSignal::query()
        ->where('flow_run_id', $run->id)
        ->firstOrFail()
        ->replicate()
        ->fill(['wait_sequence' => 99, 'name' => 'stock-synced'])
        ->save();

    // One resume, and it starts with both deliveries already written.
    $deliveredAtResume = [];

    Queue::before(function (JobProcessing $event) use ($run, &$deliveredAtResume): void {
        if ($event->job->resolveName() === ResumeWorkflowJob::class) {
            $deliveredAtResume[] = FlowSignal::query()
                ->where('flow_run_id', $run->id)
                ->where('status', SignalStatus::Received)
                ->count();
        }
    });

    SagaFlow::loadFlow($run->id)->signalRetry();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and($deliveredAtResume)->toBe([2])
        ->and(FlowSignal::query()->where('flow_run_id', $run->id)->pluck('name')->sort()->values()->all())
        ->toBe(['balance-refilled', 'stock-synced']);
});

it('wakes a run whose delivery is already in without delivering again', function (): void {
    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);

    $run = parkedOn('balance-refilled');

    // Delivered, but the resume it should have brought never ran.
    SagaFlow::loadFlow($run->id)->signal('balance-refilled');

    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', true);

    $signals = signalRows();

    SagaFlow::loadFlow($run->id)->signalRetry();

    // A second delivery would float, and a later awaitSignal() of that name would take it.
    expect(signalRows())->toBe($signals);

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('leaves no floating signal when another delivery closes the wait first', function (): void {
    $run = parkedOn('balance-refilled');

    $signals = signalRows();

    // Another process delivers between this call reading the wait open and writing into it.
    $competed = false;

    FlowSignal::retrieved(function (FlowSignal $wait) use (&$competed): void {
        if (! $competed && $wait->status === SignalStatus::Waiting) {
            $competed = true;

            FlowSignal::query()->whereKey($wait->getKey())->update([
                'status' => SignalStatus::Received,
                'received_at' => now(),
            ]);
        }
    });

    SagaFlow::loadFlow($run->id)->signalRetry();

    expect($competed)->toBeTrue()
        ->and(signalRows())->toBe($signals);

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('fills only the wait of a park it found', function (): void {
    // After the retry, the run waits on an ordinary signal of the same name.
    $run = parkedOn('balance-refilled', hold: 'balance-refilled');

    // Another delivery ends the park just after this call has read it, and the run moves
    // on to that ordinary wait before this call looks for a wait to fill.
    $raced = false;

    ActionRun::retrieved(function (ActionRun $step) use ($run, &$raced): void {
        if (! $raced && $step->flow_run_id === $run->id) {
            $raced = true;

            SagaFlow::loadFlow($run->id)->signal('balance-refilled');
            drainQueue();
        }
    });

    SagaFlow::loadFlow($run->id)->signalRetry();

    drainQueue();

    $held = FlowSignal::query()->where('flow_run_id', $run->id)->orderByDesc('id')->firstOrFail();

    expect($raced)->toBeTrue()
        ->and(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting)
        ->and($held->status)->toBe(SignalStatus::Waiting);
});

it('reads whether the wait is still open from the write connection', function (): void {
    config()->set('saga-lara-flow.models.flow_signal', LaggingReplicaFlowSignal::class);

    $run = parkedOn('balance-refilled');

    // A replica that has not seen the wait yet must not turn the delivery into a bare wake.
    LaggingReplicaBuilder::$noSignals = true;

    SagaFlow::loadFlow($run->id)->signalRetry();

    LaggingReplicaBuilder::reset();

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('keeps the payload on the signal it delivers', function (): void {
    $run = parkedOn('balance-refilled');

    SagaFlow::loadFlow($run->id)->signalRetry(payload: ['by' => 'operator']);

    $signal = FlowSignal::query()
        ->where('flow_run_id', $run->id)
        ->where('status', SignalStatus::Received)
        ->firstOrFail();

    expect($signal->name)->toBe('balance-refilled')
        ->and($signal->payload)->toBe(['by' => 'operator']);
});

it('delivers a named signal as signal() would', function (): void {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->run();

    drainQueue();

    // Not a retry at all: a name is delivered as given, with no check of what waits on it.
    SagaFlow::loadFlow($run->id)->signalRetry('go');

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('refuses a run with nothing parked on a retry', function (): void {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->run();

    drainQueue();

    $signals = signalRows();
    $jobs = DB::connection('testing')->table('jobs')->count();

    expect(fn () => SagaFlow::loadFlow($run->id)->signalRetry())
        ->toThrow(NoAwaitingRetrySignalException::class)
        ->and(fn () => SagaFlow::loadFlow($run->id)->signalRetry())
        ->toThrow(CannotSignalFlowException::class)
        ->and(SagaFlow::loadFlow($run->id)->signalRetryIfRunning())->toBeFalse();

    expect(signalRows())->toBe($signals)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe($jobs);
});

it('does not count a retry that already went through', function (bool $child): void {
    $run = parkedOn('balance-refilled', $child, hold: 'hold');

    SagaFlow::loadFlow($run->id)->signalRetry();

    drainQueue();

    // Waiting again, on an ordinary signal: the retried work keeps its signal name on the
    // row, but nothing waits on it any more.
    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);

    $signals = signalRows();

    expect(fn () => SagaFlow::loadFlow($run->id)->signalRetry())
        ->toThrow(NoAwaitingRetrySignalException::class)
        ->and(signalRows())->toBe($signals);
})->with(['step' => [false], 'child' => [true]]);

it('leaves out a park whose wait timed out', function (bool $child): void {
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(NamedRetryWorkflow::class)
        ->withArguments('balance-refilled', $child, null, 60)
        ->run();

    drainQueue();

    $this->travel(120)->seconds();
    app(FlowMonitor::class)->sweep();

    // The wait is timed out and the resume that gives the retry up is queued, not yet run:
    // the park is still on the row, but no signal can end it any more.
    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting)
        ->and(FlowSignal::query()->where('flow_run_id', $run->id)->value('status'))
        ->toBe(SignalStatus::TimedOut);

    $signals = signalRows();

    expect(fn () => SagaFlow::loadFlow($run->id)->signalRetry())
        ->toThrow(NoAwaitingRetrySignalException::class)
        ->and(SagaFlow::loadFlow($run->id)->signalRetryIfRunning())->toBeFalse()
        ->and(signalRows())->toBe($signals)
        ->and(SagaFlow::query()->whereAwaitingRetrySignal()->whereId($run->id)->count())->toBe(0);

    Artisan::call('saga-flow:list');

    expect(Artisan::output())->toContain($run->id)->not->toContain('(retry:');

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Failed);
})->with(['step' => [false], 'child' => [true]]);

it('leaves out a step settled with its delivery still unconsumed', function (): void {
    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);

    $run = parkedOn('balance-refilled');

    // Delivered, never resumed, then cancelled: the step is settled, while the delivery keeps
    // its Received status for good.
    SagaFlow::loadFlow($run->id)->signal('balance-refilled');
    SagaFlow::loadFlow($run->id)->cancel();

    expect(FlowSignal::query()->where('flow_run_id', $run->id)->firstOrFail()->status)
        ->toBe(SignalStatus::Received)
        ->and(SagaFlow::query()->whereAwaitingRetrySignal()->whereId($run->id)->count())->toBe(0);
});

it('leaves out a park that has no wait to end', function (bool $child): void {
    $run = parkedOn('balance-refilled', $child);

    // History repaired or pruned by hand: replay keeps waiting on such a park and never
    // looks for a delivery.
    FlowSignal::query()->where('flow_run_id', $run->id)->delete();

    expect(fn () => SagaFlow::loadFlow($run->id)->signalRetry())
        ->toThrow(NoAwaitingRetrySignalException::class)
        ->and(signalRows())->toBe(0)
        ->and(SagaFlow::query()->whereAwaitingRetrySignal()->whereId($run->id)->count())->toBe(0);
})->with(['step' => [false], 'child' => [true]]);

it('reads the wait through the configured signal model and its scopes', function (): void {
    config()->set('saga-lara-flow.models.flow_signal', ScopedFlowSignal::class);

    $run = parkedOn('balance-refilled');

    // What the engine cannot see through the model, it cannot consume.
    ScopedFlowSignal::$hiddenName = 'balance-refilled';

    expect(fn () => SagaFlow::loadFlow($run->id)->signalRetry())
        ->toThrow(NoAwaitingRetrySignalException::class)
        ->and(SagaFlow::query()->whereAwaitingRetrySignal()->whereId($run->id)->count())->toBe(0);

    ScopedFlowSignal::$hiddenName = null;

    expect(SagaFlow::query()->whereAwaitingRetrySignal()->whereId($run->id)->count())->toBe(1);
});

it('refuses a run that finished after its handle was taken', function (): void {
    $run = parkedOn('balance-refilled');

    // handles() takes every snapshot in one query; by the time a loop reaches this one
    // the run has been cancelled, and its parked step settled with it.
    $handle = SagaFlow::query()->whereAwaitingRetrySignal()->handles()->firstOrFail();

    SagaFlow::loadFlow($run->id)->cancel();

    expect($handle->status())->toBe(FlowStatus::Waiting);

    $signals = signalRows();

    expect(fn () => $handle->signalRetry())->toThrow(CannotSignalTerminalFlowException::class)
        ->and($handle->signalRetryIfRunning())->toBeFalse()
        ->and(signalRows())->toBe($signals);
});

it('refuses a run that is rolling back', function (): void {
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(NamedRetryWorkflow::class)
        ->withArguments('balance-refilled')
        ->expiresAt(now()->addSeconds(30))
        ->run();

    drainQueue();

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    $rollingBack = FlowRun::query()->findOrFail($run->id);

    // Still parked: the step is settled only when the rollback lands the run.
    expect($rollingBack->status)->toBe(FlowStatus::Cancelling)
        ->and($rollingBack->actions()->where('status', ActionStatus::AwaitingRetry)->count())->toBe(1);

    $signals = signalRows();

    expect(fn () => SagaFlow::loadFlow($run->id)->signalRetry())
        ->toThrow(CannotSignalCancellingFlowException::class)
        ->and(SagaFlow::loadFlow($run->id)->signalRetryIfRunning())->toBeFalse()
        ->and(signalRows())->toBe($signals);
});

it('refuses a run the writer no longer holds', function (): void {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->run();

    drainQueue();

    $handle = SagaFlow::loadFlow($run->id);

    FlowRun::query()->whereKey($run->id)->delete();

    expect(fn () => $handle->signalRetry())->toThrow(FlowNotFoundException::class)
        ->and($handle->signalRetryIfRunning())->toBeFalse();
});

it('finds a parked step on the write connection', function (): void {
    config()->set('saga-lara-flow.models.action_run', LaggingReplicaActionRun::class);

    $run = parkedOn('balance-refilled');

    LaggingReplicaBuilder::$noSteps = true;

    SagaFlow::loadFlow($run->id)->signalRetry();

    LaggingReplicaBuilder::reset();

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('finds a parked child on the write connection', function (): void {
    config()->set('saga-lara-flow.models.flow_child', LaggingReplicaFlowChild::class);

    $run = parkedOn('analysis-recovered', child: true);

    LaggingReplicaBuilder::$noLinks = true;

    SagaFlow::loadFlow($run->id)->signalRetry();

    LaggingReplicaBuilder::reset();

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});

it('refuses a predicate that retries the run it is deciding for', function (): void {
    DeclinableChargeAction::reset(failures: 99);

    $run = SagaFlow::create(SelfSignallingRetryWorkflow::class)->withArguments('order-1')->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(RetryPolicyReentryException::class)
        ->and($run->signals()->count())->toBe(0);
});
