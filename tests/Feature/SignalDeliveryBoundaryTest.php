<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalCancellingFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WriterRoutedFlowRun;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * A run rolling back can never consume a signal: the resume that a delivery queues
 * cannot drive Cancelling, and terminal settlement closes only Waiting wait-markers.
 * Accepting one writes a row nobody reads and tells the caller it landed.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    WriterRoutedFlowRun::reset();
});

final class SignalBoundaryWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->awaitSignal('go');
    }
}

/**
 * A run parked on awaitSignal whose own deadline the sweep then enforces: the rollback
 * it plans is queued, so the run sits in Cancelling with its compensation still owed.
 */
function runRollingBack(): FlowRun
{
    $run = SagaFlow::create(SignalBoundaryWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Waiting);

    test()->travel(60)->seconds();

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1);

    $rollingBack = FlowRun::query()->findOrFail($run->id);

    expect($rollingBack->status)->toBe(FlowStatus::Cancelling);

    return $rollingBack;
}

it('refuses a signal delivered to a run that is rolling back', function (): void {
    useDatabaseQueue();

    $run = runRollingBack();

    $signals = DB::connection('testing')->table('saga_flow_signals')->count();
    $events = DB::connection('testing')->table('saga_flow_events')->count();
    $jobs = DB::connection('testing')->table('jobs')->count();

    expect(fn () => SagaFlow::loadFlow($run->id)->signal('go'))
        ->toThrow(CannotSignalCancellingFlowException::class);

    expect(DB::connection('testing')->table('saga_flow_signals')->count())->toBe($signals)
        ->and(DB::connection('testing')->table('saga_flow_events')->count())->toBe($events)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe($jobs);
});

it('reports the refusal through signalIfRunning', function (): void {
    useDatabaseQueue();

    $run = runRollingBack();

    expect(SagaFlow::loadFlow($run->id)->signalIfRunning('go'))->toBeFalse();
});

it('decides on the status the writer holds, not the one the handle carries', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalBoundaryWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    drainQueue();

    // handles() takes a page of snapshots in one pass, so by the time a bulk loop
    // reaches one its run has moved on. This handle still reads Waiting.
    $handle = SagaFlow::query()->signalable()->handles()->firstOrFail();

    expect($handle->status())->toBe(FlowStatus::Waiting);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling)
        ->and($handle->status())->toBe(FlowStatus::Waiting);

    // An attribute the caller has not saved: refusing must not refresh() the model the
    // handle keeps handing back, which would discard it.
    $handle->run()->queue = 'unsaved-by-the-caller';

    expect(fn () => $handle->signal('go'))->toThrow(CannotSignalCancellingFlowException::class);

    expect($handle->status())->toBe(FlowStatus::Waiting)
        ->and($handle->run()->queue)->toBe('unsaved-by-the-caller');
});

it('reads the status it decides on from the write connection', function (): void {
    config()->set('saga-lara-flow.models.flow_run', WriterRoutedFlowRun::class);

    useDatabaseQueue();

    $run = runRollingBack();

    // Every writer-bound read the scene itself made is behind us; what follows counts
    // only the delivery's own.
    WriterRoutedFlowRun::reset();

    expect(fn () => SagaFlow::loadFlow($run->id)->signal('go'))
        ->toThrow(CannotSignalCancellingFlowException::class);

    expect(WriterRoutedFlowRun::$writerReads)->toBe(1);
});

it('refuses to signal a run the writer no longer holds', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalBoundaryWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    drainQueue();

    $handle = SagaFlow::loadFlow($run->id);

    // A prune between loading the handle and using it. Nothing references the run any
    // more, so a delivery would write a signal row pointing at a run that is not there.
    FlowRun::query()->whereKey($run->id)->delete();

    $signals = DB::connection('testing')->table('saga_flow_signals')->count();

    expect(fn () => $handle->signal('go'))->toThrow(FlowNotFoundException::class)
        ->and(DB::connection('testing')->table('saga_flow_signals')->count())->toBe($signals);

    // The safe variant absorbs it like every other reason a run cannot take a signal.
    expect($handle->signalIfRunning('go'))->toBeFalse()
        ->and(DB::connection('testing')->table('saga_flow_signals')->count())->toBe($signals);
});

it('keeps both refusals under one parent so an existing catch still holds', function (): void {
    useDatabaseQueue();

    $run = runRollingBack();

    expect(fn () => SagaFlow::loadFlow($run->id)->signal('go'))
        ->toThrow(CannotSignalFlowException::class);

    $completed = SagaFlow::create(SignalBoundaryWorkflow::class)->run();

    drainQueue();
    SagaFlow::loadFlow($completed->id)->signal('go');
    drainQueue();

    expect(SagaFlow::findRun($completed->id)->status)->toBe(FlowStatus::Completed);

    expect(fn () => SagaFlow::loadFlow($completed->id)->signal('go'))
        ->toThrow(CannotSignalTerminalFlowException::class)
        ->and(fn () => SagaFlow::loadFlow($completed->id)->signal('go'))
        ->toThrow(CannotSignalFlowException::class);
});
