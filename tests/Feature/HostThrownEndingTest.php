<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowCancelledException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ExpiringActionSagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelFailFastWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ProbeHostThrowWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalTimeoutWorkflow;

/**
 * A collecting replay ends where a seam reports the live frontier, and five of the six
 * throws that say so are ordinary public exceptions. Only the seam that read the history
 * behind one may end the pass with it: a workflow raising the same class is a fault, and
 * reading that as a frontier stops the stack there and leaves everything past it applied
 * under a run reporting a complete unwind.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    ProbeHostThrowWorkflow::reset();
});

afterEach(function (): void {
    ProbeHostThrowWorkflow::reset();
});

/**
 * A run parked on its signal with both steps done, ready to be planned against.
 */
function parkedProbeRun(): string
{
    return SagaFlow::create(ProbeHostThrowWorkflow::class)->runSync()->id;
}

it('plans every compensation the replay reaches', function (): void {
    $id = parkedProbeRun();

    $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($id));

    expect($entries)->toHaveCount(2);

    $run = SagaFlow::loadFlow($id)->compensate();

    expect($run->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves an ending the workflow raised itself', function (string $ending): void {
    $id = parkedProbeRun();

    ProbeHostThrowWorkflow::$throw = $ending;

    expect(fn () => app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($id)))
        ->toThrow($ending, 'the host raised this one itself');
})->with([
    [ActionFailedException::class],
    [FlowExpiredException::class],
    [AwaitSignalTimeoutException::class],
    [ChildWorkflowFailedException::class],
    [ChildWorkflowCancelledException::class],
]);

it('leaves an ending the workflow built through the engine\'s own factory', function (): void {
    $id = parkedProbeRun();

    ProbeHostThrowWorkflow::$viaFactory = true;

    expect(fn () => app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($id)))
        ->toThrow(ActionFailedException::class, 'the host raised this one itself');
});

it('rolls nothing back when the workflow raised the ending itself', function (): void {
    $id = parkedProbeRun();

    ProbeHostThrowWorkflow::$throw = ActionFailedException::class;

    expect(fn () => SagaFlow::loadFlow($id)->compensate())
        ->toThrow(ActionFailedException::class);

    expect(SagaFlow::findRun($id)->status)->toBe(FlowStatus::Waiting)
        ->and(CompensationLog::all())->toBe([])
        ->and(CompensationRun::query()->where('flow_run_id', $id)->count())->toBe(0);
});

it('plans past an expiry one of its own seams raised', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(ExpiringActionSagaWorkflow::class)->run();

    // Three jobs take the run to its second step, scheduled and past its deadline:
    // the first drive, the first step, and the replay that schedules the second.
    workOneJob();
    workOneJob();
    workOneJob();

    expect(app(FlowMonitor::class)->sweep()['actions'])->toBe(1);

    $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    expect($entries)->toHaveCount(1);
});

it('plans past a signal timeout one of its own seams raised', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalTimeoutWorkflow::class)->run();
    drainQueue();

    expect(app(FlowMonitor::class)->sweep()['signals'])->toBe(1);

    $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    expect($entries)->toHaveCount(1);
});

it('plans past a parallel failure one of its own seams raised', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(ParallelFailFastWorkflow::class)->run();

    // Three jobs dispatch the block and settle both members, leaving the run parked
    // with one compensatable step done and the other hard-failed.
    workOneJob();
    workOneJob();
    workOneJob();

    $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    expect($entries)->toHaveCount(1);
});
