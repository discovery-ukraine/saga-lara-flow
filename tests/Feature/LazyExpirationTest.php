<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ExpiringSagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;

beforeEach(fn () => CompensationLog::reset());

/**
 * Park a run in Waiting and mark its deadline as already past, returning the
 * reloaded model so a test can drive it directly (no monitor involved).
 */
function parkOverdueRun(string $workflowClass): FlowRun
{
    useDatabaseQueue();

    $run = SagaFlow::create($workflowClass)->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);

    expect($run->status)->toBe(FlowStatus::Waiting);

    $run->expires_at = now()->subMinute();
    $run->save();

    return $run;
}

it('expires an overdue run on drive() without the monitor (no compensations)', function () {
    $run = parkOverdueRun(SignalOnlyWorkflow::class);

    // Drive directly: the lazy deadline check fires before any replay step.
    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    expect($driven->status)->toBe(FlowStatus::Expired)
        ->and(SagaFlow::findRun($run->id)->events()
            ->where('type', FlowEventType::FlowExpired->value)->count())->toBe(1);
});

it('rolls back before Expired when an overdue run is driven directly', function () {
    $run = parkOverdueRun(ExpiringSagaWorkflow::class);

    app(FlowExecutor::class)->drive($run, RunMode::Queued);

    // Rollback runs as queued compensation jobs; drain them to finalize.
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toContain('undo:a')
        ->and($final->events()->where('type', FlowEventType::FlowExpired->value)->count())->toBe(1);
});

it('does not expire on drive() when expiration is disabled', function () {
    $run = parkOverdueRun(SignalOnlyWorkflow::class);

    config()->set('saga-lara-flow.monitor.expiration.enabled', false);

    // The deadline is past, but the kill-switch is off: the run replays and re-parks.
    $driven = app(FlowExecutor::class)->drive($run, RunMode::Queued);

    expect($driven->status)->toBe(FlowStatus::Waiting);
});
