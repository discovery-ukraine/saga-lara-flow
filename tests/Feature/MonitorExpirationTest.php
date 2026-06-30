<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowExpired;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ExpiringSagaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Event;

beforeEach(fn () => CompensationLog::reset());

/**
 * Park a run in Waiting, then mark its deadline as already past.
 */
function expireWaitingRun(string $workflowClass): string
{
    useDatabaseQueue();

    $run = SagaFlow::create($workflowClass)->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);

    expect($run->status)->toBe(FlowStatus::Waiting);

    $run->expires_at = now()->subMinute();
    $run->save();

    return $run->id;
}

it('expires a stuck run with no compensations directly', function () {
    Event::fake([FlowExpired::class]);

    $id = expireWaitingRun(SignalOnlyWorkflow::class);

    $report = app(FlowMonitor::class)->sweep();

    expect($report['runs'])->toBe(1);

    $final = SagaFlow::findRun($id);

    expect($final->status)->toBe(FlowStatus::Expired)
        ->and($final->events()->where('type', FlowEventType::FlowExpired->value)->count())->toBe(1);

    Event::assertDispatched(FlowExpired::class, 1);
});

it('rolls back a stuck run before landing it in Expired', function () {
    $id = expireWaitingRun(ExpiringSagaWorkflow::class);

    $report = app(FlowMonitor::class)->sweep();

    expect($report['runs'])->toBe(1);

    // The rollback runs as queued compensation jobs; drain them to finalize.
    drainQueue();

    $final = SagaFlow::findRun($id);

    expect($final->status)->toBe(FlowStatus::Expired)
        ->and(CompensationLog::all())->toContain('undo:a')
        ->and($final->events()->where('type', FlowEventType::FlowExpired->value)->count())->toBe(1);
});

it('re-runs the monitor idempotently', function () {
    $id = expireWaitingRun(SignalOnlyWorkflow::class);

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1);

    expect(app(FlowMonitor::class)->sweep())
        ->toBe(['runs' => 0, 'signals' => 0, 'actions' => 0]);

    expect(SagaFlow::findRun($id)->status)->toBe(FlowStatus::Expired);
});

it('expires runs via the saga-flow:monitor command', function () {
    $id = expireWaitingRun(SignalOnlyWorkflow::class);

    Artisan::call('saga-flow:monitor');

    expect(SagaFlow::findRun($id)->status)->toBe(FlowStatus::Expired);
});
