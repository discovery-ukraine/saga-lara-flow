<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalTimeoutCaughtWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalTimeoutWorkflow;

beforeEach(fn () => CompensationLog::reset());

it('times a stuck signal out and fails the flow when the timeout is uncaught', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalTimeoutWorkflow::class)->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);

    expect($run->status)->toBe(FlowStatus::Waiting);

    $marker = $run->signals()->first();

    expect($marker->status)->toBe(SignalStatus::Waiting)
        ->and($marker->timeout_at)->not->toBeNull();

    $report = app(FlowMonitor::class)->sweep();

    expect($report['signals'])->toBe(1);

    // Resume + compensation run as queued jobs.
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($final->exception['class'] ?? null)->toBe(AwaitSignalTimeoutException::class)
        ->and(CompensationLog::all())->toContain('undo:a')
        ->and($final->signals()->first()->status)->toBe(SignalStatus::TimedOut)
        ->and($final->events()->where('type', FlowEventType::SignalTimedOut->value)->count())->toBe(1);
});

it('lets the workflow catch a signal timeout and carry on', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalTimeoutCaughtWorkflow::class)->run();
    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);

    app(FlowMonitor::class)->sweep();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result)->toBe(['outcome' => 'timed-out'])
        ->and($final->signals()->first()->status)->toBe(SignalStatus::TimedOut);
});
