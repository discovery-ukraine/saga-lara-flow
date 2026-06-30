<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;

it('applies the configured default run deadline when none is set', function () {
    config()->set('saga-lara-flow.monitor.expiration.defaults.run', 3600);
    useDatabaseQueue();

    $run = SagaFlow::create(SignalOnlyWorkflow::class)->run();

    expect($run->expires_at)->not->toBeNull()
        ->and($run->expires_at->greaterThan(now()->addMinutes(30)))->toBeTrue();
});

it('applies the configured default action deadline when none is set', function () {
    config()->set('saga-lara-flow.monitor.expiration.defaults.action', 600);
    useDatabaseQueue();

    $run = SagaFlow::create(OneActionWorkflow::class)->run();

    // Drive once to schedule the action (Pending) without running its job.
    $driven = app(FlowExecutor::class)->drive(SagaFlow::findRun($run->id), RunMode::Queued);

    expect($driven->actions()->first()->expires_at)->not->toBeNull();
});

it('applies the configured default signal timeout when none is set', function () {
    config()->set('saga-lara-flow.monitor.expiration.defaults.signal', 600);
    useDatabaseQueue();

    $run = SagaFlow::create(SignalOnlyWorkflow::class)->run();
    drainQueue();

    expect(SagaFlow::findRun($run->id)->signals()->first()->timeout_at)->not->toBeNull();
});

it('lets an explicit deadline win over the configured default', function () {
    config()->set('saga-lara-flow.monitor.expiration.defaults.run', 3600);
    useDatabaseQueue();

    $run = SagaFlow::create(SignalOnlyWorkflow::class)
        ->expiresAt(now()->addSeconds(30))
        ->run();

    // ~30s in, not the 3600s default.
    expect($run->expires_at->lessThan(now()->addMinutes(5)))->toBeTrue();
});

it('leaves deadlines null when no default is configured', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(SignalOnlyWorkflow::class)->run();

    expect($run->expires_at)->toBeNull();
});
