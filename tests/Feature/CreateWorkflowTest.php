<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotCancelTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\WorkflowClassMissingException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TestWorkflow;

it('creates a flow run via runSync', function () {
    $run = SagaFlow::create(TestWorkflow::class)
        ->withArguments('order-1')
        ->version('v1')
        ->withTags(['tenant' => 't1', 'order' => 'order-1'])
        ->runSync();

    expect($run)->toBeInstanceOf(FlowRun::class)
        ->and($run->exists)->toBeTrue()
        ->and($run->workflow_class)->toBe(TestWorkflow::class)
        ->and($run->workflow_version)->toBe('v1')
        ->and($run->status)->toBe(FlowStatus::Pending)
        ->and($run->arguments)->toBe(['order-1']);

    expect($run->tags()->pluck('value', 'key')->all())
        ->toEqualCanonicalizing(['tenant' => 't1', 'order' => 'order-1']);
});

it('creates a flow run via queued run()', function () {
    $run = SagaFlow::create(TestWorkflow::class)->withArguments('x')->run();

    expect($run->exists)->toBeTrue()
        ->and($run->status)->toBe(FlowStatus::Pending);
});

it('finds an existing run and returns null for a missing one', function () {
    $run = SagaFlow::create(TestWorkflow::class)->runSync();

    expect(SagaFlow::findRun($run->id)?->id)->toBe($run->id)
        ->and(SagaFlow::findRun('01JMISSINGMISSINGMISSING'))->toBeNull();
});

it('throws when resolving a missing flow handle', function () {
    SagaFlow::run('01JMISSINGMISSINGMISSING');
})->throws(FlowNotFoundException::class);

it('throws when creating with a missing workflow class', function () {
    SagaFlow::create('App\\Workflows\\DoesNotExist');
})->throws(WorkflowClassMissingException::class);

it('cancels a non-terminal run through the handle', function () {
    $run = SagaFlow::create(TestWorkflow::class)->runSync();

    SagaFlow::run($run->id)->cancel('user requested');

    expect($run->fresh()->status)->toBe(FlowStatus::Cancelled);
});

it('rejects cancelling a terminal run', function () {
    $run = SagaFlow::create(TestWorkflow::class)->runSync();
    $run->markCancelled();

    SagaFlow::run($run->id)->cancel();
})->throws(CannotCancelTerminalFlowException::class);

it('records timestamps along a valid lifecycle', function () {
    $run = SagaFlow::create(TestWorkflow::class)->runSync();

    $run->markRunning();
    expect($run->status)->toBe(FlowStatus::Running)
        ->and($run->started_at)->not->toBeNull();

    $run->markCompleted(['ok' => true]);
    expect($run->fresh()->status)->toBe(FlowStatus::Completed)
        ->and($run->finished_at)->not->toBeNull()
        ->and($run->result)->toBe(['ok' => true]);
});

it('throws on an invalid lifecycle transition', function () {
    $run = SagaFlow::create(TestWorkflow::class)->runSync();
    $run->markRunning();
    $run->markCompleted();

    $run->markRunning();
})->throws(InvalidTransitionException::class);

it('honours configurable connection and table prefix', function () {
    config()->set('saga-lara-flow.database.table_prefix', 'custom_');
    config()->set('saga-lara-flow.database.connection', 'tenant-db');

    $run = new FlowRun;

    expect($run->getTable())->toBe('custom_flow_runs')
        ->and($run->getConnectionName())->toBe('tenant-db');
});
