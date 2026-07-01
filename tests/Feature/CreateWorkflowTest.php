<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowCancelled;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotCancelTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\WorkflowClassMissingException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TestWorkflow;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;

/**
 * Persist a Pending run without driving it, for state-machine/handle tests.
 */
function persistPendingRun(array $overrides = []): FlowRun
{
    return app(FlowRepository::class)->create(array_merge([
        'workflow_class' => TestWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ], $overrides));
}

it('creates and completes a flow run via runSync', function () {
    $run = SagaFlow::create(TestWorkflow::class)
        ->withArguments('order-1')
        ->version('v1')
        ->withTags(['tenant' => 't1', 'order' => 'order-1'])
        ->runSync();

    expect($run)->toBeInstanceOf(FlowRun::class)
        ->and($run->exists)->toBeTrue()
        ->and($run->workflow_class)->toBe(TestWorkflow::class)
        ->and($run->workflow_version)->toBe('v1')
        ->and($run->status)->toBe(FlowStatus::Completed)
        ->and($run->arguments)->toBe(['order-1']);

    expect($run->tags()->pluck('value', 'key')->all())
        ->toEqualCanonicalizing(['tenant' => 't1', 'order' => 'order-1']);
});

it('dispatches a workflow job on queued run()', function () {
    Queue::fake();

    $run = SagaFlow::create(TestWorkflow::class)->withArguments('x')->run();

    expect($run->exists)->toBeTrue()
        ->and($run->status)->toBe(FlowStatus::Pending);

    Queue::assertPushed(RunWorkflowJob::class);
});

it('finds an existing run and returns null for a missing one', function () {
    $run = SagaFlow::create(TestWorkflow::class)->runSync();

    expect(SagaFlow::findRun($run->id)?->id)->toBe($run->id)
        ->and(SagaFlow::findRun('01JMISSINGMISSINGMISSING'))->toBeNull();
});

it('throws when resolving a missing flow handle', function () {
    SagaFlow::loadFlow('01JMISSINGMISSINGMISSING');
})->throws(FlowNotFoundException::class);

it('throws when creating with a missing workflow class', function () {
    SagaFlow::create('App\\Workflows\\DoesNotExist');
})->throws(WorkflowClassMissingException::class);

it('cancels a non-terminal run through the handle and records the reason', function () {
    Event::fake([FlowCancelled::class]);

    $run = persistPendingRun();

    SagaFlow::loadFlow($run->id)->cancel('user requested');

    expect($run->fresh()->status)->toBe(FlowStatus::Cancelled);

    $event = FlowEvent::query()
        ->where('flow_run_id', $run->id)
        ->where('type', FlowEventType::FlowCancelled)
        ->first();

    expect($event)->not->toBeNull()
        ->and($event->payload)->toBe(['reason' => 'user requested']);

    Event::assertDispatched(
        FlowCancelled::class,
        fn (FlowCancelled $e) => $e->flowRun->is($run) && $e->reason === 'user requested',
    );
});

it('records a null reason when cancelled without one', function () {
    $run = persistPendingRun();

    SagaFlow::loadFlow($run->id)->cancel();

    $event = FlowEvent::query()
        ->where('flow_run_id', $run->id)
        ->where('type', FlowEventType::FlowCancelled)
        ->first();

    expect($event)->not->toBeNull()
        ->and($event->payload)->toBeNull();
});

it('rejects cancelling a terminal run', function () {
    $run = persistPendingRun();
    $run->markCancelled();

    SagaFlow::loadFlow($run->id)->cancel();
})->throws(CannotCancelTerminalFlowException::class);

it('records timestamps along a valid lifecycle', function () {
    $run = persistPendingRun();

    $run->markRunning();
    expect($run->status)->toBe(FlowStatus::Running)
        ->and($run->started_at)->not->toBeNull();

    $run->markCompleted(['ok' => true]);
    expect($run->fresh()->status)->toBe(FlowStatus::Completed)
        ->and($run->finished_at)->not->toBeNull()
        ->and($run->result)->toBe(['ok' => true]);
});

it('throws on an invalid lifecycle transition', function () {
    $run = persistPendingRun();
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
