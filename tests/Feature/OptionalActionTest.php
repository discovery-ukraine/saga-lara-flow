<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\OptionalActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalFallbackWorkflow;
use Illuminate\Support\Facades\Event;

it('does not fail the flow when an optional action fails (sync)', function () {
    $run = SagaFlow::create(OptionalFallbackWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['optional'] ?? 'unset')->toBe('skipped')
        ->and($run->result['after'] ?? null)->toBe(['label' => 'after']);

    $actions = $run->actions()->orderBy('sequence')->get();

    expect($actions)->toHaveCount(2)
        ->and($actions[0]->status)->toBe(ActionStatus::OptionalFailed)
        ->and($actions[0]->continue_on_failure)->toBeTrue()
        ->and($actions[0]->exception['message'] ?? null)->toBe('boom')
        ->and($actions[1]->status)->toBe(ActionStatus::Completed);
});

it('records an optional_failed event and dispatches OptionalActionFailed', function () {
    Event::fake([OptionalActionFailed::class]);

    $run = SagaFlow::create(OptionalFallbackWorkflow::class)->runSync();

    $types = $run->events()->pluck('type')->all();

    expect($types)->toContain(FlowEventType::ActionOptionalFailed);

    Event::assertDispatched(OptionalActionFailed::class);
});

it('respects $tries then lands OptionalFailed (queued)', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(OptionalFallbackWorkflow::class)->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['optional'] ?? 'unset')->toBe('skipped');

    $optional = $final->actions()->orderBy('sequence')->first();

    expect($optional->status)->toBe(ActionStatus::OptionalFailed);
});

it('reaches an identical final state in sync and queued modes', function () {
    $sync = SagaFlow::create(OptionalFallbackWorkflow::class)->runSync();

    useDatabaseQueue();
    $queued = SagaFlow::create(OptionalFallbackWorkflow::class)->run();
    drainQueue();

    expect(runStateSnapshot($queued->id))->toEqual(runStateSnapshot($sync->id));
});
