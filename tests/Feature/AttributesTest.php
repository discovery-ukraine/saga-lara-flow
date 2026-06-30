<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\AttributedActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\AttributedWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OptionalAttributedWorkflow;
use Illuminate\Support\Facades\Queue;

it('applies workflow attributes to the run when no explicit options are given', function () {
    Queue::fake();

    $run = SagaFlow::create(AttributedWorkflow::class)->run();

    expect($run->workflow_name)->toBe('orders.checkout')
        ->and($run->workflow_version)->toBe('v2')
        ->and($run->connection)->toBe('redis')
        ->and($run->queue)->toBe('high')
        ->and($run->expires_at)->not->toBeNull();

    expect($run->expires_at->getTimestamp() - now()->getTimestamp())
        ->toBeGreaterThan(3590)
        ->toBeLessThanOrEqual(3600);

    expect($run->tags()->pluck('value', 'key')->all())
        ->toEqualCanonicalizing(['orders' => null, 'team' => 'checkout']);
});

it('lets explicit builder options override attributes', function () {
    Queue::fake();

    $run = SagaFlow::create(AttributedWorkflow::class)
        ->onConnection('sync')
        ->onQueue('low')
        ->version('v9')
        ->expiresAt(now()->addSeconds(10))
        ->withTags(['team' => 'override'])
        ->run();

    expect($run->connection)->toBe('sync')
        ->and($run->queue)->toBe('low')
        ->and($run->workflow_version)->toBe('v9');

    expect($run->expires_at->getTimestamp() - now()->getTimestamp())
        ->toBeLessThanOrEqual(10);

    // Explicit team wins; the attribute-only 'orders' tag is still merged in.
    expect($run->tags()->pluck('value', 'key')->all())
        ->toEqualCanonicalizing(['orders' => null, 'team' => 'override']);
});

it('applies action attributes to the scheduled action', function () {
    $run = SagaFlow::create(AttributedActionWorkflow::class)->runSync();

    $action = $run->actions()->orderBy('sequence')->first();

    expect($action)->not->toBeNull()
        ->and($action->action_name)->toBe('charge-card')
        ->and($action->expires_at)->not->toBeNull();

    expect($action->expires_at->getTimestamp() - $action->created_at->getTimestamp())
        ->toBeGreaterThanOrEqual(110)
        ->toBeLessThanOrEqual(125);
});

it('honours the #[ContinueOnFailure] attribute without an explicit call', function () {
    $run = SagaFlow::create(OptionalAttributedWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed);

    $optional = $run->actions()->where('sequence', 0)->first();

    expect($optional->status)->toBe(ActionStatus::OptionalFailed);
});
