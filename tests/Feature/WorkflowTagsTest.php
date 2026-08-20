<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TaggingReplayWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TaggingWorkflow;

it('attaches several tags at once from inside the workflow', function () {
    $run = SagaFlow::create(TaggingWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->tags()->pluck('value', 'key')->all())
        ->toEqualCanonicalizing([
            // 'priority' was re-tagged by a later tags() call — last write wins.
            'priority' => 'high',
            // An int value is cast to string on the way in.
            'attempt' => '1',
            // A null value records a tag with no value.
            'orders' => null,
            // Set through the chained tag() call.
            'tenant' => 'acme',
        ]);
});

it('never duplicates a repeated tag key', function () {
    $run = SagaFlow::create(TaggingWorkflow::class)->runSync();

    // 'priority' is written twice by the workflow, but updateOrCreate matches on
    // (flow_run_id, key), so it stays a single row.
    expect($run->tags()->where('key', 'priority')->count())->toBe(1)
        ->and($run->tags()->count())->toBe(4);
});

it('keeps bulk tagging idempotent across replays', function () {
    useDatabaseQueue();

    // The action suspends the run, so handle() — and the tags() call before it —
    // is replayed on the resume pass.
    $run = SagaFlow::create(TaggingReplayWorkflow::class)->run();

    drainQueue();

    $run = SagaFlow::findRun($run->id);

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->tags()->count())->toBe(2)
        ->and($run->tags()->pluck('value', 'key')->all())
        ->toEqualCanonicalizing(['stage' => 'done', 'tenant' => 'acme']);
});

it('returns the workflow instance so tag calls chain', function () {
    $run = SagaFlow::create(TaggingWorkflow::class)->runSync();

    // The fixture only completes if tags() and tag() are chainable.
    expect($run->status)->toBe(FlowStatus::Completed);
});
