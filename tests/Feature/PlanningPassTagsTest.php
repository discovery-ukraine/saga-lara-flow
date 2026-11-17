<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TagBranchingWorkflow;

/**
 * Planning a rollback replays handle() only to learn what has to be undone, yet a
 * workflow's tag() calls still write on that replay, as on every other. Skipping them
 * there looks like the obvious way to keep the plan from writing, and it truncates the
 * rollback of a workflow that reads its own tag back: the planning replay finds the
 * later value, takes the other branch, and the step behind it is never undone.
 */
beforeEach(function (): void {
    CompensationLog::reset();
});

it('plans the rollback the ordinary replay walked, for a workflow that reads its own tag', function () {
    $run = SagaFlow::create(TagBranchingWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting)
        ->and($run->actions()->count())->toBe(2)
        ->and($run->tags()->pluck('value', 'key')->all())->toBe(['phase' => 'after-b']);

    $stack = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($run->id));

    expect(array_map(fn ($entry): int => $entry->sequence, $stack))->toBe([0, 1]);

    $compensated = SagaFlow::loadFlow($run->id)->compensate();

    expect($compensated->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});
