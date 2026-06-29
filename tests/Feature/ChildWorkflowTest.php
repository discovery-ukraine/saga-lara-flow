<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ChildEchoWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\GrandparentWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentAbandonWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentAwaitCancellableChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentAwaitWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentCancelPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentChildContinueWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentChildFailsWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentFailPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WaitingChildWorkflow;

beforeEach(function () {
    CompensationLog::reset();
    // Run dispatched jobs inline on the default sync queue driver.
    config()->set('saga-lara-flow.queue.after_commit', false);
});

it('starts a child, awaits it, and returns its result (sync)', function () {
    $run = SagaFlow::create(ParentAwaitWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['child'] ?? null)->toBe(['label' => 'x']);

    $link = $run->children()->first();

    expect($link)->not->toBeNull()
        ->and($link->status)->toBe(ChildStatus::Completed)
        ->and($link->child_workflow_class)->toBe(ChildEchoWorkflow::class)
        ->and($link->child->status)->toBe(FlowStatus::Completed)
        ->and($link->child->parent_id)->toBe($run->id);
});

it('awaits a child over the real queue and reaches the same final state', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(ParentAwaitWorkflow::class)->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['child'] ?? null)->toBe(['label' => 'x']);

    $link = $final->children()->first();

    expect($link->status)->toBe(ChildStatus::Completed)
        ->and($link->child->status)->toBe(FlowStatus::Completed);
});

it('propagates a child failure to the parent and compensates both (sync)', function () {
    $run = SagaFlow::create(ParentChildFailsWorkflow::class)->runSync();

    // Child rolls itself back first, then the failure surfaces and the parent rolls back.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:child-a', 'undo:parent']);

    expect($run->children()->first()->child->status)->toBe(FlowStatus::Failed);
});

it('isolates a child failure when continueParentOnFailure is set (sync)', function () {
    $run = SagaFlow::create(ParentChildContinueWorkflow::class)->runSync();

    // Child rolled itself back; child()->run() returned null; the parent continued.
    expect($run->status)->toBe(FlowStatus::Completed)
        ->and(CompensationLog::all())->toBe(['undo:child-a'])
        ->and($run->result['after'] ?? null)->toBe('after')
        ->and($run->result)->toHaveKey('childResult')
        ->and($run->result['childResult'])->toBeNull();

    expect($run->children()->first()->child->status)->toBe(FlowStatus::Failed);
});

it('cancels an active child without compensation when the parent is cancelled', function () {
    $run = SagaFlow::create(ParentCancelPolicyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $cancelled = SagaFlow::loadFlow($run->id)->cancel();

    expect($cancelled->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe([]);

    expect($run->children()->first()->child->status)->toBe(FlowStatus::Cancelled);
});

it('cancels an active child with compensation when the parent is compensated', function () {
    $run = SagaFlow::create(ParentCancelPolicyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $compensated = SagaFlow::loadFlow($run->id)->compensate();

    // Parent rolls back first (undo:parent), then the cascaded child (undo:child).
    expect($compensated->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child']);

    expect($run->children()->first()->child->status)->toBe(FlowStatus::Cancelled);
});

it('fails an active child with compensation under the Fail close policy', function () {
    $run = SagaFlow::create(ParentFailPolicyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $compensated = SagaFlow::loadFlow($run->id)->compensate();

    expect($compensated->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:child']);

    $child = $run->children()->first()->child;

    expect($child->status)->toBe(FlowStatus::Failed)
        ->and($child->exception['message'] ?? null)->toContain('closed under the Fail policy');
});

it('leaves an active child running under the Abandon close policy', function () {
    $run = SagaFlow::create(ParentAbandonWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    SagaFlow::loadFlow($run->id)->cancel();

    expect($run->children()->first()->child->status)->toBe(FlowStatus::Waiting);
});

it('surfaces an externally cancelled child as a business error on the parent', function () {
    $run = SagaFlow::create(ParentAwaitCancellableChildWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $childId = $run->children()->first()->child_flow_run_id;

    // Cancelling the child wakes the waiting parent (ResumeWorkflowJob runs inline).
    SagaFlow::loadFlow($childId)->cancel();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Failed)
        ->and($final->exception['message'] ?? null)->toContain('was cancelled');
});

it('cancels a nested tree down to the grandchild (Cancel policy)', function () {
    $run = SagaFlow::create(GrandparentWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    SagaFlow::loadFlow($run->id)->cancel();

    $parentRun = $run->children()->first()->child;

    expect($parentRun->status)->toBe(FlowStatus::Cancelled);

    expect($parentRun->children()->first()->child->status)->toBe(FlowStatus::Cancelled);
});

it('raises a history-contract mismatch when the recorded child class differs', function () {
    $run = app(FlowRepository::class)->create([
        'workflow_class' => ParentAwaitWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    $child = app(FlowRepository::class)->create([
        'workflow_class' => WaitingChildWorkflow::class,
        'status' => FlowStatus::Running,
        'arguments' => [],
        'parent_id' => $run->id,
    ]);

    // Parent will request ChildEchoWorkflow at sequence 0, but a different class is recorded.
    FlowChild::create([
        'parent_flow_run_id' => $run->id,
        'child_flow_run_id' => $child->id,
        'sequence' => 0,
        'child_workflow_class' => WaitingChildWorkflow::class,
        'close_policy' => ChildClosePolicy::Abandon,
        'status' => ChildStatus::Running,
    ]);

    $driven = app(FlowExecutor::class)->drive($run, RunMode::Sync);

    expect($driven->status)->toBe(FlowStatus::Failed);
});
