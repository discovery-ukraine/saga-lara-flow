<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailingUndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * A child carries a compensation of its own: it joins the parent's stack when the child
 * resolves Completed, the moment a step's does. A child that failed or expired has run its own
 * rollback and joins nothing. The close policy only reaches a child still in flight, so a
 * completed child is undone by this compensation alone, and once.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    ParentOfCompensatedChildWorkflow::$policy = ChildClosePolicy::Abandon;
    ParentOfUncompensatedChildWorkflow::$policy = ChildClosePolicy::Abandon;
    ChildCompensationPolicyWorkflow::$onFailure = null;
});

final class CompletingCompensatedChildWorkflow extends Workflow
{
    public function handle(): string
    {
        $this->action(MakeValueAction::class, 'c')->compensateWith(UndoAction::class, 'c')->run();

        return 'child-result';
    }
}

final class ParentOfCompensatedChildWorkflow extends Workflow
{
    public static ChildClosePolicy $policy = ChildClosePolicy::Abandon;

    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();

        $result = $this->child(CompletingCompensatedChildWorkflow::class)
            ->closePolicy(self::$policy)
            ->compensateWith(UndoAction::class, 'child')
            ->run();

        // The child's result still comes back through run(): b is undone under its name.
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, $result)->run();
        $this->action(ThrowingAction::class)->run();
    }
}

final class ParentOfUncompensatedChildWorkflow extends Workflow
{
    public static ChildClosePolicy $policy = ChildClosePolicy::Abandon;

    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(CompletingCompensatedChildWorkflow::class)->closePolicy(self::$policy)->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

final class ParentSurvivingCompensatedChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(FailingChildWorkflow::class)
            ->continueParentOnFailure()
            ->compensateWith(UndoAction::class, 'child')
            ->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

final class ParentParkedPastCompensatedChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(CompletingCompensatedChildWorkflow::class)
            ->continueParentOnFailure()
            ->compensateWith(UndoAction::class, 'child')
            ->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->awaitSignal('go');
    }
}

final class ChildCompensationPolicyWorkflow extends Workflow
{
    public static ?CompensationFailurePolicy $onFailure = null;

    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();

        $child = $this->child(CompletingCompensatedChildWorkflow::class)
            ->compensateWith(FailingUndoAction::class, 'child');

        if (self::$onFailure !== null) {
            $child->onCompensationFailure(self::$onFailure);
        }

        $child->run();

        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->action(ThrowingAction::class)->run();
    }
}

final class ParentOfClosureCompensatedChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $label = 'closure:child';

        $this->child(CompletingCompensatedChildWorkflow::class)
            ->compensateWith(fn () => CompensationLog::record($label))
            ->run();
        $this->action(ThrowingAction::class)->run();
    }
}

function runCompensatedParent(string $workflow, string $mode): FlowRun
{
    if ($mode === 'sync') {
        return FlowRun::query()->findOrFail(SagaFlow::create($workflow)->runSync()->id);
    }

    useDatabaseQueue();

    $run = SagaFlow::create($workflow)->run();
    drainQueue();

    return FlowRun::query()->findOrFail($run->id);
}

it('rolls a completed child back with the compensation the parent gave it', function (
    string $mode,
    ChildClosePolicy $policy,
): void {
    ParentOfCompensatedChildWorkflow::$policy = $policy;

    $parent = runCompensatedParent(ParentOfCompensatedChildWorkflow::class, $mode);
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($child->status)->toBe(FlowStatus::Completed)
        // Stack order, unwound last in, first out: the child sits between a and b.
        ->and(CompensationLog::all())->toBe(['undo:child-result', 'undo:child', 'undo:a']);

    $rows = CompensationRun::query()->where('flow_run_id', $parent->id)->orderBy('sequence')->get();

    // A child has no step row, so its compensation row points at none.
    expect($rows->pluck('arguments')->all())->toBe([['child-result'], ['child'], ['a']])
        ->and($rows->pluck('action_run_id')->map(fn ($id) => $id !== null)->all())->toBe([true, false, true])
        ->and(CompensationRun::query()->where('flow_run_id', $child->id)->count())->toBe(0);
})->with([['sync'], ['queued']])->with(ChildClosePolicy::cases());

it('leaves a completed child alone whatever its close policy', function (
    string $mode,
    ChildClosePolicy $policy,
): void {
    ParentOfUncompensatedChildWorkflow::$policy = $policy;

    $parent = runCompensatedParent(ParentOfUncompensatedChildWorkflow::class, $mode);
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    // The close policy acts on a child still in flight; one that completed stays applied.
    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($child->status)->toBe(FlowStatus::Completed)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
})->with([['sync'], ['queued']])->with(ChildClosePolicy::cases());

it('registers nothing for a child that failed under a parent told to survive it', function (
    string $mode,
): void {
    $parent = runCompensatedParent(ParentSurvivingCompensatedChildWorkflow::class, $mode);
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    // The child undid its own step on its way out; the parent has nothing to add for it.
    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($child->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:child-a', 'undo:b', 'undo:a']);
})->with([['sync'], ['queued']]);

it('plans a child compensation only over a child that completed', function (
    ?FlowStatus $childLands,
    array $planned,
    array $undone,
): void {
    useDatabaseQueue();

    $run = SagaFlow::create(ParentParkedPastCompensatedChildWorkflow::class)->run();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and($child->status)->toBe(FlowStatus::Completed);

    if ($childLands !== null) {
        $child->newQuery()->toBase()->where('id', $child->id)->update(['status' => $childLands->value]);
    }

    DB::connection('testing')->table('jobs')->delete();

    $sequences = array_map(
        fn ($entry) => $entry->sequence,
        app(FlowExecutor::class)->collectCompensations($parent),
    );

    expect($sequences)->toBe($planned);

    $rolled = SagaFlow::loadFlow($parent->id)->compensate();

    expect($rolled->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe($undone);
})->with([
    'completed' => [null, [0, 1, 2], ['undo:b', 'undo:child', 'undo:a']],
    'failed' => [FlowStatus::Failed, [0, 2], ['undo:b', 'undo:a']],
    'expired' => [FlowStatus::Expired, [0, 2], ['undo:b', 'undo:a']],
]);

it('lets a child compensation carry on past its own failure', function (): void {
    config()->set('saga-lara-flow.sagas.default_compensation_failure_policy', CompensationFailurePolicy::Stop);
    ChildCompensationPolicyWorkflow::$onFailure = CompensationFailurePolicy::Continue;

    $run = SagaFlow::create(ChildCompensationPolicyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('lets a child compensation stop the rollback at its own failure', function (): void {
    config()->set('saga-lara-flow.sagas.default_compensation_failure_policy', CompensationFailurePolicy::Continue);
    ChildCompensationPolicyWorkflow::$onFailure = CompensationFailurePolicy::Stop;

    $run = SagaFlow::create(ChildCompensationPolicyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:b']);
});

it('falls back to the configured policy for a child compensation', function (): void {
    config()->set('saga-lara-flow.sagas.default_compensation_failure_policy', CompensationFailurePolicy::Continue);

    $run = SagaFlow::create(ChildCompensationPolicyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('runs a closure compensation for a child', function (string $mode): void {
    $parent = runCompensatedParent(ParentOfClosureCompensatedChildWorkflow::class, $mode);

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['closure:child']);
})->with([['sync'], ['queued']]);
