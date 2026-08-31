<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WaitingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * A child that already reached a terminal state is history, not a frontier. The
 * collecting replay used to stop at anything other than Completed, so every
 * compensation a parent registered after a failed or cancelled child was absent from
 * the rollback while compensate() reported a complete unwind.
 *
 * Resolving those children the way the ordinary replay does turns two of them into
 * throws, and both are recorded history replaying — so both have to end the collection
 * rather than escape it. Without that, a run the sweep expires fine today becomes one
 * it can never plan, which is the failure #52 was closed for.
 */
beforeEach(function (): void {
    CompensationLog::reset();
});

final class ContinuePastFailedChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(FailingChildWorkflow::class)->continueParentOnFailure()->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->awaitSignal('go');
    }
}

final class StopAtFailedChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(FailingChildWorkflow::class)->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->awaitSignal('go');
    }
}

final class AwaitCancellableChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(WaitingChildWorkflow::class)->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
    }
}

final class ChildAfterSignalWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->awaitSignal('go');
        $this->child(FailingChildWorkflow::class)->continueParentOnFailure()->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
    }
}

/**
 * The window a parent sits in between its child finalizing and the resume job that
 * tells it so: the parent is still Waiting, the child is already terminal, and any
 * plan made now has to read the child from its row rather than wait on it.
 */
function parentWaitingOnTerminalChild(string $workflow, FlowStatus $childLands): FlowRun
{
    $run = SagaFlow::create($workflow)->expiresAt(now()->addMinutes(5))->run();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    $child->newQuery()->toBase()->where('id', $child->id)->update(['status' => $childLands->value]);
    $parent->newQuery()->toBase()->where('id', $parent->id)->update(['status' => FlowStatus::Waiting->value]);
    DB::connection('testing')->table('jobs')->delete();

    return $parent->fresh();
}

function sequencesOf(FlowRun $parent): array
{
    return array_map(
        fn ($entry) => $entry->sequence,
        app(FlowExecutor::class)->collectCompensations($parent),
    );
}

it('plans the steps a parent took after a child it was told to survive', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(ContinuePastFailedChildWorkflow::class)->run();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);
    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and($parent->actions()->where('status', 'completed')->pluck('sequence')->all())->toBe([0, 2]);

    // The child rolled itself back on its own way out; this is the parent's rollback.
    CompensationLog::reset();

    expect(sequencesOf($parent))->toBe([0, 2]);

    $rolled = SagaFlow::loadFlow($parent->id)->compensate();

    expect($rolled->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('ends a plan where a strict child ended the parent, without escaping', function () {
    useDatabaseQueue();

    $parent = parentWaitingOnTerminalChild(StopAtFailedChildWorkflow::class, FlowStatus::Failed);

    // The parent never ran a step after the child, so the whole of what it did is 'a'.
    expect(sequencesOf($parent))->toBe([0]);
});

it('ends a plan where a cancelled child ended the parent, without escaping', function () {
    useDatabaseQueue();

    $parent = parentWaitingOnTerminalChild(AwaitCancellableChildWorkflow::class, FlowStatus::Cancelled);

    expect(sequencesOf($parent))->toBe([0]);
});

it('keeps expiring a run whose child failed under it', function () {
    useDatabaseQueue();
    logToFile($log = sys_get_temp_dir().'/terminal-child-'.uniqid().'.log');

    $parent = parentWaitingOnTerminalChild(StopAtFailedChildWorkflow::class, FlowStatus::Failed);
    $this->travel(600)->seconds();

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1)
        ->and($parent->fresh()->status)->toBe(FlowStatus::Cancelling)
        // A run the sweep can plan is never held off, so nothing queues behind it.
        ->and($parent->fresh()->expiry_attempts)->toBe(0)
        ->and(substr_count((string) @file_get_contents($log), 'expiry_failed'))->toBe(0);
});

it('stops at a child this run never started, and starts none to find out', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(ChildAfterSignalWorkflow::class)->run();
    drainQueue();

    // The R2 shape: the wait was consumed, the resume that would have reached the
    // child was lost. The replay now gets past the signal with no child on record.
    SagaFlow::loadFlow($run->id)->signal('go');
    DB::connection('testing')->table((new FlowSignal)->getTable())
        ->update(['status' => SignalStatus::Consumed->value]);
    DB::connection('testing')->table('jobs')->delete();

    $parent = FlowRun::query()->findOrFail($run->id);
    $runsBefore = FlowRun::query()->count();

    expect(sequencesOf($parent))->toBe([0])
        ->and(FlowRun::query()->count())->toBe($runsBefore)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

it('resolves a terminal child without writing anything down', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(ContinuePastFailedChildWorkflow::class)->run();
    drainQueue();
    $parent = FlowRun::query()->findOrFail($run->id);

    $writes = 0;
    DB::listen(function ($query) use (&$writes): void {
        if (! str_starts_with(strtolower(ltrim($query->sql)), 'select')) {
            $writes++;
        }
    });

    expect(sequencesOf($parent))->toBe([0, 2])
        ->and($writes)->toBe(0);
});
