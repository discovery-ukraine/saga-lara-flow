<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalCancellingFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ParentClosedException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Support\Facades\DB;

/*
 * A parent that begins rolling back, or lands straight from a live state, moves its live
 * Cancel/Fail children into Cancelling with it, so none of them can finish before its close
 * reaches it. The scenes signal the child in the gap between the parent's move and the close.
 */

beforeEach(function (): void {
    CompensationLog::reset();
});

final class FencedChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'child')->compensateWith(UndoAction::class, 'child')->run();
        $this->awaitSignal('child.go');
    }
}

final class FencingParentWorkflow extends Workflow
{
    public function handle(string $policy = 'cancel', bool $seam = false): void
    {
        $this->action(MakeValueAction::class, 'parent')->compensateWith(UndoAction::class, 'parent')->run();

        $child = $this->child(FencedChildWorkflow::class)->closePolicy(ChildClosePolicy::from($policy));

        if ($seam) {
            $child->compensateWith(UndoAction::class, 'child-seam');
        }

        $child->run();
    }
}

final class FencedSelfFailingChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'c')->compensateWith(UndoAction::class, 'c')->run();
        $this->awaitSignal('c.go');
        $this->action(ThrowingAction::class)->run();
    }
}

final class FencingParentOfSelfFailingWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'parent')->compensateWith(UndoAction::class, 'parent')->run();
        $this->child(FencedSelfFailingChildWorkflow::class)->closePolicy(ChildClosePolicy::Cancel)->run();
    }
}

final class FencedMiddleWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'middle')->compensateWith(UndoAction::class, 'middle')->run();
        $this->child(FencedChildWorkflow::class)->closePolicy(ChildClosePolicy::Cancel)->run();
    }
}

final class FencingRootWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'root')->compensateWith(UndoAction::class, 'root')->run();
        $this->child(FencedMiddleWorkflow::class)->closePolicy(ChildClosePolicy::Cancel)->run();
    }
}

/**
 * @return array{0: FlowRun, 1: FlowRun} the parent waiting on its child, the child on child.go
 */
function parentWaitingOnChild(string $policy = 'cancel', bool $seam = false, ?DateTimeInterface $expiresAt = null): array
{
    useDatabaseQueue();

    $builder = SagaFlow::create(FencingParentWorkflow::class)->withArguments($policy, $seam);

    if ($expiresAt !== null) {
        $builder->expiresAt($expiresAt);
    }

    $parent = $builder->run();
    drainQueue();

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($child->status)->toBe(FlowStatus::Waiting);

    return [$parent, $child];
}

function goOnInTheGap(FlowRun $child): void
{
    expect(fn () => SagaFlow::loadFlow($child->id)->signal('child.go'))
        ->toThrow(CannotSignalCancellingFlowException::class);
}

it('rolls back a child told to go on after its parent began compensating', function (): void {
    foreach ([false, true] as $seam) {
        CompensationLog::reset();
        [$parent, $child] = parentWaitingOnChild('cancel', $seam);

        SagaFlow::loadFlow($parent->id)->compensate();
        $close = loseJobs('CancelChildWorkflowJob');

        goOnInTheGap($child);
        drainQueue();

        bringJobsBack($close);
        drainQueue();

        // The child never completed, so the parent's own compensation for it is not run.
        expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
            ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child'])
            ->and(compensationTrail($child->id))->toBe(['child:completed']);
    }
});

it('rolls back a child told to go on after its parent began expiring', function (): void {
    [$parent, $child] = parentWaitingOnChild('cancel', false, now()->addSeconds(30));

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    goOnInTheGap($child);
    drainQueue();

    expect(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Expired)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child']);
});

it('closes a child told to go on after its parent was cancelled, as its policy says', function (): void {
    [$parent, $child] = parentWaitingOnChild('fail');

    SagaFlow::loadFlow($parent->id)->cancel();
    $close = loseJobs('CancelChildWorkflowJob');

    goOnInTheGap($child);

    bringJobsBack($close);
    drainQueue();

    $failed = SagaFlow::findRun($child->id);

    expect($failed->status)->toBe(FlowStatus::Failed)
        ->and($failed->exception['class'])->toBe(ParentClosedException::class)
        ->and(CompensationLog::all())->toBe(['undo:child']);

    CompensationLog::reset();
    [$parent, $child] = parentWaitingOnChild('cancel');

    SagaFlow::loadFlow($parent->id)->cancel();
    $close = loseJobs('CancelChildWorkflowJob');

    goOnInTheGap($child);

    bringJobsBack($close);
    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe([]);
});

it('fences a child to roll back when its parent fails straight from a live state', function (): void {
    [$parent, $child] = parentWaitingOnChild('cancel');

    app(StateMachine::class)->transition(SagaFlow::findRun($parent->id), FlowStatus::Failed);

    $fenced = SagaFlow::findRun($child->id);

    expect($fenced->status)->toBe(FlowStatus::Cancelling)
        ->and($fenced->rollback_target)->toBe(FlowStatus::Cancelled)
        ->and($fenced->rollback_compensates)->toBeTrue()
        ->and($fenced->rollback_token)->toBe($child->id);
});

it('fences a grandchild with the child its root fenced', function (): void {
    useDatabaseQueue();

    $root = SagaFlow::create(FencingRootWorkflow::class)->run();
    drainQueue();

    $middle = FlowRun::query()->where('parent_id', $root->id)->firstOrFail();
    $leaf = FlowRun::query()->where('parent_id', $middle->id)->firstOrFail();

    SagaFlow::loadFlow($root->id)->compensate();
    $close = loseJobs('CancelChildWorkflowJob');

    goOnInTheGap($leaf);
    drainQueue();

    bringJobsBack($close);
    drainQueue();

    expect(SagaFlow::findRun($middle->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(SagaFlow::findRun($leaf->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:root', 'undo:middle', 'undo:child']);
});

it('leaves an abandoned child to finish outside its parent\'s rollback', function (): void {
    [$parent, $child] = parentWaitingOnChild('abandon', true);

    SagaFlow::loadFlow($parent->id)->compensate();

    SagaFlow::loadFlow($child->id)->signal('child.go');
    drainQueue();

    // Its parent planned while it was in flight, so the parent's compensation for it is in
    // no plan either.
    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Completed)
        ->and(CompensationLog::all())->toBe(['undo:parent']);
});

it('fences and closes a child whose first job has not run yet', function (): void {
    useDatabaseQueue();

    $parent = SagaFlow::create(FencingParentWorkflow::class)->withArguments('cancel', false)->run();

    for ($tick = 0; $tick < 20 && FlowRun::query()->where('parent_id', $parent->id)->doesntExist(); $tick++) {
        workOneJob();
    }

    $start = loseJobs('RunWorkflowJob');
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($start)->toHaveCount(1)
        ->and($child->status)->toBe(FlowStatus::Pending);

    SagaFlow::loadFlow($parent->id)->compensate();

    bringJobsBack($start);
    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent']);
});

it('leaves a child already rolling itself back out of its parent\'s fence', function (): void {
    useDatabaseQueue();

    $parent = SagaFlow::create(FencingParentOfSelfFailingWorkflow::class)->run();
    drainQueue();

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    SagaFlow::loadFlow($child->id)->signal('c.go');

    for ($tick = 0; $tick < 20 && SagaFlow::findRun($child->id)->status !== FlowStatus::Cancelling; $tick++) {
        workOneJob();
    }

    $own = loseJobs('RunCompensationJob');
    $token = SagaFlow::findRun($child->id)->rollback_token;

    SagaFlow::loadFlow($parent->id)->compensate();

    expect(SagaFlow::findRun($child->id)->rollback_token)->toBe($token);

    bringJobsBack($own);
    drainQueue();

    $failed = SagaFlow::findRun($child->id);

    expect($failed->status)->toBe(FlowStatus::Failed)
        ->and($failed->exception['class'])->toBe(ActionFailedException::class)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:c'])
        ->and(compensationTrail($child->id))->toBe(['c:completed']);
});

it('leaves a fence the first close job claimed to that job when another close job comes', function (): void {
    [$parent, $child] = parentWaitingOnChild('cancel');

    SagaFlow::loadFlow($parent->id)->compensate();
    loseJobs('CancelChildWorkflowJob');

    $first = new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true);

    // The first close job's process dies right after its claim lands.
    $dies = true;
    DB::listen(function (QueryExecuted $query) use (&$dies): void {
        if ($dies && str_starts_with(strtolower($query->sql), 'update') && str_contains($query->sql, 'rollback_token')) {
            $dies = false;

            throw new RuntimeException('the process died');
        }
    });

    expect(fn () => dispatch_sync($first))->toThrow(RuntimeException::class, 'the process died')
        ->and(SagaFlow::findRun($child->id)->rollback_token)->toBe($first->rollbackToken);

    dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true));

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(SagaFlow::findRun($child->id)->rollback_token)->toBe($first->rollbackToken)
        ->and(CompensationLog::all())->toBe(['undo:parent']);

    dispatch_sync(unserialize(serialize($first)));

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child']);
});

it('leaves a fenced child that a recovery took over before its close job to that recovery', function (): void {
    [$parent, $child] = parentWaitingOnChild('cancel');

    SagaFlow::loadFlow($parent->id)->compensate();
    $close = loseJobs('CancelChildWorkflowJob');

    $this->travel(2)->minutes();

    expect(app(FlowDoctor::class)->takeOverStalledRollback(SagaFlow::findRun($child->id)))->toBeTrue();

    $recovery = SagaFlow::findRun($child->id)->rollback_token;

    dispatch_sync(unserialize(json_decode($close[0]['payload'], true)['data']['command']));

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(SagaFlow::findRun($child->id)->rollback_token)->toBe($recovery)
        ->and(CompensationLog::all())->toBe(['undo:parent']);

    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child'])
        ->and(compensationTrail($child->id))->toBe(['child:completed']);
});

it('carries out the close a fence recorded when the parent\'s rollback is cut short', function (): void {
    [$parent, $child] = parentWaitingOnChild('cancel', false, now()->addSeconds(30));

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();
    loseJobs('RunCompensationJob');

    // A cancelled rollback closes its children without compensation; the fence already
    // recorded that this child is rolled back.
    SagaFlow::loadFlow($parent->id)->cancel();
    drainQueue();

    expect(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:child']);
});

it('recovers a fenced child whose close job never comes, to the close its policy owes', function (): void {
    config()->set('saga-lara-flow.repair.enabled', true);

    foreach (['cancel' => FlowStatus::Cancelled, 'fail' => FlowStatus::Failed] as $policy => $landed) {
        CompensationLog::reset();
        [$parent, $child] = parentWaitingOnChild($policy);

        SagaFlow::loadFlow($parent->id)->compensate();
        loseJobs('CancelChildWorkflowJob');

        expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(0);

        $this->travel(2)->minutes();

        expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(1);

        drainQueue();

        $closed = SagaFlow::findRun($child->id);

        expect($closed->status)->toBe($landed)
            ->and($closed->exception['class'] ?? null)->toBe($policy === 'fail' ? ParentClosedException::class : null)
            ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child']);
    }
});

it('fences the children of a child its close job moves itself, as that close says', function (): void {
    useDatabaseQueue();

    $root = SagaFlow::create(FencingRootWorkflow::class)->run();
    drainQueue();

    $middle = FlowRun::query()->where('parent_id', $root->id)->firstOrFail();
    $leaf = FlowRun::query()->where('parent_id', $middle->id)->firstOrFail();

    // A close that reaches a child nothing fenced, to land it without compensation.
    dispatch_sync(new CancelChildWorkflowJob($middle->id, FlowStatus::Cancelled, false));

    $fenced = SagaFlow::findRun($leaf->id);

    expect($fenced->status)->toBe(FlowStatus::Cancelling)
        ->and($fenced->rollback_compensates)->toBeFalse();

    drainQueue();

    expect(SagaFlow::findRun($leaf->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->not->toContain('undo:child');
});

it('fences a child when its parent lands from a rollback that fenced none', function (): void {
    foreach ([true, false] as $compensates) {
        [$parent, $child] = parentWaitingOnChild('cancel');

        // A parent moved into Cancelling by a version that did not fence its children.
        FlowRun::query()->whereKey($parent->id)->update([
            'status' => FlowStatus::Cancelling,
            'rollback_target' => FlowStatus::Cancelled,
            'rollback_compensates' => $compensates,
        ]);

        app(StateMachine::class)->transition(SagaFlow::findRun($parent->id), FlowStatus::Cancelled);

        $fenced = SagaFlow::findRun($child->id);

        expect($fenced->status)->toBe(FlowStatus::Cancelling)
            ->and($fenced->rollback_compensates)->toBe($compensates)
            ->and($fenced->rollback_token)->toBe($child->id);
    }
});

it('closes a child fenced without compensation whose close is lost, without rolling it back', function (): void {
    foreach (['repair', 'kick'] as $by) {
        CompensationLog::reset();
        config()->set('saga-lara-flow.repair.enabled', $by === 'repair');

        [$parent, $child] = parentWaitingOnChild('cancel');

        SagaFlow::loadFlow($parent->id)->cancel();
        loseJobs('CancelChildWorkflowJob');

        expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(0)
            ->and(app(FlowDoctor::class)->takeOverStalledRollback(SagaFlow::findRun($child->id)))->toBeFalse();

        $this->travel(2)->minutes();

        $by === 'repair'
            ? expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(1)
            : expect(app(FlowDoctor::class)->takeOverStalledRollback(SagaFlow::findRun($child->id)))->toBeTrue();

        drainQueue();

        expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
            ->and($parent->children()->first()->status)->toBe(ChildStatus::Cancelled)
            ->and(CompensationLog::all())->toBe([])
            ->and(compensationTrail($child->id))->toBe([]);
    }
});

it('refuses a move into Cancelling that its transaction did not keep', function (): void {
    [$parent, $child] = parentWaitingOnChild('cancel');

    // A listener that eats its own failure on the last statement of the move: PostgreSQL turns
    // the COMMIT that reports success into a rollback.
    $armed = true;
    DB::listen(function (QueryExecuted $query) use ($child, &$armed): void {
        if (! $armed || $query->connection->transactionLevel() === 0 || ! str_contains($query->sql, 'flow_children')
            || ! in_array($child->id, $query->bindings, true)) {
            return;
        }

        $armed = false;

        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // An observer that eats its own failure — broken, but nothing stops it.
        }
    });

    logToFile($log = sys_get_temp_dir().'/saga-fence-'.bin2hex(random_bytes(6)).'.log');

    expect(fn () => SagaFlow::loadFlow($parent->id)->compensate())->toThrow(ConcurrentFlowTransitionException::class)
        ->and(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Waiting)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Waiting)
        ->and(CompensationLog::all())->toBe([])
        ->and((string) @file_get_contents($log))->toContain(AnomalyLog::REASON_CLAIM_NOT_COMMITTED);
})->skip(
    fn () => TestCase::driver() !== 'pgsql',
    'Only PostgreSQL aborts a transaction on a failure swallowed inside it.',
);
