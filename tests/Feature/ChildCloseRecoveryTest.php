<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalCancellingFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ParentClosedException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowPruner;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/*
 * A close a child is owed outlives the job sent to carry it out: the child is fenced in
 * Cancelling with that close recorded on it, and the stalled-rollback recovery (the doctor's R5,
 * or a kick) carries out whatever no job finished — with or without compensation.
 */

beforeEach(function (): void {
    CompensationLog::reset();
    ClosedChildWorkflow::$fault = false;
});

final class ClosedChildWorkflow extends Workflow
{
    public static bool $fault = false;

    public function handle(): void
    {
        if (self::$fault) {
            throw new RuntimeException('the record this argument reads is gone');
        }

        $this->action(MakeValueAction::class, 'child')->compensateWith(UndoAction::class, 'child')->run();
        $this->awaitSignal('child.go');
    }
}

final class ClosingParentWorkflow extends Workflow
{
    public function handle(string $policy = 'cancel'): void
    {
        $this->action(MakeValueAction::class, 'parent')->compensateWith(UndoAction::class, 'parent')->run();
        $this->child(ClosedChildWorkflow::class)->closePolicy(ChildClosePolicy::from($policy))->run();
    }
}

/**
 * @return array{0: FlowRun, 1: FlowRun} the parent waiting on its child, the child on child.go
 */
function closingParentWaitingOnChild(string $policy = 'cancel'): array
{
    useDatabaseQueue();

    $parent = SagaFlow::create(ClosingParentWorkflow::class)->withArguments($policy)->run();
    drainQueue();

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($child->status)->toBe(FlowStatus::Waiting);

    return [$parent, $child];
}

function linkStatus(FlowRun $child): ChildStatus
{
    return $child->parent->children()->where('child_flow_run_id', $child->id)->firstOrFail()->status;
}

it('closes a child whose close job failed to plan once the fault clears', function (string $policy): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    [$parent, $child] = closingParentWaitingOnChild($policy);
    logToFile($log = sys_get_temp_dir().'/saga-close-'.bin2hex(random_bytes(6)).'.log');

    ClosedChildWorkflow::$fault = true;
    SagaFlow::loadFlow($parent->id)->compensate();
    drainQueue();

    // The job is gone; the close it carried is still on the child, and the queue's failure is
    // in the engine's own journal.
    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(linkStatus($child))->toBe(ChildStatus::Running)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0)
        ->and((string) file_get_contents($log))->toContain(AnomalyLog::REASON_CLOSE_FAILED)
        ->toContain($child->id)
        ->toContain('the record this argument reads is gone');

    ClosedChildWorkflow::$fault = false;
    $this->travel(2)->minutes();

    expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(1);

    drainQueue();

    $closed = SagaFlow::findRun($child->id);

    expect($closed->status)->toBe($policy === 'fail' ? FlowStatus::Failed : FlowStatus::Cancelled)
        ->and($closed->exception['class'] ?? null)->toBe($policy === 'fail' ? ParentClosedException::class : null)
        ->and(linkStatus($child))->toBe($policy === 'fail' ? ChildStatus::Failed : ChildStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child']);
})->with(['cancel', 'fail']);

it('closes a child whose close job failed when the same job is sent again', function (): void {
    [$parent, $child] = closingParentWaitingOnChild();

    ClosedChildWorkflow::$fault = true;
    SagaFlow::loadFlow($parent->id)->compensate();
    $close = loseJobs('CancelChildWorkflowJob');
    bringJobsBack($close);
    drainQueue();

    ClosedChildWorkflow::$fault = false;
    bringJobsBack($close);
    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:parent', 'undo:child']);
});

it('fences a child nothing fenced before planning its close, so a failed plan leaves it closing', function (): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    [, $child] = closingParentWaitingOnChild();

    ClosedChildWorkflow::$fault = true;

    expect(fn () => dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true)))
        ->toThrow(RuntimeException::class, 'the record this argument reads is gone');

    $fenced = SagaFlow::findRun($child->id);

    expect($fenced->status)->toBe(FlowStatus::Cancelling)
        ->and($fenced->rollback_target)->toBe(FlowStatus::Cancelled)
        ->and(fn () => SagaFlow::loadFlow($child->id)->signal('child.go'))
        ->toThrow(CannotSignalCancellingFlowException::class);

    ClosedChildWorkflow::$fault = false;
    $this->travel(2)->minutes();
    app(FlowDoctor::class)->repair();
    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toContain('undo:child');
});

it('lands a close without compensation that died right after its own move', function (): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    [, $child] = closingParentWaitingOnChild();

    // The process dies on its first statement after the move's transaction has committed.
    $moved = false;
    $dies = true;
    DB::listen(function (QueryExecuted $query) use (&$dies, &$moved): void {
        if (str_starts_with(strtolower($query->sql), 'update') && in_array('cancelling', $query->bindings, true)) {
            $moved = true;

            return;
        }

        if ($dies && $moved && $query->connection->transactionLevel() === 0) {
            $dies = false;

            throw new RuntimeException('the process died');
        }
    });

    expect(fn () => dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, false)))
        ->toThrow(RuntimeException::class, 'the process died')
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelling);

    $this->travel(2)->minutes();

    expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(1);

    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(linkStatus($child))->toBe(ChildStatus::Cancelled)
        ->and(CompensationLog::all())->not->toContain('undo:child');
});

it('lands a close without compensation whose job claimed the fence and died', function (): void {
    [$parent, $child] = closingParentWaitingOnChild();

    SagaFlow::loadFlow($parent->id)->cancel();
    loseJobs('CancelChildWorkflowJob');

    $dies = true;
    DB::listen(function (QueryExecuted $query) use (&$dies): void {
        if ($dies && str_starts_with(strtolower($query->sql), 'update') && str_contains($query->sql, 'rollback_token')) {
            $dies = false;

            throw new RuntimeException('the process died');
        }
    });

    expect(fn () => dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, false)))
        ->toThrow(RuntimeException::class, 'the process died');

    $this->travel(2)->minutes();

    expect(app(FlowDoctor::class)->takeOverStalledRollback(SagaFlow::findRun($child->id)))->toBeTrue()
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe([]);
});

it('lands a child fenced without compensation once, whoever of its close and a recovery comes second', function (): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.repair.grace_seconds', 0);
    [$parent, $child] = closingParentWaitingOnChild();

    SagaFlow::loadFlow($parent->id)->cancel();
    $close = loseJobs('CancelChildWorkflowJob');

    // With no grace the recovery lands the child before its close job is taken off a slow queue.
    $this->travel(1)->seconds();

    expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(1)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled);

    bringJobsBack($close);
    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(SagaFlow::findRun($child->id)->events()->where('type', FlowEventType::FlowCancelled->value)->count())->toBe(1);
});

it('lands a child once when a recovery takes it over right after its close job claimed it', function (): void {
    config()->set('saga-lara-flow.repair.grace_seconds', 0);
    [$parent, $child] = closingParentWaitingOnChild();

    SagaFlow::loadFlow($parent->id)->cancel();
    loseJobs('CancelChildWorkflowJob');

    $armed = true;
    DB::listen(function (QueryExecuted $query) use (&$armed, $child): void {
        if ($armed && str_starts_with(strtolower($query->sql), 'update') && str_contains($query->sql, 'rollback_token')) {
            $armed = false;

            test()->travel(1)->seconds();

            expect(app(FlowDoctor::class)->takeOverStalledRollback(SagaFlow::findRun($child->id)))->toBeTrue();
        }
    });

    dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, false));

    expect($armed)->toBeFalse()
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(SagaFlow::findRun($child->id)->events()->where('type', FlowEventType::FlowCancelled->value)->count())->toBe(1);
});

it('closes a child whose start commits after its parent was closed, before it does any work', function (string $policy, string $how): void {
    useDatabaseQueue();

    $parent = SagaFlow::create(ClosingParentWorkflow::class)->withArguments($policy)->run();

    // The parent is closed between its pass's check that it may start the child and the link
    // that records the child, so neither its fence nor its close sees the link.
    $reached = beforeStatement('/^insert into [`"]?\w*flow_children/i', function () use ($parent, $how): void {
        $how === 'cancel' ? SagaFlow::loadFlow($parent->id)->cancel() : SagaFlow::loadFlow($parent->id)->compensate();
    });

    drainQueue();

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();
    $closed = SagaFlow::findRun($child->id);

    expect($reached())->toBeTrue()
        ->and(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Cancelled)
        ->and($closed->status)->toBe($policy === 'fail' ? FlowStatus::Failed : FlowStatus::Cancelled)
        ->and($closed->exception['class'] ?? null)->toBe($policy === 'fail' ? ParentClosedException::class : null)
        ->and($closed->actions()->count())->toBe(0)
        ->and(linkStatus($child))->toBe($policy === 'fail' ? ChildStatus::Failed : ChildStatus::Cancelled)
        ->and(CompensationLog::all())->not->toContain('undo:child');
})->with(['cancel', 'fail'])->with(['cancel', 'compensate']);

it('closes a child whose start commits after its parent was cancelled and whose first job is lost', function (): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    [, $child] = childLeftPendingUnderClosedParent();

    $this->travel(2)->minutes();

    expect(app(FlowDoctor::class)->repair()->rewokenFlows)->toBe(1);

    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(SagaFlow::findRun($child->id)->actions()->count())->toBe(0);
});

it('sends no close of its own for a child its parent fenced while its first step asked', function (): void {
    useDatabaseQueue();

    $parent = SagaFlow::create(ClosingParentWorkflow::class)->run();

    beforeStatement('/^insert into [`"]?\w*flow_children/i', function () use ($parent): void {
        SagaFlow::loadFlow($parent->id)->cancel();
    });

    // The parent's own fence reaches the child between its first step's read and its fence.
    beforeStatement('/^update [`"]?\w*flow_runs[`"]? set .*rollback_token/i', function () use ($parent): void {
        $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

        expect(app(ChildRecorder::class)->fence($child, ChildClosePolicy::Cancel, false, ParentClosedException::for(SagaFlow::findRun($parent->id))))
            ->toBeTrue();
    });

    $closes = 0;
    Event::listen(JobQueued::class, function (JobQueued $event) use (&$closes): void {
        $closes += $event->job instanceof CancelChildWorkflowJob ? 1 : 0;
    });

    drainQueue();

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($closes)->toBe(0)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(SagaFlow::findRun($child->id)->rollback_token)->toBe($child->id);
});

/**
 * Start the parent, cancel it between its pass's check and the child's link, and lose the child's
 * first job: a Pending child left under a closed parent with nothing on the queue for it.
 *
 * @return array{0: FlowRun, 1: FlowRun}
 */
function childLeftPendingUnderClosedParent(string $policy = 'cancel'): array
{
    useDatabaseQueue();

    $parent = SagaFlow::create(ClosingParentWorkflow::class)->withArguments($policy)->run();

    beforeStatement('/^insert into [`"]?\w*flow_children/i', function () use ($parent): void {
        SagaFlow::loadFlow($parent->id)->cancel();
    });

    Event::listen(JobQueued::class, function (JobQueued $event): void {
        if ($event->job instanceof RunWorkflowJob && SagaFlow::findRun($event->job->flowRunId)->parent_id !== null) {
            DB::connection('testing')->table('jobs')->where('id', $event->id)->delete();
        }
    });

    drainQueue();
    Event::forget(JobQueued::class);

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($child->status)->toBe(FlowStatus::Pending);

    return [$parent, $child];
}

it('closes a child whose parent was pruned, as a closed parent\'s', function (string $policy): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    [$parent, $child] = childLeftPendingUnderClosedParent($policy);

    $this->travel(2)->minutes();
    app(FlowPruner::class)->prune(now()->addMinute());

    expect(FlowRun::query()->whereKey($parent->id)->exists())->toBeFalse()
        ->and(app(FlowDoctor::class)->repair()->rewokenFlows)->toBe(1);

    drainQueue();

    $closed = SagaFlow::findRun($child->id);

    expect($closed->status)->toBe($policy === 'fail' ? FlowStatus::Failed : FlowStatus::Cancelled)
        ->and($closed->actions()->count())->toBe(0)
        ->and($closed->exception['message'] ?? null)->toBe($policy === 'fail'
            ? ParentClosedException::forPruned($parent->id)->getMessage()
            : null);
})->with(['cancel', 'fail']);

it('fences a child its close job reaches even when the child\'s tenant cannot be entered', function (): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    [, $child] = closingParentWaitingOnChild();

    tenantThatCannotBeEntered();

    expect(fn () => dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true)))
        ->toThrow(RuntimeException::class, 'the tenant store is unreachable');

    $fenced = SagaFlow::findRun($child->id);

    expect($fenced->status)->toBe(FlowStatus::Cancelling)
        ->and($fenced->rollback_target)->toBe(FlowStatus::Cancelled);

    config()->set('saga-lara-flow.tenancy.auto', false);
    $this->travel(2)->minutes();

    expect(app(FlowDoctor::class)->repair()->resumedRollbacks)->toBe(1);

    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toContain('undo:child');
});

it('fences a child under a closed parent at its first step even when its tenant cannot be entered', function (): void {
    config()->set('saga-lara-flow.repair.enabled', true);
    [, $child] = childLeftPendingUnderClosedParent();

    tenantThatCannotBeEntered();
    $this->travel(2)->minutes();

    expect(app(FlowDoctor::class)->repair()->rewokenFlows)->toBe(1);

    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(SagaFlow::findRun($child->id)->actions()->count())->toBe(0);

    config()->set('saga-lara-flow.tenancy.auto', false);
    $this->travel(2)->minutes();
    app(FlowDoctor::class)->repair();
    drainQueue();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled);
});

function tenantThatCannotBeEntered(): void
{
    config()->set('saga-lara-flow.tenancy.auto', true);
    config()->set('saga-lara-flow.tenancy.capture', fn (): array => []);
    config()->set('saga-lara-flow.tenancy.restore', function (array $context): void {
        throw new RuntimeException('the tenant store is unreachable');
    });
}

it('makes its move again when the child steps between live states as its close fences it', function (): void {
    [, $child] = closingParentWaitingOnChild();

    // The child is resumed between the close's read and its move: the move, fenced on the
    // status it read, loses to a child that is still live.
    $reached = beforeStatement('/^update [`"]?\w*flow_runs[`"]? set .*rollback_token/i', function () use ($child): void {
        FlowRun::query()->whereKey($child->id)->update(['status' => FlowStatus::Running]);
    });

    dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true));

    expect($reached())->toBeTrue()
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:child']);
});

it('closes a child whose parent was pruned before its close job came, with the parent named', function (): void {
    [$parent, $child] = closingParentWaitingOnChild('fail');

    FlowRun::query()->whereKey($parent->id)->update(['status' => FlowStatus::Failed]);
    app(FlowPruner::class)->prune(now()->addMinute());

    dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Failed, true));

    $failed = SagaFlow::findRun($child->id);

    expect($failed->status)->toBe(FlowStatus::Failed)
        ->and($failed->exception['message'] ?? null)->toBe(ParentClosedException::forPruned($parent->id)->getMessage());
});

it('does not make again a move its own transaction did not keep', function (): void {
    [, $child] = closingParentWaitingOnChild();

    // A listener that eats its own failure on the last statement of the move: PostgreSQL turns
    // the COMMIT that reports success into a rollback, and nothing moved the child meanwhile.
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

    expect(fn () => dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true)))
        ->toThrow(ConcurrentFlowTransitionException::class)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Waiting);
})->skip(
    fn () => TestCase::driver() !== 'pgsql',
    'Only PostgreSQL aborts a transaction on a failure swallowed inside it.',
);
