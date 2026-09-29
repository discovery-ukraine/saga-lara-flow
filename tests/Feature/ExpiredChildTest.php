<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowChildRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowExpired;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WaitingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * Expired is terminal: nothing will ever finalize such a child again, so no resume is
 * coming for a parent that parks on it, and a rollback planned over it stops short.
 * The seam answers it the way it answers a failed child — a throw the parent may be
 * told to survive with continueParentOnFailure().
 */
beforeEach(function (): void {
    CompensationLog::reset();
});

final class ContinuePastExpiringChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(WaitingChildWorkflow::class)->continueParentOnFailure()->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->awaitSignal('go');
    }
}

final class StopAtExpiringChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(WaitingChildWorkflow::class)->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
        $this->awaitSignal('go');
    }
}

/**
 * A parent parked on a child that is itself parked on a signal, both queued and idle.
 *
 * @return array{0: FlowRun, 1: FlowRun}
 */
function parentOverWaitingChild(string $workflow): array
{
    $run = SagaFlow::create($workflow)->run();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);
    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and($child->status)->toBe(FlowStatus::Waiting);

    return [$parent, $child];
}

/**
 * The child's deadline passes and the sweep expires it, the way any run with an
 * expires_at is expired; the child finalizing wakes the parent.
 */
function expireChild(FlowRun $child): void
{
    $child->newQuery()->toBase()->where('id', $child->id)->update(['expires_at' => now()->subMinute()]);

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1)
        ->and($child->fresh()->status)->toBe(FlowStatus::Expired);
}

/**
 * @return list<int>
 */
function plannedOver(FlowRun $parent): array
{
    return array_map(
        fn ($entry) => $entry->sequence,
        app(FlowExecutor::class)->collectCompensations($parent->fresh()),
    );
}

it('carries a parent past a child that expired, when told to survive its failure', function (): void {
    useDatabaseQueue();

    [$parent, $child] = parentOverWaitingChild(ContinuePastExpiringChildWorkflow::class);
    expireChild($child);
    drainQueue();

    $parent = $parent->fresh();

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and($parent->actions()->where('status', 'completed')->pluck('sequence')->sort()->values()->all())
        ->toBe([0, 2])
        ->and(plannedOver($parent))->toBe([0, 2]);

    $rolled = SagaFlow::loadFlow($parent->id)->compensate();

    expect($rolled->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('records the expiry on the link and in the parent\'s history', function (): void {
    useDatabaseQueue();

    $announced = [];
    Event::listen(ChildWorkflowExpired::class, function (ChildWorkflowExpired $event) use (&$announced): void {
        $announced[] = $event->childFlowRun->id;
    });

    [$parent, $child] = parentOverWaitingChild(ContinuePastExpiringChildWorkflow::class);
    expireChild($child);

    $link = FlowChild::query()->where('child_flow_run_id', $child->id)->firstOrFail();
    $recorded = FlowEvent::query()
        ->where('flow_run_id', $parent->id)
        ->where('type', FlowEventType::ChildExpired->value)
        ->pluck('sequence')
        ->all();

    expect($link->status)->toBe(ChildStatus::Expired)
        ->and(app(FlowChildRepository::class)->active($parent->id))->toBeEmpty()
        ->and($recorded)->toBe([1])
        ->and($announced)->toBe([$child->id]);
});

it('fails a parent whose child expired, and rolls back what the parent did', function (): void {
    useDatabaseQueue();

    [$parent, $child] = parentOverWaitingChild(StopAtExpiringChildWorkflow::class);
    expireChild($child);
    drainQueue();

    $parent = $parent->fresh();

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->exception['class'] ?? null)->toBe(ChildWorkflowExpiredException::class)
        ->and($parent->exception['message'] ?? null)->toContain('expired')
        ->and($parent->actions()->where('sequence', 2)->exists())->toBeFalse()
        ->and(CompensationLog::all())->toBe(['undo:a']);
});

/**
 * Every step of the parent is done and the child's row is then read as each status in
 * turn: a terminal child is history the plan reads past, one in flight is its frontier.
 */
it('plans a parent over its child by what the child came to', function (
    FlowStatus $childLands,
    array $planned,
): void {
    useDatabaseQueue();

    [$parent, $child] = parentOverWaitingChild(ContinuePastExpiringChildWorkflow::class);
    SagaFlow::loadFlow($child->id)->signal('child.go');
    drainQueue();

    expect($parent->fresh()->actions()->where('status', 'completed')->count())->toBe(2);

    $child->newQuery()->toBase()->where('id', $child->id)->update(['status' => $childLands->value]);

    expect(plannedOver($parent))->toBe($planned);
})->with([
    'completed' => [FlowStatus::Completed, [0, 2]],
    'failed' => [FlowStatus::Failed, [0, 2]],
    'expired' => [FlowStatus::Expired, [0, 2]],
    'cancelling' => [FlowStatus::Cancelling, [0]],
    'waiting' => [FlowStatus::Waiting, [0]],
]);

it('ends a plan where an expired child ended the parent, without escaping', function (): void {
    useDatabaseQueue();

    [$parent, $child] = parentOverWaitingChild(StopAtExpiringChildWorkflow::class);
    $child->newQuery()->toBase()->where('id', $child->id)->update(['status' => FlowStatus::Expired->value]);
    DB::connection('testing')->table('jobs')->delete();

    expect(plannedOver($parent))->toBe([0]);
});
