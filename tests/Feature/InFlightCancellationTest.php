<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;

/**
 * Issue #54, measured: what a step write does today while another session holds a
 * cancellation it has written and not yet committed. These tests pin current behaviour,
 * defect included, so that a change to it — a fix or a regression — shows up here first.
 *
 * A conditional write is fenced on what the run's row says. A terminal transition writes
 * the run and settles its steps in one transaction, and the engine takes no lock of its own
 * before a step write, so whether the write sees that transaction before it commits is the
 * server's answer:
 *
 * - MySQL under REPEATABLE READ: the settlement's UPDATE keeps a lock on every step row it
 *   read, the Failed one it left as it is included, so every write to the step waits for the
 *   cancellation, then reads it committed and refuses.
 * - MySQL under READ COMMITTED drops the lock on a row the UPDATE read but did not change, and
 *   reads the fence without one; PostgreSQL locks only the rows a statement changes and reads
 *   the fence from a snapshot. Either way a Failed step is claimed or given up on a run that is
 *   already over. A write to a step the settlement did change waits and refuses; PostgreSQL
 *   under REPEATABLE READ fails it with a serialization error instead.
 *
 * Both sessions run at the level under test.
 *
 * A move into Cancelling is a transaction of the engine's own too: the run's write and the
 * fence of its live Cancel/Fail children, and a child's fence commits with its parent's
 * landing or move. It changes no step row, so only the claim reading the run under a shared
 * lock (MySQL under REPEATABLE READ) waits for it; elsewhere a Failed or Pending step is
 * claimed under a run that is already rolling back.
 *
 * Server only: on SQLite one writer holds the whole database, so a cancellation in flight
 * and a step write cannot overlap.
 */
beforeEach(function () {
    if (TestCase::driver() === 'sqlite') {
        $this->markTestSkipped('A cancellation in flight needs a second session on a server; SQLite admits one writer.');
    }

    lockTimeout(DB::connection('testing'), 10);
});

function inFlightStep(ActionStatus $status, ?FlowRun $parent = null): ActionRun
{
    $run = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
        'parent_id' => $parent?->id,
    ]);

    if ($parent !== null) {
        FlowChild::create([
            'parent_flow_run_id' => $parent->id,
            'child_flow_run_id' => $run->id,
            'sequence' => 0,
            'child_workflow_class' => OneActionWorkflow::class,
            'close_policy' => ChildClosePolicy::Cancel,
            'status' => ChildStatus::Running,
        ]);
    }

    $step = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => $status,
        'attempts' => $status === ActionStatus::Pending ? 0 : 1,
    ]);

    return ActionRun::query()->findOrFail($step->id);
}

/**
 * @param  array{waited: bool, returned: mixed, threw: ?Throwable}  $outcome
 * @return array{bool, mixed, FlowStatus, ActionStatus}
 */
function inFlightResult(array $outcome, ActionRun $step): array
{
    return [
        $outcome['waited'],
        $outcome['threw'] instanceof QueryException ? $outcome['threw']->getCode() : $outcome['returned'],
        FlowRun::query()->findOrFail($step->flow_run_id)->status,
        ActionRun::query()->findOrFail($step->id)->status,
    ];
}

function cancelInFlight(ActionRun $step): Closure
{
    return fn () => SagaFlow::loadFlow($step->flow_run_id)->cancel();
}

/**
 * The transaction FlowStateMachine::rollBack() opens, as compensate() and every other rollback
 * moves a run.
 */
function rollbackInFlight(string $runId): Closure
{
    return fn () => app(StateMachine::class)->transition(
        FlowRun::query()->findOrFail($runId)->intendRollback(FlowStatus::Cancelled, null),
        FlowStatus::Cancelling,
    );
}

$isolationLevels = ['read committed', 'repeatable read'];

it('claims a Failed step under a cancel() still in flight, unless the server makes the claim wait', function (string $level) {
    $step = inFlightStep(ActionStatus::Failed);

    $outcome = againstUncommitted(cancelInFlight($step), fn () => app(ActionRecorder::class)->startAction($step), $level);

    expect(inFlightResult($outcome, $step))->toBe(match (TestCase::driver().' '.$level) {
        'mysql repeatable read' => [true, false, FlowStatus::Cancelled, ActionStatus::Failed],
        'mysql read committed', 'pgsql read committed', 'pgsql repeatable read' => [false, true, FlowStatus::Cancelled, ActionStatus::Running],
    });
})->with($isolationLevels);

it('gives up on a Failed optional step under a cancel() still in flight, unless the server makes it wait', function (string $level) {
    $step = inFlightStep(ActionStatus::Failed);

    $outcome = againstUncommitted(cancelInFlight($step), fn () => app(ActionRecorder::class)->optionalFail($step), $level);

    expect(inFlightResult($outcome, $step))->toBe(match (TestCase::driver().' '.$level) {
        'mysql repeatable read' => [true, false, FlowStatus::Cancelled, ActionStatus::Failed],
        'mysql read committed', 'pgsql read committed', 'pgsql repeatable read' => [false, true, FlowStatus::Cancelled, ActionStatus::OptionalFailed],
    });
})->with($isolationLevels);

it('makes an outcome write wait on the step a cancel() in flight has settled, and refuses it', function (string $level) {
    $step = inFlightStep(ActionStatus::Running);

    $outcome = againstUncommitted(cancelInFlight($step), fn () => app(ActionRecorder::class)->completeAction($step, 'value'), $level);

    expect(inFlightResult($outcome, $step))->toBe(match (TestCase::driver().' '.$level) {
        'mysql read committed', 'mysql repeatable read', 'pgsql read committed' => [true, false, FlowStatus::Cancelled, ActionStatus::Cancelled],
        'pgsql repeatable read' => [true, '40001', FlowStatus::Cancelled, ActionStatus::Cancelled],
    });
})->with($isolationLevels);

it('claims a step under a move into Cancelling still in flight, unless the server makes the claim wait', function (string $level, ActionStatus $status) {
    $step = inFlightStep($status);

    $outcome = againstUncommitted(rollbackInFlight($step->flow_run_id), fn () => app(ActionRecorder::class)->startAction($step), $level);

    expect(inFlightResult($outcome, $step))->toBe(match (TestCase::driver().' '.$level) {
        'mysql repeatable read' => [true, false, FlowStatus::Cancelling, $status],
        'mysql read committed', 'pgsql read committed', 'pgsql repeatable read' => [false, true, FlowStatus::Cancelling, ActionStatus::Running],
    });
})->with($isolationLevels)->with([ActionStatus::Failed, ActionStatus::Pending]);

it('claims a child\'s step while its parent\'s transaction fences it, unless the server makes the claim wait', function (string $level, string $parentMove) {
    // A real queue: the parent's close job is queued inside the rival's transaction rather than
    // run there.
    useDatabaseQueue();

    $parent = app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);

    $step = inFlightStep(ActionStatus::Failed, $parent);

    $move = $parentMove === 'cancel()' ? fn () => SagaFlow::loadFlow($parent->id)->cancel() : rollbackInFlight($parent->id);

    $outcome = againstUncommitted($move, fn () => app(ActionRecorder::class)->startAction($step), $level);

    expect(inFlightResult($outcome, $step))->toBe(match (TestCase::driver().' '.$level) {
        'mysql repeatable read' => [true, false, FlowStatus::Cancelling, ActionStatus::Failed],
        'mysql read committed', 'pgsql read committed', 'pgsql repeatable read' => [false, true, FlowStatus::Cancelling, ActionStatus::Running],
    });
})->with($isolationLevels)->with(['cancel()', 'its move into Cancelling']);
