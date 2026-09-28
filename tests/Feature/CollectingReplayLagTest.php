<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\CompensationEntry;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ProbeHostThrowWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ProbeLaggingReplicaWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;

/**
 * Planning a rollback replays handle() over the run's history, and the replay stops at
 * the first seam its reads call unfinished. Under read/write splitting a lagging replica
 * answers those reads with an earlier state, so the plan comes back short and the
 * rollback reports a complete unwind over steps that stay applied. The pass reads the
 * history as the writer holds it.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    ProbeHostThrowWorkflow::reset();
});

afterEach(function (): void {
    ProbeHostThrowWorkflow::reset();
});

/**
 * The parent parked on its signal with both of its steps done and its child failed.
 *
 * @return array{parent: string, child: string}
 */
function laggingReplicaScene(): array
{
    $parent = SagaFlow::create(ProbeLaggingReplicaWorkflow::class)->runSync();

    // The child rolled itself back on the way; only the parent's rollback is under test.
    CompensationLog::reset();

    return [
        'parent' => $parent->id,
        'child' => $parent->children()->sole()->child_flow_run_id,
    ];
}

/**
 * Whether the connection has been told to read from the writer, which it keeps to itself.
 */
function readsFromWriter(Connection $connection): bool
{
    return (bool) (new ReflectionProperty($connection, 'readOnWriteConnection'))->getValue($connection);
}

/**
 * Whether the connection would hand this read to a replica — the rule of
 * Connection::getReadPdo(), for a connection that is not sticky.
 */
function servedByReplica(Model $model): bool
{
    $connection = $model->getConnection();

    return $connection->transactionLevel() === 0 && ! readsFromWriter($connection);
}

/**
 * @param  list<CompensationEntry>  $entries
 * @return list<int>
 */
function plannedSequences(array $entries): array
{
    return array_map(static fn (CompensationEntry $entry): int => $entry->sequence, $entries);
}

it('plans the whole history whichever part of it a replica holds back', function (string $lag): void {
    $scene = laggingReplicaScene();

    match ($lag) {
        // A: the child as it stood before it failed.
        'the child' => FlowRun::retrieved(function (FlowRun $run) use ($scene): void {
            if ($run->id === $scene['child'] && servedByReplica($run)) {
                $run->status = FlowStatus::Running;
                $run->syncOriginal();
            }
        }),
        // B: the child is current; only the parent's own later step is behind.
        "the parent's own later step" => ActionRun::retrieved(function (ActionRun $step) use ($scene): void {
            if ($step->flow_run_id === $scene['parent'] && $step->sequence === 2 && servedByReplica($step)) {
                $step->status = ActionStatus::Running;
                $step->result = null;
                $step->finished_at = null;
                $step->syncOriginal();
            }
        }),
        // C: nothing behind at all.
        'nothing' => null,
    };

    $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($scene['parent']));

    expect(plannedSequences($entries))->toBe([0, 2]);

    $run = SagaFlow::loadFlow($scene['parent'])->compensate();

    expect($run->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
})->with([
    'the child',
    "the parent's own later step",
    'nothing',
]);

it('reads nothing of the planning pass from the replica', function (): void {
    $scene = laggingReplicaScene();

    $run = SagaFlow::findRun($scene['parent']);
    $connection = $run->getConnection();

    // A replica that has seen none of this database: any read routed to it fails.
    $connection->setReadPdo(new PDO('sqlite::memory:'));

    try {
        $entries = app(FlowExecutor::class)->collectCompensations($run);
    } finally {
        $connection->setReadPdo(null);
    }

    expect(plannedSequences($entries))->toBe([0, 2]);
});

it('hands the connection back reading as it did', function (bool $pinned): void {
    $scene = laggingReplicaScene();

    $connection = SagaFlow::findRun($scene['parent'])->getConnection();
    $connection->useWriteConnectionWhenReading($pinned);

    app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($scene['parent']));

    expect(readsFromWriter($connection))->toBe($pinned);
})->with([
    'from its routing' => false,
    'from the writer, as its owner set it' => true,
]);

it('hands the connection back when planning fails', function (): void {
    $id = SagaFlow::create(ProbeHostThrowWorkflow::class)->runSync()->id;

    ProbeHostThrowWorkflow::$throw = RuntimeException::class;

    $run = SagaFlow::findRun($id);

    expect(fn () => app(FlowExecutor::class)->collectCompensations($run))
        ->toThrow(RuntimeException::class, 'the host raised this one itself')
        ->and(readsFromWriter($run->getConnection()))->toBeFalse();
});

it('sees a step another connection settles while the plan is being read', function (): void {
    $scene = laggingReplicaScene();

    $table = (new ActionRun)->getTable();
    $settled = (array) DB::connection('testing')->table($table)
        ->where('flow_run_id', $scene['parent'])->where('sequence', 2)->first();

    // Step 2 still running as the pass begins, the way a worker that claimed it leaves it.
    DB::connection('testing')->table($table)->where('id', $settled['id'])
        ->update(['status' => ActionStatus::Running->value, 'result' => null, 'finished_at' => null]);

    // The strictest isolation a host may configure on the connection, on both servers.
    DB::connection('testing')->statement(TestCase::driver() === 'pgsql'
        ? 'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ'
        : 'SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ');

    config()->set('database.connections.competitor', config('database.connections.testing'));

    // Its worker commits the outcome once the pass has read step 0 and before it reaches 2.
    $done = false;

    ActionRun::retrieved(function (ActionRun $step) use ($scene, $table, $settled, &$done): void {
        if ($done || $step->flow_run_id !== $scene['parent'] || $step->sequence !== 0
            || ! readsFromWriter($step->getConnection())) {
            return;
        }

        $done = true;

        DB::connection('competitor')->table($table)->where('id', $settled['id'])->update([
            'status' => ActionStatus::Completed->value,
            'result' => $settled['result'],
            'finished_at' => $settled['finished_at'],
        ]);
    });

    try {
        $entries = app(FlowExecutor::class)->collectCompensations(SagaFlow::findRun($scene['parent']));
    } finally {
        DB::purge('competitor');
    }

    expect($done)->toBeTrue()
        ->and(plannedSequences($entries))->toBe([0, 2]);
})->skip(fn () => TestCase::driver() === 'sqlite', 'An in-memory SQLite database has no second connection.');
