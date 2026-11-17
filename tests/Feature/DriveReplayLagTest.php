<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ParentClosedException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaActionRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaBuilder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaCompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaFlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ManualCompensateWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelEchoWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentFailPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SagaRollbackWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\StealsRunCompensation;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\StolenRollbackWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * A pass that drives a run replays handle() over the history it reads back, and every
 * job decides from a first read whether there is anything for it to do. Under
 * read/write splitting a replica that has not caught up answers those reads with an
 * earlier state: a row recorded a moment ago looks missing, still running, or a status
 * behind. Both read from the writer.
 */
beforeEach(function (): void {
    LaggingReplicaBuilder::reset();
    CompensationLog::reset();
    StealsRunCompensation::reset();
    config()->set('saga-lara-flow.models.action_run', LaggingReplicaActionRun::class);
});

afterEach(function (): void {
    LaggingReplicaBuilder::reset();
    StealsRunCompensation::reset();
});

final class DriveReplayLagWorkflow extends Workflow
{
    /**
     * @return array{a: mixed, b: mixed}
     */
    public function handle(): array
    {
        $a = $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $b = $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();

        return ['a' => $a, 'b' => $b];
    }
}

/**
 * @return list<string>
 */
function driveReplayLagSteps(string $flowRunId): array
{
    return ActionRun::query()
        ->useWritePdo()
        ->where('flow_run_id', $flowRunId)
        ->orderBy('sequence')
        ->get()
        ->map(fn (ActionRun $step): string => $step->sequence.':'.$step->status->value)
        ->all();
}

/**
 * @return list<string>
 */
function driveReplayLagCompleted(int $steps): array
{
    return array_map(fn (int $sequence): string => $sequence.':'.ActionStatus::Completed->value, range(0, $steps - 1));
}

function driveReplayLagRun(string $flowRunId): FlowRun
{
    return FlowRun::query()->useWritePdo()->findOrFail($flowRunId);
}

it('replays a step it has just run inline although the replica does not hold it yet', function (): void {
    LaggingReplicaBuilder::$noSteps = true;

    $run = SagaFlow::create(DriveReplayLagWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->exception)->toBeNull()
        ->and(driveReplayLagSteps($run->id))->toBe(driveReplayLagCompleted(2))
        ->and(CompensationLog::all())->toBe([]);
});

it('drives a run on a sync connection although the replica does not hold its steps yet', function (): void {
    useDatabaseQueue();
    config()->set('queue.default', 'sync');
    LaggingReplicaBuilder::$noSteps = true;

    $run = SagaFlow::create(DriveReplayLagWorkflow::class)->run();

    expect(driveReplayLagRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(driveReplayLagSteps($run->id))->toBe(driveReplayLagCompleted(2));
});

it('resumes past a step the replica still shows running', function (): void {
    useDatabaseQueue();
    LaggingReplicaBuilder::$unfinishedSteps = true;

    $run = SagaFlow::create(DriveReplayLagWorkflow::class)->run();
    drainQueue();

    expect(driveReplayLagRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(driveReplayLagSteps($run->id))->toBe(driveReplayLagCompleted(2));
});

it('reads nothing of the drive pass from the replica', function (): void {
    $pending = SagaFlow::create(DriveReplayLagWorkflow::class);
    $connection = DB::connection();

    // A replica that has seen none of this database: any read routed to it fails.
    $connection->setReadPdo(new PDO('sqlite::memory:'));

    try {
        $run = $pending->runSync();
    } finally {
        $connection->setReadPdo(null);
    }

    expect($run->status)->toBe(FlowStatus::Completed);
});

it('hands the connection back reading as it did', function (bool $pinned): void {
    $connection = DB::connection();
    $connection->useWriteConnectionWhenReading($pinned);

    try {
        SagaFlow::create(DriveReplayLagWorkflow::class)->runSync();

        expect(readsFromWriter($connection))->toBe($pinned);
    } finally {
        $connection->useWriteConnectionWhenReading(false);
    }
})->with([
    'from its routing' => false,
    'from the writer, as its owner set it' => true,
]);

it('hands the connection back when the pass throws', function (): void {
    FlowEvent::creating(function (FlowEvent $event): void {
        if ($event->type === FlowEventType::FlowStarted) {
            throw new RuntimeException('the event log refused the write');
        }
    });

    expect(fn () => SagaFlow::create(DriveReplayLagWorkflow::class)->runSync())
        ->toThrow(RuntimeException::class, 'the event log refused the write')
        ->and(readsFromWriter(DB::connection()))->toBeFalse();
});

it('starts and resumes a run whose jobs the replica shows a status behind', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);

    $run = SagaFlow::create(DriveReplayLagWorkflow::class)->run();

    LaggingReplicaBuilder::$runningRuns = [$run->id];

    drainQueue();

    expect(driveReplayLagRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(driveReplayLagSteps($run->id))->toBe(driveReplayLagCompleted(2));
});

it('runs a step whose job the replica does not show its row yet', function (): void {
    useDatabaseQueue();
    LaggingReplicaBuilder::$noSteps = true;

    $run = SagaFlow::create(DriveReplayLagWorkflow::class)->run();
    drainQueue();

    expect(driveReplayLagRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(driveReplayLagSteps($run->id))->toBe(driveReplayLagCompleted(2));
});

it('runs a parallel member whose job the replica does not show its row yet', function (): void {
    useDatabaseQueue();
    LaggingReplicaBuilder::$noSteps = true;

    $run = SagaFlow::create(ParallelEchoWorkflow::class)->run();
    drainQueue();

    expect(driveReplayLagRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(driveReplayLagSteps($run->id))->toBe(driveReplayLagCompleted(3));
});

it('runs a compensation whose job the replica does not show its row yet', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.models.compensation_run', LaggingReplicaCompensationRun::class);
    LaggingReplicaBuilder::$noCompensations = true;

    $run = SagaFlow::create(SagaRollbackWorkflow::class)->run();
    drainQueue();

    expect(driveReplayLagRun($run->id)->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('leaves a finished child alone although the replica still shows it running', function (): void {
    logToFile($log = sys_get_temp_dir().'/drive-replay-lag-'.uniqid().'.log');
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);

    $child = SagaFlow::create(DriveReplayLagWorkflow::class)->runSync();

    LaggingReplicaBuilder::$runningRuns = [$child->id];

    dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true));

    expect(driveReplayLagRun($child->id)->status)->toBe(FlowStatus::Completed)
        ->and(CompensationLog::all())->toBe([])
        ->and(file_exists($log) ? file_get_contents($log) : '')->not->toContain('transition_lost');
});

it('ends a child close quietly when the child is taken mid-rollback, whatever the replica shows', function (): void {
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);

    $child = SagaFlow::create(StolenRollbackWorkflow::class)->runSync();

    LaggingReplicaBuilder::$runningRuns = [$child->id];

    dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Cancelled, true));

    expect(driveReplayLagRun($child->id)->status)->toBe(FlowStatus::Failed);
});

it('unwinds every level although the replica shows its compensations still running', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.models.compensation_run', LaggingReplicaCompensationRun::class);
    LaggingReplicaBuilder::$unfinishedCompensations = true;

    $run = SagaFlow::create(SagaRollbackWorkflow::class)->run();
    drainQueue();

    $run = driveReplayLagRun($run->id);

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a'])
        ->and($run->exception)->not->toHaveKey('compensation');
});

it('lands a queued rollback although the replica shows the run a status behind', function (): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);

    $run = SagaFlow::create(SagaRollbackWorkflow::class)->run();

    LaggingReplicaBuilder::$runningRuns = [$run->id];

    drainQueue();

    expect(driveReplayLagRun($run->id)->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:b', 'undo:a']);
});

it('reads nothing of a child close and its rollback from the replica', function (): void {
    $child = SagaFlow::create(ManualCompensateWorkflow::class)->runSync();
    CompensationLog::reset();

    $connection = DB::connection();
    $connection->setReadPdo(new PDO('sqlite::memory:'));

    try {
        dispatch_sync(new CancelChildWorkflowJob($child->id, FlowStatus::Failed, true));
    } finally {
        $connection->setReadPdo(null);
    }

    expect(driveReplayLagRun($child->id)->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:a']);
});

it('runs the jobs of a run the replica does not hold yet', function (string $workflow, array $undo): void {
    useDatabaseQueue();
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);

    $run = SagaFlow::create($workflow)->run();

    LaggingReplicaBuilder::$missingRuns = [$run->id];

    drainQueue();

    expect(driveReplayLagRun($run->id)->status)->toBe($undo === [] ? FlowStatus::Completed : FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe($undo);
})->with([
    'a step' => [DriveReplayLagWorkflow::class, []],
    'a parallel block' => [ParallelEchoWorkflow::class, []],
    'a rollback' => [SagaRollbackWorkflow::class, ['undo:b', 'undo:a']],
]);

it('keeps reading from the writer when a tenancy hook reconnects mid-pass', function (): void {
    $pending = SagaFlow::create(DriveReplayLagWorkflow::class);

    // The step's own tenancy bracket reconnects; nothing after the pass does.
    $restores = 0;

    config()->set('saga-lara-flow.tenancy.auto', true);
    config()->set('saga-lara-flow.tenancy.capture', fn (): array => []);
    config()->set('saga-lara-flow.tenancy.restore', function () use (&$restores): void {
        if (++$restores === 2) {
            DB::purge('testing');
        }
    });

    // A replica that is not there: any read routed to it fails.
    config()->set('database.connections.testing.read', ['database' => 'saga_replica_that_does_not_exist']);
    config()->set('database.connections.testing.write', []);
    DB::purge('testing');

    try {
        $run = $pending->runSync();

        expect($run->status)->toBe(FlowStatus::Completed)
            ->and($restores)->toBeGreaterThan(2)
            ->and(readsFromWriter(DB::connection('testing')))->toBeFalse();
    } finally {
        config()->set('database.connections.testing.read', null);
        config()->set('database.connections.testing.write', null);
        DB::purge('testing');
    }
})->skip(fn () => TestCase::driver() === 'sqlite', 'Reconnecting an in-memory SQLite database empties it.');

it('records why a child failed although the replica does not hold its parent yet', function (): void {
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);

    $parent = SagaFlow::create(ParentFailPolicyWorkflow::class)->runSync();
    $child = $parent->children()->sole()->child_flow_run_id;

    LaggingReplicaBuilder::$missingRuns = [$parent->id];

    dispatch_sync(new CancelChildWorkflowJob($child, FlowStatus::Failed, true));

    expect(driveReplayLagRun($child)->status)->toBe(FlowStatus::Failed)
        ->and(driveReplayLagRun($child)->exception['class'] ?? null)->toBe(ParentClosedException::class);
});
