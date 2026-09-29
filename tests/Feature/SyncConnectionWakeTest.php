<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowQueue;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunActionJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ChildEchoWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FailedStepWithCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaBuilder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaFlowChild;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\LaggingReplicaFlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParallelEchoWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\RaceOnTransition;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UngivableUpActionRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WaitingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;

/**
 * A run woken by work it started: a step, a parallel block or a child. On a sync queue
 * connection that work runs inside the pass that sent it, and the pass replays instead
 * of waiting for a resume. On any connection a child can end before its parent has
 * written Waiting, and the parent reads it again once it has.
 */
beforeEach(function (): void {
    RaceOnTransition::reset();
    LaggingReplicaBuilder::reset();
    CompensationLog::reset();
    SyncWakeParentWorkflow::$child = ChildEchoWorkflow::class;
});

#[FlowQueue(connection: 'sync')]
final class SyncConnectionEchoChildWorkflow extends Workflow
{
    /**
     * @return array{label: string}
     */
    public function handle(string $label): array
    {
        return $this->action(MakeValueAction::class, $label)->run();
    }
}

final class SyncWakeParentWorkflow extends Workflow
{
    public static string $child = ChildEchoWorkflow::class;

    /**
     * @return array{child: mixed}
     */
    public function handle(): array
    {
        return ['child' => $this->child(self::$child, ['x'])->run()];
    }
}

final class SyncWakeTwoStepsWorkflow extends Workflow
{
    /**
     * @return array{steps: list<mixed>}
     */
    public function handle(): array
    {
        return ['steps' => [
            $this->action(MakeValueAction::class, 'a')->run(),
            $this->action(MakeValueAction::class, 'b')->run(),
        ]];
    }
}

final class SyncWakeStepThenChildWorkflow extends Workflow
{
    /**
     * @return array{child: mixed}
     */
    public function handle(): array
    {
        $this->action(MakeValueAction::class, 'a')->run();

        return ['child' => $this->child(ChildEchoWorkflow::class, ['x'])->run()];
    }
}

final class SyncWakeStepThenParallelWorkflow extends Workflow
{
    /**
     * @return array{results: list<mixed>}
     */
    public function handle(): array
    {
        $this->action(MakeValueAction::class, 'a')->run();

        return ['results' => $this->parallel()
            ->action(MakeValueAction::class, 'b')
            ->action(MakeValueAction::class, 'c')
            ->run()];
    }
}

final class SyncWakeStepThenSignalWorkflow extends Workflow
{
    /**
     * @return array{step: mixed}
     */
    public function handle(): array
    {
        $step = $this->action(MakeValueAction::class, 'a')->run();

        $this->awaitSignal('go');

        return ['step' => $step];
    }
}

final class SyncWakeOptionalFailureWorkflow extends Workflow
{
    /**
     * @return array{step: mixed}
     */
    public function handle(): array
    {
        return ['step' => $this->action(ThrowingAction::class)->continueOnFailure()->fallbackValueOnFail('fallback')->run()];
    }
}

final class PaymentDeclined extends RuntimeException {}

final class DeclinePaymentAction extends Action
{
    public function handle(): never
    {
        throw new PaymentDeclined('declined');
    }
}

final class SyncWakeCatchesDeclineWorkflow extends Workflow
{
    /**
     * @return array{paid: bool}
     */
    public function handle(): array
    {
        try {
            $this->action(DeclinePaymentAction::class)->run();
        } catch (PaymentDeclined) {
            return ['paid' => false];
        }

        return ['paid' => true];
    }
}

final class SyncWakeRetriedFailureWorkflow extends Workflow
{
    /**
     * @return array{step: mixed}
     */
    public function handle(): array
    {
        return ['step' => $this->action(ThrowingAction::class)->retryOnSignal('again')->run()];
    }
}

#[FlowQueue(connection: 'database')]
final class SyncWakeDatabaseChildWorkflow extends Workflow
{
    /**
     * @return array{done: bool}
     */
    public function handle(): array
    {
        return ['done' => true];
    }
}

final class SyncWakeSignalThenChildWorkflow extends Workflow
{
    /**
     * @return array{child: mixed}
     */
    public function handle(): array
    {
        $this->awaitSignal('start');

        return ['child' => $this->child(SyncWakeDatabaseChildWorkflow::class)->run()];
    }
}

final class SignalOwnRunAction extends Action
{
    public function handle(string $flowRunId): string
    {
        SagaFlow::loadFlow($flowRunId)->signal('self');

        return 'sent';
    }
}

final class SyncWakeSignalsItselfWorkflow extends Workflow
{
    /**
     * @return array{sent: mixed}
     */
    public function handle(): array
    {
        $sent = $this->action(SignalOwnRunAction::class, $this->runtime->run()->id)->run();

        $this->awaitSignal('self');

        return ['sent' => $sent];
    }
}

/**
 * The database queue's tables and batching, with the default connection switched to sync
 * and job locks as the package ships them.
 */
function useSyncQueueConnection(bool $afterCommit = false, bool $locks = true): void
{
    useDatabaseQueue();

    config()->set('queue.default', 'sync');
    config()->set('saga-lara-flow.locks.enabled', $locks);
    config()->set('saga-lara-flow.queue.after_commit', $afterCommit);
}

/**
 * @return list<string>
 */
function flowEventTypesOf(string $flowRunId): array
{
    return DB::table('saga_flow_events')->where('flow_run_id', $flowRunId)->orderBy('id')->pluck('type')->all();
}

it('wakes a queued parent whose child ran on a sync connection', function (string $tree, bool $afterCommit): void {
    if ($tree === 'whole tree') {
        useSyncQueueConnection($afterCommit);
    } else {
        useDatabaseQueue();
        config()->set('saga-lara-flow.queue.after_commit', $afterCommit);
        SyncWakeParentWorkflow::$child = SyncConnectionEchoChildWorkflow::class;
    }

    $parent = SagaFlow::create(SyncWakeParentWorkflow::class)->run();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($parent->id);
    $link = FlowChild::query()->where('parent_flow_run_id', $parent->id)->sole();

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result['child'] ?? null)->toBe(['label' => 'x'])
        ->and($link->status)->toBe(ChildStatus::Completed)
        ->and(DB::table('jobs')->count())->toBe(0);
})->with([['whole tree'], ['child only']])->with([[false], [true]]);

it('runs a queued run to the end on a sync connection', function (string $workflow, bool $locks): void {
    useSyncQueueConnection(locks: $locks);

    $run = SagaFlow::create($workflow)->run();

    // Every step past the first is woken while a resume for the same run still holds
    // its lock; the sync queue drops a job its lock turns away rather than retrying it.
    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Completed);
})->with([
    [SyncWakeTwoStepsWorkflow::class],
    [SyncWakeStepThenChildWorkflow::class],
    [SyncWakeStepThenParallelWorkflow::class],
    [ParallelEchoWorkflow::class],
])->with([[true], [false]]);

it('replays the pass rather than resuming it from inside itself', function (string $workflow, array $events): void {
    useSyncQueueConnection(locks: false);

    $run = SagaFlow::create($workflow)->run();

    // One pass from start to end: no nested pass took the run over and finished it
    // underneath the one that sent the work.
    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(flowEventTypesOf($run->id))->toBe(['flow.started', ...$events, 'flow.completed']);
})->with([
    'two steps' => [SyncWakeTwoStepsWorkflow::class, [
        'action.scheduled', 'action.started', 'action.completed',
        'action.scheduled', 'action.started', 'action.completed',
    ]],
    'a child' => [SyncWakeParentWorkflow::class, ['child.started', 'child.completed']],
    'a parallel block' => [ParallelEchoWorkflow::class, [
        'action.scheduled', 'action.scheduled', 'action.scheduled',
        'action.started', 'action.completed',
        'action.started', 'action.completed',
        'action.started', 'action.completed',
    ]],
]);

it('still parks on a wait nothing in the pass can answer', function (): void {
    useSyncQueueConnection();

    $run = SagaFlow::create(SyncWakeStepThenSignalWorkflow::class)->run();

    // The step's wake is spent on the replay that resolves it, not carried to the wait.
    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Waiting);

    SagaFlow::loadFlow($run->id)->signal('go');

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Completed);
});

it('resolves a step that failed on a sync connection as a queue would', function (bool $locks): void {
    useSyncQueueConnection(locks: $locks);

    $compensated = SagaFlow::create(FailedStepWithCompensationWorkflow::class)->run();
    $optional = SagaFlow::create(SyncWakeOptionalFailureWorkflow::class)->run();
    $retried = SagaFlow::create(SyncWakeRetriedFailureWorkflow::class)->run();

    // The sync queue records the failure through the job's failed() and then rethrows it
    // out of the dispatch; the pass resolves what was recorded rather than the throw.
    $compensated = FlowRun::query()->findOrFail($compensated->id);
    $optional = FlowRun::query()->findOrFail($optional->id);
    $retried = FlowRun::query()->findOrFail($retried->id);

    expect($compensated->status)->toBe(FlowStatus::Failed)
        ->and($compensated->exception['class'] ?? null)->toBe(ActionFailedException::class)
        ->and(CompensationLog::all())->toBe(['undo:a'])
        ->and($optional->status)->toBe(FlowStatus::Completed)
        ->and($optional->result['step'] ?? null)->toBe('fallback')
        ->and($retried->status)->toBe(FlowStatus::Waiting)
        ->and(ActionRun::query()->where('flow_run_id', $retried->id)->sole()->status)
        ->toBe(ActionStatus::AwaitingRetry);

    // The retry sends the step's job again, onto the same connection, and it fails again.
    SagaFlow::loadFlow($retried->id)->signal('again');

    $step = ActionRun::query()->where('flow_run_id', $retried->id)->sole();

    expect(FlowRun::query()->findOrFail($retried->id)->status)->toBe(FlowStatus::Waiting)
        ->and($step->status)->toBe(ActionStatus::AwaitingRetry)
        ->and($step->retry_signal_attempts)->toBe(1);
})->with([[true], [false]]);

it('hands the workflow the failure a queue would, not the one the sync queue rethrows', function (string $connection): void {
    $connection === 'database' ? useDatabaseQueue() : useSyncQueueConnection();

    $run = SagaFlow::create(SyncWakeCatchesDeclineWorkflow::class)->run();
    drainQueue();

    // A replay raises ActionFailedException at the step; the action's own exception is
    // not what the workflow sees, on either connection.
    $run = FlowRun::query()->findOrFail($run->id);

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(ActionFailedException::class);
})->with([['database'], ['sync']]);

it('fails the run on a sync job that broke before its step ran', function (): void {
    useSyncQueueConnection();

    Event::listen(JobProcessing::class, function (JobProcessing $event): void {
        if ($event->job->resolveName() === RunActionJob::class) {
            throw new RuntimeException('the queue broke first');
        }
    });

    $run = SagaFlow::create(SyncWakeTwoStepsWorkflow::class)->run();

    // Nothing is on record for the step to be resolved from, so the throw is the answer
    // rather than a replay that would park on a step no job is left to run.
    $run = FlowRun::query()->findOrFail($run->id);

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['message'] ?? null)->toBe('the queue broke first');
});

it('fails the run on a sync job whose failure was not fully recorded', function (string $workflow): void {
    useSyncQueueConnection();

    // The step has failed on its row, but the hook that settles it never runs.
    Event::listen(JobExceptionOccurred::class, function (JobExceptionOccurred $event): void {
        if ($event->job->resolveName() === RunActionJob::class) {
            throw new RuntimeException('a listener broke the failure hook');
        }
    });

    $run = SagaFlow::create($workflow)->run();

    $run = FlowRun::query()->findOrFail($run->id);

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['message'] ?? null)->toBe('a listener broke the failure hook');
})->with([[SyncWakeRetriedFailureWorkflow::class], [SyncWakeOptionalFailureWorkflow::class]]);

it('fails the run on an optional step the failure hook could not give up', function (): void {
    useSyncQueueConnection();
    config()->set('saga-lara-flow.models.action_run', UngivableUpActionRun::class);

    $run = SagaFlow::create(SyncWakeOptionalFailureWorkflow::class)->run();

    // The attempts are recorded as spent, the give-up is not: a replay would wait for an
    // OptionalFailed nothing is left to write.
    $run = FlowRun::query()->findOrFail($run->id);
    $step = ActionRun::query()->where('flow_run_id', $run->id)->sole();

    expect($step->status)->toBe(ActionStatus::Failed)
        ->and($step->queue_attempts_exhausted)->toBeTrue()
        ->and($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['message'] ?? null)->toBe('the give-up could not be written');
});

it('lets a step on a sync connection signal its own run', function (bool $locks): void {
    useSyncQueueConnection(locks: $locks);

    $run = SagaFlow::create(SyncWakeSignalsItselfWorkflow::class)->run();

    $run = FlowRun::query()->findOrFail($run->id);

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['sent'] ?? null)->toBe('sent');
})->with([[true], [false]]);

it('wakes a parent on a sync connection whose child ended on a worker before it wrote Waiting', function (): void {
    useSyncQueueConnection();
    app()->bind(StateMachine::class, RaceOnTransition::class);

    $workDatabaseQueue = fn (array $options) => Artisan::call('queue:work', [
        'connection' => 'database', '--sleep' => 0, '--memory' => 4096, ...$options,
    ]);

    $parent = SagaFlow::create(SyncWakeSignalThenChildWorkflow::class)->run();

    // The pass that starts the child is a resume, so a resume sent from inside it would
    // meet that pass's own lock.
    RaceOnTransition::$on = FlowStatus::Waiting;
    RaceOnTransition::$race = fn () => $workDatabaseQueue(['--once' => true]);

    SagaFlow::loadFlow($parent->id)->signal('start');
    $workDatabaseQueue(['--stop-when-empty' => true]);

    $parent = FlowRun::query()->findOrFail($parent->id);

    expect(RaceOnTransition::$races)->toBe(1)
        ->and($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result['child'] ?? null)->toBe(['done' => true]);
});

it('wakes a parent whose child ended just before it wrote Waiting', function (string $moment): void {
    useDatabaseQueue();
    app()->bind(StateMachine::class, RaceOnTransition::class);
    SyncWakeParentWorkflow::$child = WaitingChildWorkflow::class;

    $parent = SagaFlow::create(SyncWakeParentWorkflow::class)->run();

    if ($moment === 'while the child is still in flight') {
        drainQueue();

        // The parent is back on its child for another reason — a doctor's wake, say.
        ResumeWorkflowJob::dispatch($parent->id);
    }

    $child = fn (): FlowRun => FlowRun::query()->where('parent_id', $parent->id)->sole();

    // Another worker finishes the child after the parent last read it and before the
    // parent's Waiting is on record: the child finds the parent still Running.
    RaceOnTransition::$on = FlowStatus::Waiting;
    RaceOnTransition::$race = function () use ($child): void {
        if ($child()->status === FlowStatus::Pending) {
            workOneJob();
        }

        SagaFlow::loadFlow($child()->id)->signal('child.go');
        workOneJob();
    };

    drainQueue();

    $parent = FlowRun::query()->findOrFail($parent->id);

    expect(RaceOnTransition::$races)->toBe(1)
        ->and($child()->status)->toBe(FlowStatus::Completed)
        ->and($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result['child'] ?? null)->toBe(['done' => true]);
})->with([['on the child\'s first start'], ['while the child is still in flight']]);

it('reads the child from the writer once the parent is Waiting', function (): void {
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);
    useDatabaseQueue();
    app()->bind(StateMachine::class, RaceOnTransition::class);
    SyncWakeParentWorkflow::$child = WaitingChildWorkflow::class;

    // Whatever runs next reads a replica that has caught up.
    Queue::before(fn () => LaggingReplicaBuilder::reset());

    $parent = SagaFlow::create(SyncWakeParentWorkflow::class)->run();

    $child = fn (): FlowRun => FlowRun::query()->where('parent_id', $parent->id)->sole();

    RaceOnTransition::$on = FlowStatus::Waiting;
    RaceOnTransition::$race = function () use ($child): void {
        workOneJob();
        SagaFlow::loadFlow($child()->id)->signal('child.go');
        workOneJob();

        // The child has ended on the writer; the replica has not seen it yet.
        LaggingReplicaBuilder::$runningRuns = [$child()->id];
    };

    drainQueue();

    expect(RaceOnTransition::$races)->toBe(1)
        ->and(FlowRun::query()->findOrFail($parent->id)->status)->toBe(FlowStatus::Completed);
});

it('reads its link and its parent from the writer when a child ends', function (string $lag): void {
    config()->set('saga-lara-flow.models.flow_run', LaggingReplicaFlowRun::class);
    config()->set('saga-lara-flow.models.flow_child', LaggingReplicaFlowChild::class);
    useDatabaseQueue();
    SyncWakeParentWorkflow::$child = WaitingChildWorkflow::class;

    $parent = SagaFlow::create(SyncWakeParentWorkflow::class)->run();
    drainQueue();

    $child = FlowRun::query()->where('parent_id', $parent->id)->sole();

    expect(FlowRun::query()->findOrFail($parent->id)->status)->toBe(FlowStatus::Waiting);

    SagaFlow::loadFlow($child->id)->signal('child.go');

    // The replica has not seen the parent park, or the link being written.
    if ($lag === 'the parent still Running') {
        LaggingReplicaBuilder::$runningRuns = [$parent->id];
    } else {
        LaggingReplicaBuilder::$noLinks = true;
    }

    workOneJob();

    LaggingReplicaBuilder::reset();

    drainQueue();

    expect(FlowRun::query()->findOrFail($child->id)->status)->toBe(FlowStatus::Completed)
        ->and(FlowRun::query()->findOrFail($parent->id)->status)->toBe(FlowStatus::Completed);
})->with([['the parent still Running'], ['no link yet']]);
