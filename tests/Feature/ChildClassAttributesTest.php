<?php

use DiscoveryUkraine\SagaLaraFlow\Attributes\Flow;
use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowQueue;
use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowTimeout;
use DiscoveryUkraine\SagaLaraFlow\Attributes\Tag;
use DiscoveryUkraine\SagaLaraFlow\Builders\CreateWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildWorkflowManager;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UnwritableFlowTag;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WaitingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Queue;

/**
 * A child run resolves its class the way a root run does: the same #[Tag], #[Flow],
 * #[FlowTimeout] and configured default deadline. #[FlowQueue] resolves field by field
 * with the parent where config stands for a root. Each case starts the same class both
 * ways and holds them to one answer, or to the one difference the parent makes.
 */
beforeEach(function (): void {
    CompensationLog::reset();
});

#[Flow(name: 'ledger.parent', version: 'p1')]
#[Tag('parent-only', 'yes')]
final class ParentOfAttributedChildWorkflow extends Workflow
{
    public function handle(string $child, ?string $deadline = null): mixed
    {
        $builder = $this->child($child);

        if ($deadline !== null) {
            $builder->expiresAt(Carbon::parse($deadline));
        }

        return $builder->run();
    }
}

#[Flow(name: 'ledger.child', version: 'v3')]
#[FlowTimeout(seconds: 600)]
#[Tag('ledger')]
#[Tag('team', 'payments')]
#[FlowQueue(connection: 'primary', queue: 'ledger')]
final class FullyAttributedChildWorkflow extends Workflow
{
    public function handle(): string
    {
        return 'done';
    }
}

#[FlowQueue(queue: 'heavy')]
final class QueueOnlyChildWorkflow extends Workflow
{
    public function handle(): string
    {
        return 'done';
    }
}

#[FlowQueue(connection: 'primary')]
final class ConnectionOnlyChildWorkflow extends Workflow
{
    public function handle(): string
    {
        return 'done';
    }
}

final class BareChildWorkflow extends Workflow
{
    public function handle(): string
    {
        return 'done';
    }
}

final class ContinuePastInlineExpiredChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(WaitingChildWorkflow::class)
            ->expiresAt(Carbon::parse('2020-01-01 00:00:00'))
            ->continueParentOnFailure()
            ->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
    }
}

final class StopAtInlineExpiredChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->child(WaitingChildWorkflow::class)
            ->expiresAt(Carbon::parse('2020-01-01 00:00:00'))
            ->run();
        $this->action(MakeValueAction::class, 'b')->compensateWith(UndoAction::class, 'b')->run();
    }
}

/**
 * The run of $class, started as a root, or as the child of a parent the caller may
 * configure — both inline, so nothing is dispatched and only the rows are compared.
 *
 * @param  (callable(CreateWorkflowBuilder): mixed)|null  $parent
 */
function runStartedAs(string $as, string $class, ?callable $parent = null): FlowRun
{
    if ($as === 'root') {
        return SagaFlow::create($class)->runSync();
    }

    $builder = SagaFlow::create(ParentOfAttributedChildWorkflow::class)->withArguments($class);

    if ($parent !== null) {
        $parent($builder);
    }

    $run = $builder->runSync();

    expect($run->status)->toBe(FlowStatus::Completed);

    return FlowRun::query()->where('parent_id', $run->id)->firstOrFail();
}

/**
 * @return array<string, ?string>
 */
function tagsOf(FlowRun $run): array
{
    return $run->tags()->pluck('value', 'key')->all();
}

it('writes the tags the class declares', function (string $as): void {
    $run = runStartedAs($as, FullyAttributedChildWorkflow::class);

    expect(tagsOf($run))->toEqualCanonicalizing(['ledger' => null, 'team' => 'payments']);
})->with(['root', 'child']);

it('names and versions a run from its own #[Flow]', function (string $as): void {
    $run = runStartedAs($as, FullyAttributedChildWorkflow::class);

    expect($run->workflow_name)->toBe('ledger.child')
        ->and($run->workflow_version)->toBe('v3');
})->with(['root', 'child']);

it('leaves a class with no #[Flow] unnamed, whatever its parent is called', function (string $as): void {
    $run = runStartedAs($as, BareChildWorkflow::class);

    expect($run->workflow_name)->toBeNull()
        ->and($run->workflow_version)->toBeNull()
        ->and(tagsOf($run))->toBe([]);
})->with(['root', 'child']);

it('gives a run the deadline its #[FlowTimeout] declares', function (string $as): void {
    $this->freezeSecond();
    config()->set('saga-lara-flow.monitor.expiration.defaults.run', 900);

    $run = runStartedAs($as, FullyAttributedChildWorkflow::class);

    expect($run->expires_at?->getTimestamp())->toBe(now()->addSeconds(600)->getTimestamp());
})->with(['root', 'child']);

it('falls back to the configured default deadline', function (string $as): void {
    $this->freezeSecond();
    config()->set('saga-lara-flow.monitor.expiration.defaults.run', 900);

    $run = runStartedAs($as, BareChildWorkflow::class);

    expect($run->expires_at?->getTimestamp())->toBe(now()->addSeconds(900)->getTimestamp());
})->with(['root', 'child']);

it('does not hand a child its parent\'s deadline', function (?int $default, ?int $expected): void {
    $this->freezeSecond();
    config()->set('saga-lara-flow.monitor.expiration.defaults.run', $default);

    $child = runStartedAs('child', BareChildWorkflow::class, function (CreateWorkflowBuilder $parent): void {
        $parent->expiresAt(now()->addHour());
    });

    expect($child->expires_at?->getTimestamp())
        ->toBe($expected === null ? null : now()->addSeconds($expected)->getTimestamp());
})->with([
    'no default configured' => [null, null],
    'a default configured' => [900, 900],
]);

it('lets the child builder\'s deadline win over the class and the default', function (): void {
    config()->set('saga-lara-flow.monitor.expiration.defaults.run', 900);

    $parent = SagaFlow::create(ParentOfAttributedChildWorkflow::class)
        ->withArguments(FullyAttributedChildWorkflow::class, '2031-05-01 12:00:00')
        ->runSync();

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect($child->expires_at?->toDateTimeString())->toBe('2031-05-01 12:00:00');
});

it('resolves connection and queue field by field', function (
    string $as,
    string $class,
    array $expected,
): void {
    config()->set('saga-lara-flow.queue.connection', 'configured');
    config()->set('saga-lara-flow.queue.queue', 'configured-queue');

    // For a child, the parent stands where config stands for a root.
    $run = runStartedAs($as, $class, function (CreateWorkflowBuilder $parent): void {
        $parent->onConnection('secondary')->onQueue('parent-queue');
    });

    expect([$run->connection, $run->queue])->toBe($expected);
})->with([
    'both fields, root' => ['root', FullyAttributedChildWorkflow::class, ['primary', 'ledger']],
    'both fields, child' => ['child', FullyAttributedChildWorkflow::class, ['primary', 'ledger']],
    'queue only, root' => ['root', QueueOnlyChildWorkflow::class, ['configured', 'heavy']],
    'queue only, child' => ['child', QueueOnlyChildWorkflow::class, ['secondary', 'heavy']],
    'connection only, root' => ['root', ConnectionOnlyChildWorkflow::class, ['primary', 'configured-queue']],
    'connection only, child' => ['child', ConnectionOnlyChildWorkflow::class, ['primary', 'parent-queue']],
    'neither, root' => ['root', BareChildWorkflow::class, ['configured', 'configured-queue']],
    'neither, child' => ['child', BareChildWorkflow::class, ['secondary', 'parent-queue']],
]);

it('falls back to config for a field neither the class nor the parent names', function (): void {
    Queue::fake();

    // The parent is on record with no connection of its own; by the time it starts the
    // child, the configured connection has changed.
    $parent = SagaFlow::create(ParentOfAttributedChildWorkflow::class)
        ->withArguments(QueueOnlyChildWorkflow::class)
        ->run();

    expect($parent->connection)->toBeNull();

    config()->set('saga-lara-flow.queue.connection', 'configured');

    app(FlowExecutor::class)->drive($parent, RunMode::Queued);

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    expect([$child->connection, $child->queue])->toBe(['configured', 'heavy']);
});

it('sends the child\'s job where its own class routes it', function (): void {
    Queue::fake();

    $parent = SagaFlow::create(ParentOfAttributedChildWorkflow::class)
        ->withArguments(QueueOnlyChildWorkflow::class)
        ->onConnection('secondary')
        ->onQueue('parent-queue')
        ->run();

    app(FlowExecutor::class)->drive($parent, RunMode::Queued);

    $child = FlowRun::query()->where('parent_id', $parent->id)->firstOrFail();

    Queue::assertPushed(
        RunWorkflowJob::class,
        fn (RunWorkflowJob $job): bool => $job->flowRunId === $child->id
            && $job->connection === 'secondary'
            && $job->queue === 'heavy',
    );
});

it('closes a child from its parent\'s queue, not the child\'s own', function (): void {
    Queue::fake();

    $parent = SagaFlow::create(ParentOfAttributedChildWorkflow::class)
        ->withArguments(FullyAttributedChildWorkflow::class)
        ->onConnection('secondary')
        ->onQueue('parent-queue')
        ->run();

    app(FlowExecutor::class)->drive($parent, RunMode::Queued);

    $link = FlowChild::query()->where('parent_flow_run_id', $parent->id)->firstOrFail();
    $link->newQuery()->toBase()->where('id', $link->id)->update(['close_policy' => ChildClosePolicy::Cancel->value]);
    $parent->newQuery()->toBase()->where('id', $parent->id)->update(['status' => FlowStatus::Failed->value]);

    app(ChildWorkflowManager::class)->onFlowFinalized($parent->fresh(), true);

    // Closing is the parent's policy at work, and its workers are the ones known to be up.
    Queue::assertPushed(
        CancelChildWorkflowJob::class,
        fn (CancelChildWorkflowJob $job): bool => $job->childFlowRunId === $link->child_flow_run_id
            && $job->connection === 'secondary'
            && $job->queue === 'parent-queue',
    );
});

it('writes a child\'s tags with the child, or leaves no child at all', function (): void {
    Queue::fake();

    $parent = SagaFlow::create(ParentOfAttributedChildWorkflow::class)
        ->withArguments(FullyAttributedChildWorkflow::class)
        ->run();

    config()->set('saga-lara-flow.models.flow_tag', UnwritableFlowTag::class);

    $driven = app(FlowExecutor::class)->drive($parent, RunMode::Queued);

    // The tags share the transaction of the child run and its link, so a tag that cannot
    // be written takes the child down with it rather than leaving it on record untagged.
    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and(FlowRun::query()->where('parent_id', $parent->id)->count())->toBe(0)
        ->and(FlowChild::query()->where('parent_flow_run_id', $parent->id)->count())->toBe(0);
});

it('carries a sync parent past a child that expired inline, when told to survive it', function (): void {
    $run = SagaFlow::create(ContinuePastInlineExpiredChildWorkflow::class)->runSync();

    $child = FlowRun::query()->where('parent_id', $run->id)->firstOrFail();
    $link = FlowChild::query()->where('child_flow_run_id', $child->id)->firstOrFail();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($child->status)->toBe(FlowStatus::Expired)
        ->and($link->status)->toBe(ChildStatus::Expired)
        ->and($run->actions()->where('status', 'completed')->pluck('sequence')->sort()->values()->all())
        ->toBe([0, 2])
        ->and(CompensationLog::all())->toBe([]);
});

it('fails a sync parent whose child expired inline, and rolls back what it did', function (): void {
    $run = SagaFlow::create(StopAtInlineExpiredChildWorkflow::class)->runSync();

    $child = FlowRun::query()->where('parent_id', $run->id)->firstOrFail();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->exception['class'] ?? null)->toBe(ChildWorkflowExpiredException::class)
        ->and($child->status)->toBe(FlowStatus::Expired)
        ->and($run->actions()->where('sequence', 2)->exists())->toBeFalse()
        ->and(CompensationLog::all())->toBe(['undo:a']);
});
