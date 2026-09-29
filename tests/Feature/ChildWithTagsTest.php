<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Attributes\Tag;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UnwritableFlowTag;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WaitingChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\Queue;

/**
 * withTags() on child(): the parent tags the child it starts. The tags are written with
 * the child run, merge over the child class's own #[Tag], and are not written again once
 * the child exists — a retried attempt is a new run and takes them from its own replay.
 */
beforeEach(function (): void {
    TaggingParentWorkflow::reset();
    TaggedFlakyChildWorkflow::$failures = 0;
    TaggedAttemptAction::$executions = 0;
});

final class TaggedBareChildWorkflow extends Workflow
{
    public function handle(): string
    {
        return 'done';
    }
}

#[Tag('team', 'payments')]
#[Tag('customer', 'from-class')]
final class TaggedByClassChildWorkflow extends Workflow
{
    public function handle(): string
    {
        return 'done';
    }
}

final class TaggedSelfChildWorkflow extends Workflow
{
    public function handle(): string
    {
        $this->tag('customer', 'from-child');

        return 'done';
    }
}

final class TaggedAttemptAction extends Action
{
    public static int $executions = 0;

    public function handle(): int
    {
        return ++self::$executions;
    }
}

final class TaggedFlakyChildWorkflow extends Workflow
{
    public static int $failures = 0;

    public function handle(): string
    {
        $attempt = $this->action(TaggedAttemptAction::class)->run();

        if ($attempt <= self::$failures) {
            $this->tag('failed-attempt', $attempt);

            throw new RuntimeException("outage on attempt {$attempt}");
        }

        return "attempt:{$attempt}";
    }
}

final class TaggingParentWorkflow extends Workflow
{
    public static string $child = TaggedBareChildWorkflow::class;

    /** @var list<array<array-key, string|int|null>> */
    public static array $calls = [];

    public static bool $retry = false;

    public static function reset(): void
    {
        self::$child = TaggedBareChildWorkflow::class;
        self::$calls = [];
        self::$retry = false;
    }

    public function handle(): mixed
    {
        $child = $this->child(self::$child);

        foreach (self::$calls as $tags) {
            $child->withTags($tags);
        }

        if (self::$retry) {
            $child->retryOnSignal('child.retry');
        }

        return $child->run();
    }
}

function startTaggingParent(string $mode): FlowRun
{
    if ($mode === 'sync') {
        // Driven again by hand, so the inline path stays under test.
        config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);

        $run = SagaFlow::create(TaggingParentWorkflow::class)->withTags(['parent-only' => 'yes'])->runSync();

        return FlowRun::query()->findOrFail($run->id);
    }

    useDatabaseQueue();

    $run = SagaFlow::create(TaggingParentWorkflow::class)->withTags(['parent-only' => 'yes'])->run();
    drainQueue();

    return FlowRun::query()->findOrFail($run->id);
}

/**
 * Deliver $signal to $run and let whatever it wakes run to rest.
 */
function signalTaggedRun(FlowRun $run, string $signal, string $mode): void
{
    SagaFlow::loadFlow($run->id)->signal($signal);

    if ($mode === 'queued') {
        drainQueue();

        return;
    }

    $executor = app(FlowExecutor::class);
    $executor->drive(FlowRun::query()->findOrFail($run->id), RunMode::Sync);

    if ($run->parent_id !== null) {
        $executor->drive(FlowRun::query()->findOrFail($run->parent_id), RunMode::Sync);
    }
}

/**
 * The run's tags as "key=value", or a bare "key" for a tag with no value, sorted.
 *
 * @return list<string>
 */
function tagLinesOf(FlowRun $run): array
{
    $lines = $run->tags()->get()
        ->map(fn ($tag): string => $tag->value === null ? $tag->key : "{$tag->key}={$tag->value}")
        ->all();

    sort($lines);

    return $lines;
}

/**
 * @return list<FlowRun>
 */
function childRunsOf(FlowRun $parent): array
{
    return FlowRun::query()->where('parent_id', $parent->id)->orderBy('created_at')->orderBy('id')->get()->all();
}

it('writes the tags the parent gives the child', function (string $mode): void {
    TaggingParentWorkflow::$calls = [['customer' => 42, 'order' => 'A-1', 'priority' => null]];

    $parent = startTaggingParent($mode);
    [$child] = childRunsOf($parent);

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and(tagLinesOf($child))->toBe(['customer=42', 'order=A-1', 'priority']);
})->with([['sync'], ['queued']]);

it('does not hand the child the parent\'s own tags', function (string $mode): void {
    TaggingParentWorkflow::$calls = [['customer' => 42]];

    $parent = startTaggingParent($mode);
    [$child] = childRunsOf($parent);

    expect(tagLinesOf($parent))->toBe(['parent-only=yes'])
        ->and(tagLinesOf($child))->toBe(['customer=42']);
})->with([['sync'], ['queued']]);

it('merges repeated calls, and a later key wins', function (): void {
    TaggingParentWorkflow::$calls = [['customer' => 1, 'order' => 'A-1'], ['customer' => 2, 'lane' => null]];

    [$child] = childRunsOf(startTaggingParent('sync'));

    expect(tagLinesOf($child))->toBe(['customer=2', 'lane', 'order=A-1']);
});

it('keeps a numeric tag name as written', function (): void {
    TaggingParentWorkflow::$calls = [['2024' => 'a', '7' => 'x'], ['2024' => 'b']];

    [$child] = childRunsOf(startTaggingParent('sync'));

    expect(tagLinesOf($child))->toBe(['2024=b', '7=x']);
});

it('keeps a numeric tag name as written on a root run too', function (): void {
    $run = SagaFlow::create(TaggedBareChildWorkflow::class)
        ->withTags(['2024' => 'a', 'k' => 'x'])
        ->withTags(['2024' => 'b', '7' => null])
        ->runSync();

    expect(tagLinesOf($run))->toBe(['2024=b', '7', 'k=x']);
});

it('keeps a numeric tag name as written through a handle, and overwrites it', function (): void {
    $run = SagaFlow::create(TaggedBareChildWorkflow::class)->runSync();

    // PHP hands the key over as an int; PostgreSQL refuses to compare one with the key column.
    SagaFlow::loadFlow($run->id)->withTags(['2024' => 'a', '7' => 1]);
    SagaFlow::loadFlow($run->id)->withTags(['2024' => 'b']);

    expect(tagLinesOf($run))->toBe(['2024=b', '7=1']);
});

it('wins over the child class\'s #[Tag] on the same name and keeps the rest', function (string $mode): void {
    TaggingParentWorkflow::$child = TaggedByClassChildWorkflow::class;
    TaggingParentWorkflow::$calls = [['customer' => 42]];

    [$child] = childRunsOf(startTaggingParent($mode));

    expect(tagLinesOf($child))->toBe(['customer=42', 'team=payments']);
})->with([['sync'], ['queued']]);

it('yields to a tag the child writes itself later', function (string $mode): void {
    TaggingParentWorkflow::$child = TaggedSelfChildWorkflow::class;
    TaggingParentWorkflow::$calls = [['customer' => 42]];

    [$child] = childRunsOf(startTaggingParent($mode));

    expect(tagLinesOf($child))->toBe(['customer=from-child']);
})->with([['sync'], ['queued']]);

it('writes the tags with the child, or leaves no child at all', function (): void {
    Queue::fake();
    TaggingParentWorkflow::$calls = [['customer' => 42]];

    $parent = SagaFlow::create(TaggingParentWorkflow::class)->run();

    config()->set('saga-lara-flow.models.flow_tag', UnwritableFlowTag::class);

    $driven = app(FlowExecutor::class)->drive($parent, RunMode::Queued);

    expect($driven->status)->toBe(FlowStatus::Failed)
        ->and($driven->exception['message'] ?? null)->toBe('flow tag could not be written')
        ->and(FlowRun::query()->where('parent_id', $parent->id)->count())->toBe(0)
        ->and(FlowChild::query()->where('parent_flow_run_id', $parent->id)->count())->toBe(0);
});

it('does not write the tags again once the child exists', function (string $mode): void {
    TaggingParentWorkflow::$child = WaitingChildWorkflow::class;
    TaggingParentWorkflow::$calls = [['release' => 'v1']];

    $parent = startTaggingParent($mode);
    [$child] = childRunsOf($parent);

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and(tagLinesOf($child))->toBe(['release=v1']);

    // A deploy changes what the builder is given; the parent replays past the child.
    TaggingParentWorkflow::$calls = [['release' => 'v2', 'added' => 'later']];
    signalTaggedRun($child, 'child.go', $mode);

    expect(FlowRun::query()->findOrFail($parent->id)->status)->toBe(FlowStatus::Completed)
        ->and(tagLinesOf($child))->toBe(['release=v1']);
})->with([['sync'], ['queued']]);

it('tags a retried attempt from the replay that starts it', function (string $mode): void {
    TaggingParentWorkflow::$child = TaggedFlakyChildWorkflow::class;
    TaggingParentWorkflow::$retry = true;
    TaggingParentWorkflow::$calls = [['release' => 'v1']];
    TaggedFlakyChildWorkflow::$failures = 1;

    $parent = startTaggingParent($mode);

    expect($parent->status)->toBe(FlowStatus::Waiting);

    TaggingParentWorkflow::$calls = [['release' => 'v2']];
    signalTaggedRun($parent, 'child.retry', $mode);

    [$first, $second] = childRunsOf($parent);

    // The new attempt carries what the builder gives it now, and nothing the failed
    // attempt wrote on its own run.
    expect(FlowRun::query()->findOrFail($parent->id)->status)->toBe(FlowStatus::Completed)
        ->and($second->status)->toBe(FlowStatus::Completed)
        ->and(tagLinesOf($first))->toBe(['failed-attempt=1', 'release=v1'])
        ->and(tagLinesOf($second))->toBe(['release=v2']);
})->with([['sync'], ['queued']]);
