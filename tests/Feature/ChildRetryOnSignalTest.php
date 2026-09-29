<?php

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowAwaitingRetry;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowRetried;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowCancelledException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryPolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalRecorder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Log;

/**
 * retryOnSignal() on a child: a child that fails or expires parks its parent on a signal
 * instead of handing it the failure, and the signal starts the child again as a new run at
 * the same ordinal. Most scenes run in both modes and must leave the same rows behind.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    ChildAttemptAction::$executions = 0;
    RetriedChildWorkflow::$failures = 0;
    RetryingParentWorkflow::reset();
    ParentPassingFailedChildWorkflow::$retry = false;
    ParentEditedAtParkedChildWorkflow::$edit = null;
    ParentOfExpiringChildWorkflow::$waitSeconds = null;
});

final class RetriedChildOutage extends RuntimeException {}

final class ChildAttemptAction extends Action
{
    public static int $executions = 0;

    public function handle(): int
    {
        return ++self::$executions;
    }
}

final class RetriedChildWorkflow extends Workflow
{
    public static int $failures = 0;

    public function handle(string $order): string
    {
        $this->action(MakeValueAction::class, 'c')->compensateWith(UndoAction::class, 'c')->run();

        $attempt = $this->action(ChildAttemptAction::class)->run();

        if ($attempt <= self::$failures) {
            throw new RetriedChildOutage("outage on attempt {$attempt}");
        }

        return "{$order}:{$attempt}";
    }
}

final class RetryingParentWorkflow extends Workflow
{
    public static RetryPolicy|string $signal = 'child.retry';

    public static ?int $maxRetries = null;

    public static ?int $waitSeconds = null;

    /** @var list<class-string<Throwable>>|null */
    public static ?array $only = null;

    public static ?Closure $when = null;

    public static bool $continue = false;

    public static bool $compensate = false;

    public static bool $failAfter = false;

    public static bool $waitAfter = false;

    public static string $order = 'order';

    public static function reset(): void
    {
        self::$signal = 'child.retry';
        self::$maxRetries = null;
        self::$waitSeconds = null;
        self::$only = null;
        self::$when = null;
        self::$continue = false;
        self::$compensate = false;
        self::$failAfter = false;
        self::$waitAfter = false;
        self::$order = 'order';
    }

    public function handle(): string
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();

        $child = $this->child(RetriedChildWorkflow::class, [self::$order])->continueParentOnFailure(self::$continue);

        $child = self::$signal instanceof RetryPolicy
            ? $child->retryOnSignal(self::$signal)
            : $child->retryOnSignal(
                self::$signal,
                maxRetries: self::$maxRetries,
                waitSeconds: self::$waitSeconds,
                only: self::$only,
                when: self::$when,
            );

        if (self::$compensate) {
            $child->compensateWith(UndoAction::class, 'child');
        }

        $result = $child->run();

        if (self::$failAfter) {
            $this->action(ThrowingAction::class)->run();
        }

        if (self::$waitAfter) {
            $this->awaitSignal('after');
        }

        return 'parent:'.($result ?? 'none');
    }
}

final class ParentPassingFailedChildWorkflow extends Workflow
{
    public static bool $retry = false;

    public function handle(): void
    {
        $child = $this->child(RetriedChildWorkflow::class, ['order'])->continueParentOnFailure();

        if (self::$retry) {
            $child->retryOnSignal('child.retry');
        }

        $child->run();

        $this->action(MakeValueAction::class, 'b')->run();
        $this->awaitSignal('go');
    }
}

final class ParentEditedAtParkedChildWorkflow extends Workflow
{
    public static ?string $edit = null;

    public function handle(): void
    {
        match (self::$edit) {
            'signal' => $this->awaitSignal('child.retry'),
            'action' => $this->action(MakeValueAction::class, 'x')->run(),
            'side effect' => $this->sideEffect('x', fn (): int => 1),
            default => $this->child(RetriedChildWorkflow::class, ['order'])->retryOnSignal('child.retry')->run(),
        };
    }
}

final class ExpiringRetriedChildWorkflow extends Workflow
{
    public function handle(): string
    {
        $attempt = $this->action(ChildAttemptAction::class)->run();

        if ($attempt === 1) {
            $this->awaitSignal('never');
        }

        return "attempt:{$attempt}";
    }
}

final class ParentOfExpiringChildWorkflow extends Workflow
{
    public static ?int $waitSeconds = null;

    public function handle(): string
    {
        return $this->child(ExpiringRetriedChildWorkflow::class)
            ->retryOnSignal('child.retry', waitSeconds: self::$waitSeconds, only: [FlowExpiredException::class])
            ->run();
    }
}

final class ParentOfCancellableChildWorkflow extends Workflow
{
    public function handle(): mixed
    {
        return $this->child(ExpiringRetriedChildWorkflow::class)->retryOnSignal('child.retry')->run();
    }
}

final class ChildOutagePolicy extends RetryPolicy
{
    public function signal(): string
    {
        return 'outage.over';
    }

    public function maxRetries(): ?int
    {
        return 3;
    }

    public function only(): ?array
    {
        return [RetriedChildOutage::class];
    }
}

function startRetryingParent(string $mode, string $workflow = RetryingParentWorkflow::class): FlowRun
{
    if ($mode === 'sync') {
        // Driven again by hand below, so the inline path stays under test.
        config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);

        return FlowRun::query()->findOrFail(SagaFlow::create($workflow)->runSync()->id);
    }

    useDatabaseQueue();

    $run = SagaFlow::create($workflow)->run();
    drainQueue();

    return FlowRun::query()->findOrFail($run->id);
}

function signalRetryingParent(FlowRun $parent, string $mode, string $signal = 'child.retry'): FlowRun
{
    SagaFlow::loadFlow($parent->id)->signal($signal);

    if ($mode === 'sync') {
        app(FlowExecutor::class)->drive(FlowRun::query()->findOrFail($parent->id), RunMode::Sync);
    } else {
        drainQueue();
    }

    return FlowRun::query()->findOrFail($parent->id);
}

function retryLinkOf(FlowRun $parent): FlowChild
{
    return FlowChild::query()->where('parent_flow_run_id', $parent->id)->firstOrFail();
}

/**
 * @return list<FlowRun>
 */
function attemptsOf(FlowRun $parent): array
{
    return FlowRun::query()->where('parent_id', $parent->id)->orderBy('created_at')->orderBy('id')->get()->all();
}

/**
 * @return list<string>
 */
function eventTypesOf(FlowRun $run): array
{
    return $run->events()->get()->map(fn (FlowEvent $event): string => $event->type->value)->all();
}

it('parks the parent on the child retry signal instead of handing it the failure', function (string $mode): void {
    RetriedChildWorkflow::$failures = 99;

    $parked = [];
    Event::listen(ChildWorkflowAwaitingRetry::class, function (ChildWorkflowAwaitingRetry $event) use (&$parked): void {
        $parked[] = [$event->childFlowRun->id, $event->signal];
    });

    $parent = startRetryingParent($mode);
    $link = retryLinkOf($parent);
    [$child] = attemptsOf($parent);

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and($child->status)->toBe(FlowStatus::Failed)
        ->and($child->exception['class'] ?? null)->toBe(RetriedChildOutage::class)
        ->and($link->status)->toBe(ChildStatus::AwaitingRetry)
        ->and($link->child_flow_run_id)->toBe($child->id)
        ->and($link->retry_signal)->toBe('child.retry')
        ->and($link->retry_signal_attempts)->toBe(0)
        ->and($link->retry_signal_max_attempts)->toBeNull()
        ->and($parked)->toBe([[$child->id, 'child.retry']]);

    $waits = $parent->signals()->get();

    expect($waits)->toHaveCount(1)
        ->and($waits[0]->status)->toBe(SignalStatus::Waiting)
        ->and($waits[0]->name)->toBe('child.retry')
        ->and($waits[0]->wait_sequence)->toBe($link->sequence);

    // The failed attempt rolled its own step back; the parent's step stays applied.
    expect(CompensationLog::all())->toBe(['undo:c'])
        ->and(eventTypesOf($parent))->toContain('child.failed', 'child.awaiting_retry');
})->with([['sync'], ['queued']]);

it('starts the child again at the same ordinal when the signal arrives', function (string $mode): void {
    RetriedChildWorkflow::$failures = 1;

    $retried = [];
    Event::listen(ChildWorkflowRetried::class, function (ChildWorkflowRetried $event) use (&$retried): void {
        $retried[] = [$event->previousChildFlowRun->id, $event->childFlowRun->id];
    });

    $parent = signalRetryingParent(startRetryingParent($mode), $mode);
    $link = retryLinkOf($parent);
    [$first, $second] = attemptsOf($parent);

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result)->toBe('parent:order:2')
        ->and($first->status)->toBe(FlowStatus::Failed)
        ->and($second->status)->toBe(FlowStatus::Completed)
        ->and($second->workflow_class)->toBe(RetriedChildWorkflow::class)
        ->and($link->child_flow_run_id)->toBe($second->id)
        ->and($link->status)->toBe(ChildStatus::Completed)
        ->and($link->retry_signal_attempts)->toBe(1)
        ->and($retried)->toBe([[$first->id, $second->id]])
        ->and(FlowChild::query()->count())->toBe(1)
        ->and($parent->signals()->pluck('status')->all())->toBe([SignalStatus::Consumed])
        ->and(CompensationLog::all())->toBe(['undo:c']);

    // The same history in both modes, whatever the parent's own waits and resumes were.
    expect(array_values(array_filter(
        eventTypesOf($parent),
        fn (string $type): bool => str_starts_with($type, 'child.') || str_starts_with($type, 'signal.'),
    )))->toBe([
        'child.started', 'child.failed', 'child.awaiting_retry', 'signal.received',
        'signal.consumed', 'child.retried', 'child.started', 'child.completed',
    ]);
})->with([['sync'], ['queued']]);

it('parks again when the retried child fails once more', function (string $mode): void {
    RetriedChildWorkflow::$failures = 2;

    $parent = signalRetryingParent(startRetryingParent($mode), $mode);

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::AwaitingRetry)
        ->and(retryLinkOf($parent)->retry_signal_attempts)->toBe(1)
        ->and($parent->signals()->pluck('status')->all())->toBe([SignalStatus::Consumed, SignalStatus::Waiting]);

    $parent = signalRetryingParent($parent, $mode);

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result)->toBe('parent:order:3')
        ->and(retryLinkOf($parent)->retry_signal_attempts)->toBe(2)
        ->and(array_map(fn (FlowRun $run) => $run->status, attemptsOf($parent)))
        ->toBe([FlowStatus::Failed, FlowStatus::Failed, FlowStatus::Completed]);
})->with([['sync'], ['queued']]);

it('hands the failure to the parent once the budget is spent', function (string $mode): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$maxRetries = 1;

    $parent = signalRetryingParent(startRetryingParent($mode), $mode);
    $link = retryLinkOf($parent);

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->exception['class'] ?? null)->toBe(ChildWorkflowFailedException::class)
        ->and($link->status)->toBe(ChildStatus::Failed)
        ->and($link->retry_signal_attempts)->toBe(1)
        ->and($link->retry_signal_max_attempts)->toBe(1)
        ->and(CompensationLog::all())->toBe(['undo:c', 'undo:c', 'undo:a']);
})->with([['sync'], ['queued']]);

it('hands the parent a failure outside only at once', function (string $mode): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$only = [LogicException::class];

    $parent = startRetryingParent($mode);

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->signals()->count())->toBe(0)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c', 'undo:a']);
})->with([['sync'], ['queued']]);

it('parks on a failure that matches only by subclass', function (): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$only = [RuntimeException::class];

    expect(startRetryingParent('sync')->status)->toBe(FlowStatus::Waiting);
});

it('judges the failure the child recorded, and tells the policy which attempt it was', function (): void {
    RetriedChildWorkflow::$failures = 99;

    $asked = [];
    RetryingParentWorkflow::$maxRetries = 5;
    RetryingParentWorkflow::$when = function (RetryContext $context) use (&$asked): bool {
        $asked[] = $context;

        return true;
    };

    $parent = signalRetryingParent(startRetryingParent('sync'), 'sync');
    [$first, $second] = attemptsOf($parent);

    expect($asked)->toHaveCount(2);

    [$one, $two] = $asked;

    expect($one->runId)->toBe($parent->id)
        ->and($one->workflowClass)->toBe(RetryingParentWorkflow::class)
        ->and($one->actionClass)->toBe(RetriedChildWorkflow::class)
        ->and($one->childRunId)->toBe($first->id)
        ->and($one->sequence)->toBe(retryLinkOf($parent)->sequence)
        ->and($one->signal)->toBe('child.retry')
        ->and($one->cyclesSpent)->toBe(0)
        ->and($one->cap)->toBe(5)
        ->and($one->executions)->toBe(1)
        ->and($one->failure->class)->toBe(RetriedChildOutage::class)
        ->and($one->failure->message)->toBe('outage on attempt 1')
        ->and($two->childRunId)->toBe($second->id)
        ->and($two->cyclesSpent)->toBe(1)
        ->and($two->executions)->toBe(2)
        ->and($two->failure->message)->toBe('outage on attempt 2');
});

it('hands the parent the failure when the policy declines it', function (): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$when = fn (): bool => false;

    $parent = startRetryingParent('sync');

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->signals()->count())->toBe(0);
});

it('reads a policy that throws as a refusal and records it', function (): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$when = fn (): bool => throw new RuntimeException('the policy itself is broken');

    $logged = [];
    Log::listen(function ($message) use (&$logged): void {
        $logged[] = [$message->message, $message->context];
    });

    $parent = startRetryingParent('sync');
    [$child] = attemptsOf($parent);

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->exception['class'] ?? null)->toBe(ChildWorkflowFailedException::class)
        ->and($logged)->toHaveCount(1)
        ->and($logged[0][0])->toBe('saga-lara-flow: retry_policy_threw')
        ->and($logged[0][1]['child_flow_run_id'] ?? null)->toBe($child->id)
        ->and($logged[0][1]['message'] ?? null)->toBe('the policy itself is broken');
});

it('lets the parent carry on once the policy gives up under continueParentOnFailure', function (string $mode): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$continue = true;
    RetryingParentWorkflow::$maxRetries = 1;

    $parent = signalRetryingParent(startRetryingParent($mode), $mode);

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result)->toBe('parent:none')
        ->and(CompensationLog::all())->toBe(['undo:c', 'undo:c']);
})->with([['sync'], ['queued']]);

it('takes a RetryPolicy for a child, and refuses one given alongside the arguments it replaces', function (): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$signal = new ChildOutagePolicy;

    $parent = startRetryingParent('sync');

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and(retryLinkOf($parent)->retry_signal)->toBe('outage.over')
        ->and(retryLinkOf($parent)->retry_signal_max_attempts)->toBe(3);

    $refused = SagaFlow::create(ParentOfMixedPolicyWorkflow::class)->runSync();

    expect($refused->status)->toBe(FlowStatus::Failed)
        ->and($refused->exception['message'] ?? '')->toContain('takes a RetryPolicy or the arguments it replaces');
});

final class ParentOfMixedPolicyWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->child(RetriedChildWorkflow::class, ['order'])
            ->retryOnSignal(new ChildOutagePolicy, maxRetries: 2)
            ->run();
    }
}

it('gives up once the wait for the signal times out', function (bool $continue, FlowStatus $lands): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$waitSeconds = 60;
    RetryingParentWorkflow::$continue = $continue;
    RetryingParentWorkflow::$waitAfter = $continue;

    $parent = startRetryingParent('queued');

    expect($parent->status)->toBe(FlowStatus::Waiting);

    $this->travel(2)->minutes();

    expect(app(FlowMonitor::class)->sweep()['signals'])->toBe(1);

    drainQueue();

    $parent->refresh();

    // A parent that carries on waits further on, with the link no longer parked.
    expect($parent->status)->toBe($lands)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Failed)
        ->and($parent->signals()->pluck('status')->first())->toBe(SignalStatus::TimedOut)
        ->and(SagaFlow::query()->whereAwaitingRetrySignal()->count())->toBe(0)
        ->and(attemptsOf($parent))->toHaveCount(1);
})->with([
    'required' => [false, FlowStatus::Failed],
    'survivable' => [true, FlowStatus::Waiting],
]);

it('does not park again on a wait that already timed out', function (): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$waitSeconds = 60;
    RetryingParentWorkflow::$continue = true;

    $parent = startRetryingParent('queued');

    $this->travel(2)->minutes();
    app(FlowMonitor::class)->sweep();

    // A pass that settled the give-up and died before the parent went any further.
    $link = retryLinkOf($parent);
    $link->newQuery()->toBase()->where('id', $link->id)->update(['status' => ChildStatus::Failed->value]);

    drainQueue();
    $parent->refresh();

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result)->toBe('parent:none')
        ->and($parent->signals()->pluck('status')->all())->toBe([SignalStatus::TimedOut]);
});

it('holds a parked child to the budget it parked with', function (): void {
    RetriedChildWorkflow::$failures = 99;
    RetryingParentWorkflow::$maxRetries = 1;

    $parent = startRetryingParent('sync');

    // A deploy raises the budget while the parent waits.
    RetryingParentWorkflow::$maxRetries = 5;

    $parent = signalRetryingParent($parent, 'sync');

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and(retryLinkOf($parent)->retry_signal_max_attempts)->toBe(1);
});

it('plans a rollback past a failed child without parking on it', function (): void {
    RetriedChildWorkflow::$failures = 99;
    useDatabaseQueue();

    $run = SagaFlow::create(RetryingParentWorkflow::class)->run();

    // The child has failed; the parent has not replayed since, so nothing is parked yet.
    for ($i = 0; $i < 20 && (attemptsOf($run)[0] ?? null)?->status !== FlowStatus::Failed; $i++) {
        workOneJob();
    }

    $parent = SagaFlow::loadFlow($run->id)->compensate();
    drainQueue();

    expect($parent->fresh()->status)->toBe(FlowStatus::Cancelled)
        ->and($parent->signals()->count())->toBe(0)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c', 'undo:a']);
});

it('plans a rollback up to a parked child whose signal is in, without starting it again', function (): void {
    RetriedChildWorkflow::$failures = 1;

    $parent = startRetryingParent('sync');

    SagaFlow::loadFlow($parent->id)->signal('child.retry');

    $parent = SagaFlow::loadFlow($parent->id)->compensate();

    expect($parent->status)->toBe(FlowStatus::Cancelled)
        ->and(attemptsOf($parent))->toHaveCount(1)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c', 'undo:a']);
});

function parkOverExpiredChild(): FlowRun
{
    useDatabaseQueue();

    $run = SagaFlow::create(ParentOfExpiringChildWorkflow::class)->run();
    drainQueue();

    [$first] = attemptsOf($run);
    $first->newQuery()->toBase()->where('id', $first->id)->update(['expires_at' => now()->subMinute()]);

    app(FlowMonitor::class)->sweep();
    drainQueue();

    return FlowRun::query()->findOrFail($run->id);
}

it('retries a child that expired', function (): void {
    $parent = parkOverExpiredChild();
    [$first] = attemptsOf($parent);

    expect($first->fresh()->status)->toBe(FlowStatus::Expired)
        ->and($first->fresh()->exception['class'] ?? null)->toBe(FlowExpiredException::class)
        ->and($parent->status)->toBe(FlowStatus::Waiting)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::AwaitingRetry);

    $parent = signalRetryingParent($parent, 'queued');

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result)->toBe('attempt:2');
});

it('hands the parent the expiry once the wait over an expired child times out', function (): void {
    ParentOfExpiringChildWorkflow::$waitSeconds = 60;

    $parent = parkOverExpiredChild();

    expect(retryLinkOf($parent)->status)->toBe(ChildStatus::AwaitingRetry);

    $this->travel(2)->minutes();
    app(FlowMonitor::class)->sweep();
    drainQueue();

    $parent->refresh();

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->exception['class'] ?? null)->toBe(ChildWorkflowExpiredException::class)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Expired);
});

it('puts a link parked over an expired child back as expired', function (): void {
    $parent = parkOverExpiredChild();

    SagaFlow::loadFlow($parent->id)->cancel();

    expect(retryLinkOf($parent)->status)->toBe(ChildStatus::Expired);
});

it('does not retry a child that was cancelled', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(ParentOfCancellableChildWorkflow::class)->run();
    drainQueue();

    [$child] = attemptsOf($run);

    SagaFlow::loadFlow($child->id)->cancel();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);

    expect($child->fresh()->status)->toBe(FlowStatus::Cancelled)
        ->and($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->exception['class'] ?? null)->toBe(ChildWorkflowCancelledException::class)
        ->and($parent->signals()->count())->toBe(0);
});

it('takes a delivery that landed between the failure and the parking', function (): void {
    RetriedChildWorkflow::$failures = 1;
    useDatabaseQueue();

    $run = SagaFlow::create(RetryingParentWorkflow::class)->run();

    // Stop once the child has failed, before the parent's resume has run.
    for ($i = 0; $i < 20 && (attemptsOf($run)[0] ?? null)?->status !== FlowStatus::Failed; $i++) {
        workOneJob();
    }

    expect(attemptsOf($run)[0]->status)->toBe(FlowStatus::Failed)
        ->and(retryLinkOf($run)->status)->not->toBe(ChildStatus::AwaitingRetry);

    SagaFlow::loadFlow($run->id)->signal('child.retry');
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);
    $signals = $parent->signals()->get();

    // Spent straight away: no wait was ever opened, and the delivery is bound to the child.
    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($parent->result)->toBe('parent:order:2')
        ->and($signals)->toHaveCount(1)
        ->and($signals[0]->status)->toBe(SignalStatus::Consumed)
        ->and($signals[0]->wait_sequence)->toBe(retryLinkOf($parent)->sequence)
        ->and(retryLinkOf($parent)->retry_signal)->toBe('child.retry')
        ->and(retryLinkOf($parent)->retry_signal_attempts)->toBe(1);
});

it('ignores a floating signal older than the failed attempt', function (): void {
    RetriedChildWorkflow::$failures = 1;
    useDatabaseQueue();

    $run = SagaFlow::create(RetryingParentWorkflow::class)->run();
    workOneJob();

    SagaFlow::loadFlow($run->id)->signal('child.retry');

    $this->travel(5)->seconds();
    drainQueue();

    $parent = FlowRun::query()->findOrFail($run->id);

    expect($parent->status)->toBe(FlowStatus::Waiting)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::AwaitingRetry)
        ->and($parent->signals()->pluck('status')->all())->toBe([SignalStatus::Received, SignalStatus::Waiting]);
});

it('takes a floating delivery that raced the parking', function (): void {
    RetriedChildWorkflow::$failures = 1;

    $parent = startRetryingParent('sync');
    $wait = $parent->signals()->firstOrFail();

    // A delivery that looked for the wait before it was written, and floated instead.
    app(SignalRecorder::class)->storeReceivedSignal($parent, 'child.retry', []);

    app(FlowExecutor::class)->drive($parent->fresh(), RunMode::Sync);

    $parent->refresh();
    $sequence = retryLinkOf($parent)->sequence;

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and($wait->fresh()->status)->toBe(SignalStatus::Consumed)
        ->and($parent->signals()->get()->map(fn (FlowSignal $signal) => [$signal->status, $signal->wait_sequence])->all())
        ->toBe([[SignalStatus::Consumed, $sequence], [SignalStatus::Consumed, $sequence]]);
});

it('finds a parent parked on a child through the retry query and the list command', function (): void {
    RetriedChildWorkflow::$failures = 1;

    $parent = startRetryingParent('sync');

    expect(SagaFlow::query()->whereAwaitingRetrySignal()->get()->pluck('id')->all())->toBe([$parent->id])
        ->and(SagaFlow::query()->whereAwaitingRetrySignal('child.retry')->count())->toBe(1)
        ->and(SagaFlow::query()->whereAwaitingRetrySignal('other')->count())->toBe(0);

    Artisan::call('saga-flow:list');

    expect(Artisan::output())->toContain('waiting (retry: child.retry)');

    signalRetryingParent($parent, 'sync');

    expect(SagaFlow::query()->whereAwaitingRetrySignal()->count())->toBe(0);
});

it('puts a parked link back when the parent ends while it waits', function (
    string $how,
    FlowStatus $lands,
    array $undone,
): void {
    RetriedChildWorkflow::$failures = 99;

    $parent = startRetryingParent('queued');

    match ($how) {
        'cancel' => SagaFlow::loadFlow($parent->id)->cancel(),
        'compensate' => SagaFlow::loadFlow($parent->id)->compensate(),
        'expire' => $parent->newQuery()->toBase()->where('id', $parent->id)->update(['expires_at' => now()->subMinute()])
            && app(FlowMonitor::class)->sweep(),
    };

    drainQueue();
    $parent->refresh();

    // A rollback is planned up to the parked child: only the parent's own step is undone.
    expect($parent->status)->toBe($lands)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Failed)
        ->and($parent->signals()->pluck('status')->all())->toBe([SignalStatus::Cancelled])
        ->and(CompensationLog::all())->toBe($undone)
        ->and(SagaFlow::query()->whereAwaitingRetrySignal()->count())->toBe(0);
})->with([
    'cancelled' => ['cancel', FlowStatus::Cancelled, ['undo:c']],
    'compensated' => ['compensate', FlowStatus::Cancelled, ['undo:c', 'undo:a']],
    'expired' => ['expire', FlowStatus::Expired, ['undo:c', 'undo:a']],
]);

it('keeps a link parked when the child notification arrives after the parking', function (): void {
    RetriedChildWorkflow::$failures = 99;

    $parent = startRetryingParent('sync');
    [$child] = attemptsOf($parent);

    app(ChildRecorder::class)->recordFailed(retryLinkOf($parent), $child);

    expect(retryLinkOf($parent)->status)->toBe(ChildStatus::AwaitingRetry);
});

it('does not park on a child the parent already went past', function (): void {
    RetriedChildWorkflow::$failures = 99;
    config()->set('saga-lara-flow.signals.wake_workflow_on_signal', false);

    $parent = SagaFlow::create(ParentPassingFailedChildWorkflow::class)->runSync();

    expect($parent->status)->toBe(FlowStatus::Waiting);

    // A deploy adds the policy while the parent waits further on.
    ParentPassingFailedChildWorkflow::$retry = true;

    SagaFlow::loadFlow($parent->id)->signal('go');
    $parent = app(FlowExecutor::class)->drive($parent->fresh(), RunMode::Sync);

    expect($parent->status)->toBe(FlowStatus::Completed)
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Failed)
        ->and($parent->signals()->pluck('name')->all())->toBe(['go']);
});

it('refuses to hand a parked child ordinal to another operation', function (string $edit, string $names): void {
    RetriedChildWorkflow::$failures = 99;

    $parent = startRetryingParent('sync', ParentEditedAtParkedChildWorkflow::class);

    expect($parent->status)->toBe(FlowStatus::Waiting);

    ParentEditedAtParkedChildWorkflow::$edit = $edit;

    SagaFlow::loadFlow($parent->id)->signal('child.retry');
    $parent = app(FlowExecutor::class)->drive($parent->fresh(), RunMode::Sync);

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and($parent->exception['class'] ?? null)->toBe(HistoryContractMismatchException::class)
        ->and($parent->exception['message'] ?? '')->toContain($names);
})->with([
    'an awaitSignal()' => ['signal', 'child workflow'],
    'an action()' => ['action', 'child workflow'],
    'a sideEffect()' => ['side effect', 'child workflow'],
]);

it('refuses to park or retry a link under a parent that has moved on', function (): void {
    RetriedChildWorkflow::$failures = 99;

    $parent = startRetryingParent('sync');
    $link = retryLinkOf($parent);
    [$child] = attemptsOf($parent);

    // Read before another pass started the next attempt: its view of the link is stale.
    $stale = retryLinkOf($parent);

    expect(app(ChildRecorder::class)->retryChild(retryLinkOf($parent), $child, $child, 'child.retry', null))->toBeTrue()
        ->and(app(ChildRecorder::class)->retryChild($stale, $child, $child, 'child.retry', null))->toBeFalse()
        ->and(retryLinkOf($parent)->retry_signal_attempts)->toBe(1);

    $link->newQuery()->toBase()->where('id', $link->id)->update([
        'status' => ChildStatus::AwaitingRetry->value,
        'retry_signal_attempts' => 0,
    ]);
    $link = retryLinkOf($parent);

    $parent->newQuery()->toBase()->where('id', $parent->id)->update(['status' => FlowStatus::Cancelling->value]);

    expect(app(ChildRecorder::class)->retryChild($link, $child, $child, 'child.retry', null))->toBeFalse()
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::AwaitingRetry)
        ->and(retryLinkOf($parent)->retry_signal_attempts)->toBe(0);

    $parent->newQuery()->toBase()->where('id', $parent->id)->update(['status' => FlowStatus::Failed->value]);

    $link->newQuery()->toBase()->where('id', $link->id)->update(['status' => ChildStatus::Failed->value]);

    expect(app(ChildRecorder::class)->awaitRetry(retryLinkOf($parent), $child, 'child.retry', null))->toBeFalse()
        ->and(retryLinkOf($parent)->status)->toBe(ChildStatus::Failed);
});

it('registers the compensation of the attempt that completed, once', function (string $mode): void {
    RetriedChildWorkflow::$failures = 1;
    RetryingParentWorkflow::$compensate = true;
    RetryingParentWorkflow::$failAfter = true;

    $parent = signalRetryingParent(startRetryingParent($mode), $mode);

    expect($parent->status)->toBe(FlowStatus::Failed)
        ->and(CompensationLog::all())->toBe(['undo:c', 'undo:child', 'undo:a']);
})->with([['sync'], ['queued']]);

it('never announces a retried child without its link, poisoned through a model observer', function (): void {
    RetriedChildWorkflow::$failures = 1;

    $parent = startRetryingParent('sync');

    // The last write of the retry is the new attempt's child.started row; PostgreSQL turns
    // the COMMIT that reports success into a rollback once an observer swallows a failure.
    FlowEvent::created(function (FlowEvent $event): void {
        if ($event->type !== FlowEventType::ChildStarted) {
            return;
        }

        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // An observer that eats its own failure — broken, but nothing stops it.
        }
    });

    $announced = 0;
    Event::listen(ChildWorkflowRetried::class, function () use (&$announced): void {
        $announced++;
    });

    SagaFlow::loadFlow($parent->id)->signal('child.retry');
    app(FlowExecutor::class)->drive($parent->fresh(), RunMode::Sync);

    expect($announced)->toBe(retryLinkOf($parent)->retry_signal_attempts);
});

it('starts the next attempt with the arguments the failed one was given', function (): void {
    RetriedChildWorkflow::$failures = 1;

    $parent = startRetryingParent('sync');

    // A deploy changes how the parent builds the child's arguments while it waits.
    RetryingParentWorkflow::$order = 'rebuilt';

    $parent = signalRetryingParent($parent, 'sync');

    expect($parent->result)->toBe('parent:order:2');
});

it('leaves a link that moved to a new attempt to that attempt', function (): void {
    RetriedChildWorkflow::$failures = 1;

    $parent = startRetryingParent('sync');
    [$first] = attemptsOf($parent);

    // Loaded by the first attempt's notification before the retry moved the link on.
    $stale = retryLinkOf($parent);

    $parent = signalRetryingParent($parent, 'sync');

    app(ChildRecorder::class)->recordFailed($stale, $first);

    expect(retryLinkOf($parent)->status)->toBe(ChildStatus::Completed);
});

it('never announces a parking without its link, poisoned through a model observer', function (): void {
    RetriedChildWorkflow::$failures = 99;

    // The last write of the parking is its child.awaiting_retry row; on PostgreSQL a failure
    // an observer swallows there turns the COMMIT that reports success into a rollback.
    FlowEvent::created(function (FlowEvent $event): void {
        if ($event->type !== FlowEventType::ChildAwaitingRetry) {
            return;
        }

        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // An observer that eats its own failure — broken, but nothing stops it.
        }
    });

    $announced = 0;
    Event::listen(ChildWorkflowAwaitingRetry::class, function () use (&$announced): void {
        $announced++;
    });

    $parent = startRetryingParent('sync');

    expect($announced)->toBe(FlowChild::query()->where('status', ChildStatus::AwaitingRetry)->count())
        ->and($parent->signals()->count())->toBe($announced);
});
