<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\CancelChildWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRepairReport;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CountedActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentAwaitWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentCancelPolicyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * A run's first job goes out after the row that describes it is committed: a root run's,
 * a child's, and a child's started again on a retry signal. A process that dies in
 * between leaves a Pending run with nothing on the queue for it, and the doctor is the
 * one to send that job again.
 */
beforeEach(function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    FlakyPaymentAction::reset();
    PendingRetriedChildWorkflow::$attempts = 0;
});

/**
 * Throw away the first $times RunWorkflowJobs of every run $lose picks, the moment each
 * reaches the queue — the job a process dying between the commit and the dispatch never sends.
 *
 * @param  Closure(FlowRun): bool  $lose
 */
function loseFirstJob(Closure $lose, int $times = 1): void
{
    $lost = [];

    Event::listen(JobQueued::class, function (JobQueued $event) use ($lose, $times, &$lost): void {
        if (! $event->job instanceof RunWorkflowJob) {
            return;
        }

        $id = $event->job->flowRunId;

        if (($lost[$id] ?? 0) < $times && $lose(SagaFlow::findRun($id))) {
            $lost[$id] = ($lost[$id] ?? 0) + 1;
            DB::connection('testing')->table('jobs')->where('id', $event->id)->delete();
        }
    });
}

function repairOnceTheJobIsLate(int $seconds = 120): FlowRepairReport
{
    test()->travel($seconds)->seconds();

    $report = app(FlowDoctor::class)->repair();
    drainQueue();

    return $report;
}

function loseCloseJobs(): void
{
    Event::listen(JobQueued::class, function (JobQueued $event): void {
        if ($event->job instanceof CancelChildWorkflowJob) {
            DB::connection('testing')->table('jobs')->where('id', $event->id)->delete();
        }
    });
}

function eventCount(string $runId, FlowEventType $type): int
{
    return SagaFlow::findRun($runId)->events()->where('type', $type->value)->count();
}

/**
 * @return list<string>
 */
function rewakeReasons(string $runId): array
{
    return SagaFlow::findRun($runId)->events()
        ->where('type', FlowEventType::FlowRewoken->value)
        ->get()
        ->map(fn ($event): ?string => $event->payload['reason'] ?? null)
        ->all();
}

final class PendingRetriedChildWorkflow extends Workflow
{
    public static int $attempts = 0;

    public function handle(): string
    {
        if (++self::$attempts === 1) {
            throw new RuntimeException('first attempt fails');
        }

        return 'child';
    }
}

final class ParentRetryingPendingChildWorkflow extends Workflow
{
    public function handle(): string
    {
        return 'parent:'.$this->child(PendingRetriedChildWorkflow::class)->retryOnSignal('child.retry')->run();
    }
}

it('starts a root run whose first job was lost', function () {
    loseFirstJob(fn (FlowRun $run): bool => true);

    $run = SagaFlow::create(CountedActionWorkflow::class)->run();
    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Pending);

    $report = repairOnceTheJobIsLate();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(FlakyPaymentAction::$calls)->toBe(1)
        ->and($report->rewokenFlows)->toBe(1)
        ->and(rewakeReasons($run->id))->toBe(['lost_start']);
});

it('starts a child whose first job was lost, and its parent completes', function () {
    loseFirstJob(fn (FlowRun $run): bool => $run->parent_id !== null);

    $parent = SagaFlow::create(ParentAwaitWorkflow::class)->run();
    drainQueue();

    $child = $parent->children()->first()->child;

    expect($child->status)->toBe(FlowStatus::Pending);

    repairOnceTheJobIsLate();

    expect(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Completed)
        ->and(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Completed);
});

it('starts a child retried on a signal whose first job was lost', function () {
    $parent = SagaFlow::create(ParentRetryingPendingChildWorkflow::class)->run();
    drainQueue();

    loseFirstJob(fn (FlowRun $run): bool => $run->parent_id !== null);
    SagaFlow::loadFlow($parent->id)->signal('child.retry');
    drainQueue();

    $retried = $parent->children()->first()->child;

    expect(PendingRetriedChildWorkflow::$attempts)->toBe(1)
        ->and($retried->status)->toBe(FlowStatus::Pending);

    repairOnceTheJobIsLate();

    expect(SagaFlow::findRun($retried->id)->status)->toBe(FlowStatus::Completed)
        ->and(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Completed)
        ->and(SagaFlow::findRun($parent->id)->result)->toBe('parent:child');
});

it('leaves a Pending run alone while the repair is off', function (string $off) {
    config()->set($off, false);
    loseFirstJob(fn (FlowRun $run): bool => true);

    $run = SagaFlow::create(CountedActionWorkflow::class)->run();

    repairOnceTheJobIsLate();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Pending)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
})->with([['saga-lara-flow.repair.enabled'], ['saga-lara-flow.repair.redispatch_lost_starts']]);

it('leaves a Pending run alone while it is younger than the grace window', function () {
    loseFirstJob(fn (FlowRun $run): bool => true);

    $run = SagaFlow::create(CountedActionWorkflow::class)->run();

    repairOnceTheJobIsLate(seconds: 30);

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Pending)
        ->and(rewakeReasons($run->id))->toBe([]);
});

it('gives up on a run after max_attempts', function () {
    config()->set('saga-lara-flow.repair.max_attempts', 3);

    // Every job the doctor sends is lost as well.
    loseFirstJob(fn (FlowRun $run): bool => true, times: PHP_INT_MAX);

    $run = SagaFlow::create(CountedActionWorkflow::class)->run();

    foreach (range(1, 5) as $pass) {
        // Past the grace window and the longest backoff.
        repairOnceTheJobIsLate(seconds: 400);
    }

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Pending)
        ->and(rewakeReasons($run->id))->toBe(['lost_start', 'lost_start', 'lost_start']);
});

it('sends nothing for a run that started between the pick and the lock', function () {
    loseFirstJob(fn (FlowRun $run): bool => true);

    $run = SagaFlow::create(CountedActionWorkflow::class)->run();

    test()->travel(2)->minutes();

    // The original job arrives after all and takes the run while the doctor is deciding.
    $reached = beforeStatement(
        '/^select \\* from [`"]?\\w*flow_runs[`"]? where [`"]?\\w*flow_runs[`"]?\\.[`"]?id[`"]? = \\?/i',
        function () use ($run): void {
            config('saga-lara-flow.models.flow_run')::query()
                ->whereKey($run->id)
                ->update(['status' => FlowStatus::Running]);
        },
    );

    $report = app(FlowDoctor::class)->repair();

    expect($reached())->toBeTrue()
        ->and($report->rewokenFlows)->toBe(0)
        ->and($report->skipped)->toBe(1)
        ->and(rewakeReasons($run->id))->toBe([])
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

it('holds its job back while a late original still holds the run', function (string $by) {
    config()->set('saga-lara-flow.locks.enabled', true);
    loseFirstJob(fn (FlowRun $run): bool => true);

    $run = SagaFlow::create(CountedActionWorkflow::class)->run();

    // The original RunWorkflowJob was not lost after all: a worker has it and is driving the
    // run. A second job driving the same run beside it can fail a run nothing failed in.
    $original = app(LockMiddlewareFactory::class)->workflowMiddleware($run->id)[0]
        ->getLockKey(new RunWorkflowJob($run->id));
    $held = Cache::lock($original, 600);

    expect($held->get())->toBeTrue();

    if ($by === 'doctor') {
        test()->travel(2)->minutes();
        app(FlowDoctor::class)->repair();
    } else {
        app(FlowDoctor::class)->kick($run);
    }

    workOneJob();

    // The job went back on the queue for later rather than driving the run.
    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Pending)
        ->and(FlakyPaymentAction::$calls)->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(1);

    $held->release();
})->with([['doctor'], ['kick']]);

it('leaves the run to a late original that started it first', function (string $by) {
    $run = SagaFlow::create(CountedActionWorkflow::class)->run();

    // The original is not lost, only late: the copy goes on the queue behind it.
    if ($by === 'doctor') {
        test()->travel(2)->minutes();
        app(FlowDoctor::class)->repair();
    } else {
        app(FlowDoctor::class)->kick($run);
    }

    expect(DB::connection('testing')->table('jobs')->count())->toBe(2);

    workOneJob();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);

    // The copy finds the run started. Driving it now would put a second replay beside
    // whatever resume picks the run up next, under a lock of another class.
    workOneJob();

    expect(eventCount($run->id, FlowEventType::FlowStarted))->toBe(1)
        ->and(eventCount($run->id, FlowEventType::FlowResumed))->toBe(0);

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(FlakyPaymentAction::$calls)->toBe(1);
})->with([['doctor'], ['kick']]);

it('leaves a child to the close policy of a parent that closed it', function () {
    loseFirstJob(fn (FlowRun $run): bool => $run->parent_id !== null);
    loseCloseJobs();

    $parent = SagaFlow::create(ParentCancelPolicyWorkflow::class)->run();
    drainQueue();

    $child = $parent->children()->first()->child;
    SagaFlow::loadFlow($parent->id)->cancel('operator');
    drainQueue();

    $report = repairOnceTheJobIsLate();

    // Started now, the child would do work its parent's policy says should not happen,
    // with nothing left to undo it.
    expect(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Pending)
        ->and($report->rewokenFlows)->toBe(0)
        ->and($report->skipped)->toBe(0)
        ->and(rewakeReasons($child->id))->toBe([]);
});

it('starts a child its closed parent abandoned', function () {
    loseFirstJob(fn (FlowRun $run): bool => $run->parent_id !== null);

    $parent = SagaFlow::create(ParentAwaitWorkflow::class)->run();
    drainQueue();

    $child = $parent->children()->first()->child;
    SagaFlow::loadFlow($parent->id)->cancel('operator');
    drainQueue();

    repairOnceTheJobIsLate();

    expect(SagaFlow::findRun($parent->id)->status)->toBe(FlowStatus::Cancelled)
        ->and(SagaFlow::findRun($child->id)->status)->toBe(FlowStatus::Completed);
});

it('leaves a child alone whose parent is closed between the pick and the lock', function () {
    // R2 would wake the parent first and lock it before this rule gets to the child.
    config()->set('saga-lara-flow.repair.wake_stuck_flows', false);
    loseFirstJob(fn (FlowRun $run): bool => $run->parent_id !== null);

    $parent = SagaFlow::create(ParentCancelPolicyWorkflow::class)->run();
    drainQueue();

    $child = $parent->children()->first()->child;

    test()->travel(2)->minutes();

    $reached = beforeStatement(
        '/^select \\* from [`"]?\\w*flow_runs[`"]? where [`"]?\\w*flow_runs[`"]?\\.[`"]?id[`"]? = \\?/i',
        function () use ($parent): void {
            config('saga-lara-flow.models.flow_run')::query()
                ->whereKey($parent->id)
                ->update(['status' => FlowStatus::Cancelled]);
        },
    );

    $report = app(FlowDoctor::class)->repair();

    expect($reached())->toBeTrue()
        ->and($report->rewokenFlows)->toBe(0)
        ->and($report->skipped)->toBe(1)
        ->and(rewakeReasons($child->id))->toBe([])
        ->and(DB::connection('testing')->table('jobs')->count())->toBe(0);
});

it('starts a run from a RunWorkflowJob queued before the job could be marked start-only', function () {
    $run = SagaFlow::create(CountedActionWorkflow::class)->run();

    // The payload a worker finds on the queue after an upgrade: the job as it was
    // serialized before it had the property.
    $job = DB::connection('testing')->table('jobs')->first();
    $payload = json_decode($job->payload, true);
    $command = preg_replace('/s:9:"startOnly";b:0;/', '', $payload['data']['command'], 1, $stripped);

    if ($stripped === 1) {
        $command = preg_replace_callback(
            '/^(O:\\d+:"[^"]+":)(\\d+)/',
            fn (array $match): string => $match[1].($match[2] - 1),
            $command,
        );
    }

    $payload['data']['command'] = $command;
    DB::connection('testing')->table('jobs')->where('id', $job->id)->update(['payload' => json_encode($payload)]);

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed);
});
