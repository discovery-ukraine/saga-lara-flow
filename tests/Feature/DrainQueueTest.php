<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TwoStepWorkflow;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use PHPUnit\Framework\AssertionFailedError;

/**
 * The database queue hands out a job only once available_at <= now, and a worker told
 * to stop when empty stops at the first pop that finds nothing it may take. A wall clock
 * that steps back between the push and the pop — WSL2's does, by more than a second —
 * hides a job the engine meant to run at once. drainQueue() waits such a step out, and
 * fails on anything else it finds left, naming the job.
 *
 * The clock these tests step is Carbon's, driven from hrtime: the machine's own wall
 * clock is the one that steps, and a test of the wait must not depend on it. Elapsed time
 * is measured with hrtime for the same reason.
 */
function secondsSince(int $hrtime): float
{
    return (hrtime(true) - $hrtime) / 1e9;
}

it('tells a clock step back from a real leftover', function (array $ahead, ?int $wait) {
    $left = collect($ahead)->map(fn (int $seconds): object => (object) ['available_at' => 1000 + $seconds]);

    expect(drainQueueClockStepBack($left, 1000))->toBe($wait);
})->with([
    'one second ahead' => [[1], 1],
    'three seconds ahead' => [[3], 3],
    'the furthest sets the wait' => [[1, 3, 2], 3],
    'already due' => [[0], null],
    'long past due' => [[-5], null],
    'further ahead than a step' => [[4], null],
    'one of them due' => [[2, 0], null],
    'one of them too far ahead' => [[2, 4], null],
]);

it('waits out a clock step back and runs the job it hid', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->run();

    // The clock steps 2 s back from the push and then runs on, as WSL2's does.
    $availableAt = (int) DB::table('jobs')->value('available_at');
    $steppedAt = hrtime(true);

    // A native date, not a Carbon one: building Carbon here would ask this clock again.
    Carbon::setTestNow(fn () => DateTimeImmutable::createFromFormat(
        'U.u',
        sprintf('%.6F', $availableAt - 2 + secondsSince($steppedAt)),
    ));

    drainQueue();

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Completed)
        ->and(secondsSince($steppedAt))->toBeGreaterThan(1.0);
});

it('names a job a clock that never catches up left behind, after one bounded wait', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->run();

    $availableAt = (int) DB::table('jobs')->value('available_at');

    // Stepped 2 s back and stopped there: the wait must end on hrtime, not on this clock.
    Carbon::setTestNow(Carbon::createFromTimestamp($availableAt - 2));

    $started = hrtime(true);

    expect(fn () => drainQueue())->toThrow(
        AssertionFailedError::class,
        RunWorkflowJob::class.' on "default": attempts 0, reserved_at null, available_at '.$availableAt.' (+2 s from now)',
    );

    // One wait of the 2 s the job was ahead, and no second one.
    expect(secondsSince($started))->toBeLessThan(4.0)
        ->and(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Pending);
});

it('names a job further ahead than a clock step without waiting for it', function () {
    useDatabaseQueue();

    SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->run();

    DB::table('jobs')->update(['available_at' => Carbon::now()->getTimestamp() + 60]);

    $started = hrtime(true);

    expect(fn () => drainQueue())->toThrow(AssertionFailedError::class, RunWorkflowJob::class.' on "default"');

    // Short of the shortest wait drainQueue() would take for a clock step.
    expect(secondsSince($started))->toBeLessThan(1.0);
});

it('names a job the worker left on a queue it does not work', function () {
    useDatabaseQueue();

    SagaFlow::create(TwoStepWorkflow::class)->withArguments('order')->run();

    DB::table('jobs')->update(['queue' => 'elsewhere']);

    expect(fn () => drainQueue())->toThrow(AssertionFailedError::class, 'on "elsewhere"');
});
