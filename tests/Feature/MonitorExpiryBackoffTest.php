<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\VanishedArgumentWorkflow;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Support\Facades\DB;

beforeEach(fn () => VanishedArgumentWorkflow::reset());

/**
 * An overdue run whose replay will throw once its argument is taken away.
 */
function overdueUnplannableRun(int $minutesOverdue): string
{
    $run = SagaFlow::create(VanishedArgumentWorkflow::class)->run();
    drainQueue();

    $run = SagaFlow::findRun($run->id);
    $run->expires_at = now()->subMinutes($minutesOverdue);
    $run->save();

    return $run->id;
}

it('lets the run behind an unplannable one expire on the next pass', function () {
    useDatabaseQueue();

    // One row wide, so the stuck run IS the page — the same holds for any size once
    // that many runs at the head fail.
    config()->set('saga-lara-flow.monitor.expiration.batch_size', 1);

    $stuck = overdueUnplannableRun(120);

    $healthy = SagaFlow::create(SignalOnlyWorkflow::class)->run();
    drainQueue();
    $healthy = SagaFlow::findRun($healthy->id);
    $healthy->expires_at = now()->subMinute();
    $healthy->save();

    VanishedArgumentWorkflow::$label = null;

    // The first pass spends the page on the stuck run and steps it aside.
    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(0)
        ->and(SagaFlow::findRun($healthy->id)->status)->toBe(FlowStatus::Waiting);

    // The second reaches the run behind it, which is what never happened before.
    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1)
        ->and(SagaFlow::findRun($healthy->id)->status)->toBe(FlowStatus::Expired)
        ->and(SagaFlow::findRun($stuck)->status)->toBe(FlowStatus::Waiting);
});

it('counts the failure and stops re-reading the run every pass', function () {
    useDatabaseQueue();
    logToFile($path = sys_get_temp_dir().'/saga-backoff-'.bin2hex(random_bytes(6)).'.log');

    $stuck = overdueUnplannableRun(120);
    VanishedArgumentWorkflow::$label = null;

    app(FlowMonitor::class)->sweep();

    $run = SagaFlow::findRun($stuck);

    expect($run->expiry_attempts)->toBe(1)
        ->and($run->expiry_available_at)->not->toBeNull()
        ->and($run->expiry_available_at->isFuture())->toBeTrue();

    // Held off, so the next passes do not plan it again and the journal stops
    // repeating the same line for as long as the window is open.
    app(FlowMonitor::class)->sweep();
    app(FlowMonitor::class)->sweep();

    $log = is_file($path) ? (string) file_get_contents($path) : '';

    expect(substr_count($log, AnomalyLog::REASON_EXPIRY_FAILED.'"'))->toBe(1)
        ->and(SagaFlow::findRun($stuck)->expiry_attempts)->toBe(1)
        // The count travels with the journal line too, so an operator reading the log
        // can tell a first failure from the five hundredth.
        ->and($log)->toContain('"expiry_attempts":1');
});

it('widens the window with each failure and stops at the ceiling', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.monitor.expiration.backoff.base_seconds', 60);
    config()->set('saga-lara-flow.monitor.expiration.backoff.max_seconds', 200);

    $stuck = overdueUnplannableRun(120);
    VanishedArgumentWorkflow::$label = null;

    $windows = [];

    foreach (range(1, 3) as $attempt) {
        app(FlowMonitor::class)->sweep();

        $run = SagaFlow::findRun($stuck);
        $windows[] = (int) now()->diffInSeconds($run->expiry_available_at, absolute: false);

        // Reopen it by hand: the point here is the shape of the window, not the wait.
        $run->expiry_available_at = null;
        $run->save();
    }

    // 60, then 120, then 240 clipped to the 200 ceiling — never a give-up.
    expect($windows[0])->toBeGreaterThan(50)->toBeLessThan(70)
        ->and($windows[1])->toBeGreaterThan(110)->toBeLessThan(130)
        ->and($windows[2])->toBeGreaterThan(190)->toBeLessThan(210)
        ->and(SagaFlow::findRun($stuck)->expiry_attempts)->toBe(3);
});

it('reaches a run that has waited longer than the retries queued in front of it', function () {
    useDatabaseQueue();

    // Scaled to the shape that starves: once a page's worth of held-off runs comes
    // back every period, retries can fill every pass for ever on deadline order alone.
    config()->set('saga-lara-flow.monitor.expiration.batch_size', 1);
    config()->set('saga-lara-flow.monitor.expiration.backoff.base_seconds', 60);
    config()->set('saga-lara-flow.monitor.expiration.backoff.max_seconds', 120);

    foreach ([600, 500, 400] as $minutesOverdue) {
        overdueUnplannableRun($minutesOverdue);
    }

    $healthy = SagaFlow::create(SignalOnlyWorkflow::class)->run();
    drainQueue();
    $healthy = SagaFlow::findRun($healthy->id);
    $healthy->expires_at = now()->subMinute();
    $healthy->save();

    VanishedArgumentWorkflow::$label = null;

    $reached = null;

    foreach (range(1, 40) as $sweep) {
        app(FlowMonitor::class)->sweep();

        if (SagaFlow::findRun($healthy->id)->status === FlowStatus::Expired) {
            $reached = $sweep;

            break;
        }

        $this->travel(60)->seconds();
    }

    // The three broken runs are much older, so on deadline order alone they hold every
    // pass between them and this one is never looked at.
    expect($reached)->not->toBeNull()->toBeLessThanOrEqual(10);
});

it('keeps host model events out of the pass that must not throw', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.monitor.expiration.batch_size', 1);

    $stuck = overdueUnplannableRun(120);

    $healthy = SagaFlow::create(SignalOnlyWorkflow::class)->run();
    drainQueue();
    $healthy = SagaFlow::findRun($healthy->id);
    $healthy->expires_at = now()->subMinute();
    $healthy->save();

    VanishedArgumentWorkflow::$label = null;

    FlowRun::updating(function (FlowRun $run): void {
        throw new RuntimeException('a host observer on the run model');
    });

    // A save() here would hand the sweep to host code: a throw would escape the branch
    // that exists not to throw, and a refusal would leave the run holding the page
    // while the journal reported it stepped aside.
    app(FlowMonitor::class)->sweep();
    $this->travel(2)->minutes();
    app(FlowMonitor::class)->sweep();

    expect(SagaFlow::findRun($stuck)->expiry_attempts)->toBe(1)
        ->and(SagaFlow::findRun($healthy->id)->status)->toBe(FlowStatus::Expired);
});

it('counts from the row the writer holds, not from a stale read', function () {
    useDatabaseQueue();

    $stuck = overdueUnplannableRun(120);
    VanishedArgumentWorkflow::$label = null;

    $table = (new FlowRun)->getTable();

    DB::connection('testing')->table($table)->where('id', $stuck)->update([
        'expiry_attempts' => 6,
        'expiry_available_at' => now()->subMinute(),
    ]);

    // What a lagging replica hands the page: the row as it stood before those six.
    FlowRun::retrieved(function (FlowRun $run) use ($stuck): void {
        if ($run->id === $stuck) {
            $run->expiry_attempts = 0;
            $run->syncOriginal();
        }
    });

    app(FlowMonitor::class)->sweep();

    $row = DB::connection('testing')->table($table)->where('id', $stuck)->first();

    // Six failures are on the row; the page was read before them. Counting from the
    // model would write a seventh failure as a first one and shrink the window with it.
    expect((int) $row->expiry_attempts)->toBe(7)
        ->and((int) now()->diffInSeconds($row->expiry_available_at, absolute: false))
        ->toBeGreaterThan(3500);
});

/**
 * Stage the one interleaving that matters in a single process: run $competitor the
 * moment the hold-off has read the count and before it writes.
 */
function betweenTheReadAndTheWrite(callable $competitor): void
{
    $fired = false;

    DB::listen(function (QueryExecuted $query) use ($competitor, &$fired): void {
        if ($fired || ! str_contains($query->sql, 'expiry_attempts') || ! str_starts_with($query->sql, 'select')) {
            return;
        }

        $fired = true;

        $competitor();
    });
}

it('refuses a hold-off the row moved under', function () {
    useDatabaseQueue();

    $stuck = overdueUnplannableRun(120);
    VanishedArgumentWorkflow::$label = null;

    $table = (new FlowRun)->getTable();

    // A second sweep on the same run gets there first: it counts a failure and takes
    // the window. Stacking a hold-off on top of that would double the wait.
    betweenTheReadAndTheWrite(fn () => DB::connection('testing')->table($table)
        ->where('id', $stuck)
        ->update(['expiry_attempts' => 4, 'expiry_available_at' => now()->addHour()]));

    app(FlowMonitor::class)->sweep();

    $row = DB::connection('testing')->table($table)->where('id', $stuck)->first();

    expect((int) $row->expiry_attempts)->toBe(4)
        ->and((int) now()->diffInSeconds($row->expiry_available_at, absolute: false))->toBeGreaterThan(3500);
});

it('refuses a hold-off when only the window was taken', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.monitor.expiration.backoff.max_seconds', 120);

    $stuck = overdueUnplannableRun(120);
    VanishedArgumentWorkflow::$label = null;

    $table = (new FlowRun)->getTable();

    // The count still reads as it did, so a fence on the count alone would let this
    // write through and push the window a second time.
    betweenTheReadAndTheWrite(fn () => DB::connection('testing')->table($table)
        ->where('id', $stuck)
        ->update(['expiry_available_at' => now()->addHour()]));

    app(FlowMonitor::class)->sweep();

    $row = DB::connection('testing')->table($table)->where('id', $stuck)->first();

    expect((int) $row->expiry_attempts)->toBe(0)
        ->and((int) now()->diffInSeconds($row->expiry_available_at, absolute: false))->toBeGreaterThan(3500);
});

it('holds a run off without resetting the clock the doctor reads', function () {
    useDatabaseQueue();

    $stuck = overdueUnplannableRun(120);

    // The R2 shape: the wait was consumed, the resume that should have followed was
    // lost, so the run sits Waiting with nothing in flight — a doctor candidate and an
    // expiry candidate at the same time.
    SagaFlow::loadFlow($stuck)->signal('go');
    DB::connection('testing')->table('jobs')->delete();

    $run = SagaFlow::findRun($stuck);
    $run->updated_at = now()->subHour();
    $run->save();

    VanishedArgumentWorkflow::$label = null;

    $repository = app(FlowRepository::class);
    $grace = (int) config('saga-lara-flow.repair.grace_seconds');

    expect(collect($repository->dueForRepair(100, $grace, 10))->pluck('id')->all())->toContain($stuck);

    app(FlowMonitor::class)->sweep();

    // updated_at is the staleness clock: a sweep that bumped it would keep this run
    // permanently too fresh for the doctor, every minute, for ever.
    expect(SagaFlow::findRun($stuck)->expiry_attempts)->toBe(1)
        ->and(collect($repository->dueForRepair(100, $grace, 10))->pluck('id')->all())->toContain($stuck);
});
