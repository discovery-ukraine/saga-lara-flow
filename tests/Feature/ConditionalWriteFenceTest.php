<?php

use Carbon\Carbon;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Support\Facades\DB;

/**
 * Every fence in the engine reads `=== 1` off an UPDATE, and the drivers do not agree
 * about what an UPDATE reports: MySQL counts the rows it CHANGED, not the rows it
 * MATCHED, since Laravel never sets PDO::MYSQL_ATTR_FOUND_ROWS. An update whose every
 * value already equals what is stored therefore reports zero there and one on SQLite and
 * PostgreSQL.
 *
 * FlowStateMachine::write() carries an escape hatch for exactly that, and on SQLite no
 * test can reach it — the guard returns on the first branch. These tests are written to
 * pass on both drivers and to be load-bearing on MySQL, which is why they run there
 * before a commit:
 *
 *   docker compose --profile mysql run --rm app-mysql vendor/bin/pest
 *
 * Time is frozen at the timestamp the run's last write left, so the same-value update is
 * exact rather than a race with the second boundary: without it the UPDATE would
 * sometimes move updated_at, report one row on MySQL as well, and quietly test nothing.
 */
afterEach(function () {
    Carbon::setTestNow();
});

/**
 * A run parked in Waiting, re-read the way a second caller would hold it, with the clock
 * standing at the moment of its last write.
 */
function fenceSubject(): object
{
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $fresh = SagaFlow::findRun($run->id);

    Carbon::setTestNow($fresh->updated_at);

    return $fresh;
}

it('runs against the driver the harness was asked for', function () {
    // The worst outcome this harness could produce is a green MySQL run that quietly
    // fell back to SQLite. It cannot: a driver the connection does not report is a
    // failure, not a default.
    expect(DB::connection('testing')->getDriverName())->toBe(TestCase::driver());
});

it('counts a same-state transition with nothing of its own to write as a success', function () {
    $run = fenceSubject();

    // status is already waiting and the clock has not moved, so every value in the SET
    // equals what the row holds: zero changed rows on MySQL, one matched row on SQLite.
    // The guard did not fail — there was nothing to change.
    app(StateMachine::class)->transition($run, FlowStatus::Waiting);

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Waiting);
});

it('leaves the terminal marks alone when a terminal transition arrives twice', function () {
    $run = fenceSubject();

    SagaFlow::loadFlow($run->id)->cancel('operator');

    // At-least-once delivery does reach this: a duplicated batch callback lands a run
    // that is already Cancelled, and it must not move the moment the run ended. The
    // clock is advanced so a rewrite would be visible — which also means updated_at
    // genuinely changes, so this one says nothing about the driver.
    $cancelled = SagaFlow::findRun($run->id);

    Carbon::setTestNow($cancelled->updated_at->copy()->addMinute());

    app(StateMachine::class)->transition($cancelled, FlowStatus::Cancelled);

    $after = SagaFlow::findRun($run->id);

    expect($after->status)->toBe(FlowStatus::Cancelled)
        ->and($after->cancelled_at->equalTo($cancelled->cancelled_at))->toBeTrue()
        ->and($after->finished_at->equalTo($cancelled->finished_at))->toBeTrue();
});

it('counts a duplicate terminal transition landing in the same instant as a success', function () {
    $run = fenceSubject();

    SagaFlow::loadFlow($run->id)->cancel('operator');

    // The same shape as the test above, on the other path out of transition(): a
    // terminal target goes through finish(), which writes the run inside the settling
    // transaction. Nothing has moved and nothing is left to write, so MySQL reports zero
    // changed rows here too.
    $cancelled = SagaFlow::findRun($run->id);

    Carbon::setTestNow($cancelled->updated_at);

    app(StateMachine::class)->transition($cancelled, FlowStatus::Cancelled);

    expect(SagaFlow::findRun($run->id)->status)->toBe(FlowStatus::Cancelled);
});

it('still refuses a same-state transition when the row has moved on', function () {
    $stale = fenceSubject();

    SagaFlow::loadFlow($stale->id)->cancel('operator');

    // The mutation pair to the two above: reading zero rows as success unconditionally
    // would pass them and lose this one, which is the whole point of the fence.
    expect(fn () => app(StateMachine::class)->transition($stale, FlowStatus::Waiting))
        ->toThrow(ConcurrentFlowTransitionException::class);

    expect(SagaFlow::findRun($stale->id)->status)->toBe(FlowStatus::Cancelled);
});
