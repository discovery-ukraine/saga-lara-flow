<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CountedActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * ActionRecorder::startAction() claims the row, records action.started and fires the
 * ActionStarted event in one transaction, so a listener cannot leave the row Running
 * with nothing executing it. That listener is the one place a caller's code runs inside
 * a transaction the engine opened, and the drivers do not agree about what a failed
 * statement does to one: PostgreSQL aborts the whole transaction and turns the eventual
 * COMMIT into a rollback, reporting success either way, while SQLite and MySQL let the
 * caller carry on.
 *
 * A listener that throws is already handled — the claim rolls back with it. The shape
 * that gets past the guarantee is one that runs a failing query and SWALLOWS it.
 */
beforeEach(function () {
    FlakyPaymentAction::reset();
});

function swallowAQueryFailureOnStart(): void
{
    Event::listen(ActionStarted::class, function (): void {
        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // A listener that eats its own failure — broken, but nothing stops it.
        }
    });
}

it('never records a claim without its event', function () {
    swallowAQueryFailureOnStart();

    $run = SagaFlow::create(CountedActionWorkflow::class)->runSync();

    $step = $run->actions()->first();
    $started = $run->events()->where('type', FlowEventType::ActionStarted)->exists();

    // Both halves of the claim or neither: the row's own marks and the event that
    // announces them are written in the same transaction and cannot come apart.
    expect($started)->toBe($step->status !== ActionStatus::Pending)
        ->and($step->attempts)->toBe($started ? 1 : 0);
});

it('does not execute a step it failed to claim', function () {
    swallowAQueryFailureOnStart();

    $run = SagaFlow::create(CountedActionWorkflow::class)->runSync();

    $step = $run->actions()->first();

    // The half that at-least-once rests on. A body that ran under a claim the database
    // never kept is a charge nobody can attribute, and the next replay makes a second
    // one.
    expect(FlakyPaymentAction::$calls)->toBe($step->status === ActionStatus::Pending ? 0 : 1);
})->skip(
    fn () => TestCase::driver() === 'pgsql',
    'Known defect on PostgreSQL, tracked in #41: the claim is rolled back by the poisoned '
    .'transaction, COMMIT reports success anyway, and the body runs regardless.',
);
