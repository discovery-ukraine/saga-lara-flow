<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Harness\ForkedRival;
use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use PHPUnit\Framework\AssertionFailedError;
use Symfony\Component\Uid\Ulid;

/**
 * The harness itself: a second session on the suite's server, a break point before an
 * engine statement, and a rival in a process of its own for an interleaving that blocks.
 *
 * Server only. On SQLite :memory: is a database per connection, and a file database
 * admits one writer at a time, so neither interleaving can occur there.
 */
function harnessRun(): FlowRun
{
    return app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);
}

beforeEach(function () {
    if (TestCase::driver() === 'sqlite') {
        $this->markTestSkipped('A second session needs a server: SQLite :memory: is a database per connection.');
    }
});

it('hides a rival write until the rival commits it', function () {
    $run = harnessRun();

    rivalSession()->beginTransaction();
    FlowRun::on('testing_rival')->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);

    expect(FlowRun::query()->find($run->id)->status)->toBe(FlowStatus::Waiting);

    rivalSession()->commit();

    expect(FlowRun::query()->find($run->id)->status)->toBe(FlowStatus::Cancelling);
});

it('rolls back a rival transaction the test left open', function () {
    $run = harnessRun();

    rivalSession()->beginTransaction();
    FlowRun::on('testing_rival')->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);

    DB::purge('testing_rival');

    // A lock the purged session still held would stall this update until the timeout.
    lockTimeout(DB::connection('testing'), 1);
    FlowRun::query()->whereKey($run->id)->update(['status' => FlowStatus::Running]);

    expect(FlowRun::query()->find($run->id)->status)->toBe(FlowStatus::Running);
});

it('runs a rival move once, just before the first matching engine statement', function () {
    $run = harnessRun();
    $seen = [];

    $reached = beforeStatement('/^update .*flow_runs/i', function () use ($run, &$seen) {
        $seen[] = 'move';
        FlowRun::on('testing_rival')->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);
    });

    // A listener hears every connection; the rival's own update is not the engine's.
    DB::listen(function ($query) use (&$seen) {
        if ($query->connectionName === 'testing') {
            $seen[] = strtok($query->sql, ' ');
        }
    });

    FlowRun::query()->find($run->id);
    $changed = FlowRun::query()->whereKey($run->id)->where('status', FlowStatus::Waiting)->update(['status' => FlowStatus::Running]);
    FlowRun::query()->whereKey($run->id)->update(['updated_at' => now()]);

    expect($reached())->toBeTrue()
        ->and($seen)->toBe(['select', 'move', 'update', 'update'])
        ->and($changed)->toBe(0)
        ->and(FlowRun::query()->find($run->id)->status)->toBe(FlowStatus::Cancelling);
});

it('reads by each session\'s own isolation level', function (string $level, FlowStatus $reread) {
    $run = harnessRun();
    $engine = DB::connection('testing');

    isolation($engine, $level);

    $engine->transaction(function () use ($run, &$first, &$second) {
        $first = FlowRun::query()->find($run->id)->status;

        FlowRun::on('testing_rival')->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);

        $second = FlowRun::query()->find($run->id)->status;
    });

    expect($first)->toBe(FlowStatus::Waiting)->and($second)->toBe($reread);
})->with([
    'read committed sees the rival commit' => ['read committed', FlowStatus::Cancelling],
    'repeatable read keeps its snapshot' => ['repeatable read', FlowStatus::Waiting],
]);

it('stops an engine statement on a rival lock after the lock timeout', function () {
    $run = harnessRun();

    rivalSession()->beginTransaction();
    FlowRun::on('testing_rival')->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);

    lockTimeout(DB::connection('testing'), 1);
    $started = hrtime(true);

    expect(fn () => DB::connection('testing')->transaction(
        fn () => FlowRun::query()->whereKey($run->id)->lockForUpdate()->first(),
    ))->toThrow(QueryException::class);

    expect((hrtime(true) - $started) / 1e9)->toBeLessThan(3.0);
});

it('makes an engine row lock wait on a forked rival, which commits once it sees the wait', function () {
    $run = harnessRun();
    $engine = DB::connection('testing');
    // Longer than any barrier: a rival that gives up dies and frees the row, so the
    // test fails on the barrier's name rather than on the engine's lock timeout.
    lockTimeout($engine, 10);
    $engineSession = sessionId($engine);

    $rival = ForkedRival::start(function (ForkedRival $parent) use ($run, $engineSession) {
        lockTimeout(rivalSession(), 10);
        $rivalSession = sessionId(rivalSession());
        rivalSession()->beginTransaction();
        FlowRun::on('testing_rival')->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);

        // The engine is still waiting on the barrier below, not on a lock.
        if (waitsOnLock($engineSession, $rivalSession)) {
            throw new RuntimeException('An idle engine session was reported waiting on a lock.');
        }

        $parent->arrive('rival holds the row');

        awaitLockWait($engineSession, $rivalSession, 'engine waits on the row');

        rivalSession()->commit();
    });

    $rival->await('rival holds the row');

    $status = $engine->transaction(
        fn () => FlowRun::query()->whereKey($run->id)->lockForUpdate()->first()->status,
    );

    $rival->finish();

    expect($status)->toBe(FlowStatus::Cancelling)
        ->and(sessionId($engine))->toBe($engineSession);
});

it('fails by timeout naming a barrier the rival never reaches', function () {
    $rival = ForkedRival::start(function (ForkedRival $parent) {
        $parent->await('never', 30);
    });

    $started = hrtime(true);

    expect(fn () => $rival->await('rival holds the row', 0.5))
        ->toThrow(AssertionFailedError::class, "did not reach barrier 'rival holds the row' within 0.5 s");

    expect((hrtime(true) - $started) / 1e9)->toBeLessThan(2.0);
});

it('fails by timeout even when signals interrupt the wait', function () {
    // A handler makes a delivered signal interrupt the select the wait sleeps in.
    $previous = pcntl_signal_get_handler(SIGUSR1);
    pcntl_signal(SIGUSR1, static function (): void {});

    try {
        $parentPid = getmypid();

        $rival = ForkedRival::start(function (ForkedRival $parent) use ($parentPid) {
            for ($i = 0; $i < 40; $i++) {
                posix_kill($parentPid, SIGUSR1);
                usleep(25_000);
            }

            $parent->await('never', 30);
        });

        $started = hrtime(true);

        expect(fn () => $rival->await('rival holds the row', 0.5))
            ->toThrow(AssertionFailedError::class, "did not reach barrier 'rival holds the row' within 0.5 s");

        expect((hrtime(true) - $started) / 1e9)->toBeLessThan(2.0);
    } finally {
        // The rival is still signalling: under the default handler one more would end the
        // whole run, so it goes before the handler does.
        ForkedRival::reapAll();
        pcntl_signal(SIGUSR1, $previous);
    }
});

it('does not take a barrier named done for the end of the rival\'s body', function () {
    $rival = ForkedRival::start(function (ForkedRival $parent) {
        $parent->arrive('done');
        $parent->await('never', 30);
    });

    $started = hrtime(true);

    expect(fn () => $rival->finish(0.5))
        ->toThrow(AssertionFailedError::class, 'did not reach the end of its body within 0.5 s');

    expect((hrtime(true) - $started) / 1e9)->toBeLessThan(2.0);
});

it('fails naming the barrier a rival gave up at', function () {
    $run = harnessRun();
    $engineSession = sessionId(DB::connection('testing'));

    // The engine never takes the lock, so the rival never sees it wait.
    $rival = ForkedRival::start(function () use ($engineSession) {
        awaitLockWait($engineSession, sessionId(rivalSession()), 'engine waits on the row', 0.5);
    });

    expect(fn () => $rival->finish())
        ->toThrow(AssertionFailedError::class, "Barrier 'engine waits on the row': session {$engineSession} was not seen waiting");

    expect($run->exists)->toBeTrue();
});

it('fails at once when the rival ends before a barrier', function () {
    $rival = ForkedRival::start(function () {
        throw new RuntimeException('rival broke');
    });

    $started = hrtime(true);

    expect(fn () => $rival->await('rival holds the row'))
        ->toThrow(AssertionFailedError::class, "failed before barrier 'rival holds the row': RuntimeException: rival broke");

    expect((hrtime(true) - $started) / 1e9)->toBeLessThan(1.0);
});

it('keeps the engine\'s session and transaction when a rival exits', function () {
    $run = harnessRun();
    $engine = DB::connection('testing');
    $engineSession = sessionId($engine);

    $engine->beginTransaction();
    FlowRun::query()->whereKey($run->id)->update(['status' => FlowStatus::Running]);

    $rival = ForkedRival::start(function () {
        exit(0);
    });

    expect(fn () => $rival->finish())
        ->toThrow(AssertionFailedError::class, 'ended before the end of its body');

    // Inside a transaction Laravel does not reconnect: a closed session would fail here.
    expect(sessionId($engine))->toBe($engineSession)
        ->and(FlowRun::query()->find($run->id)->status)->toBe(FlowStatus::Running);

    $engine->commit();
});

it('reports a rival failure whose message is not UTF-8', function () {
    $rival = ForkedRival::start(function () {
        throw new RuntimeException("rival broke on \xB1");
    });

    expect(fn () => $rival->finish())
        ->toThrow(AssertionFailedError::class, 'RuntimeException: rival broke on');
});

it('reports a value the rival cannot hand back', function () {
    $rival = ForkedRival::start(fn () => INF);

    expect(fn () => $rival->finish())
        ->toThrow(AssertionFailedError::class, 'failed before the end of its body: JsonException');
});

it('gives a forked rival sessions of its own and leaves the engine\'s alone', function () {
    $run = harnessRun();
    $engine = DB::connection('testing');
    $engineSession = sessionId($engine);

    $rivalSession = ForkedRival::start(function () {
        FlowRun::query()->count();

        return sessionId(DB::connection('testing'));
    })->finish();

    ForkedRival::reapAll();

    // Laravel reconnects a session the server closed without saying so; the id would not survive.
    expect($rivalSession)->not->toBe($engineSession)
        ->and(sessionId($engine))->toBe($engineSession)
        ->and(FlowRun::query()->find($run->id)->status)->toBe(FlowStatus::Waiting);
});

it('gives a forked rival ids of its own', function () {
    // Parked in a millisecond the clock has not reached, the generator only increments
    // what it inherited — on both sides of the fork, unless the child forgets it.
    $park = Closure::bind(static function (string $time): void {
        self::$time = $time;
    }, null, Ulid::class);

    // One id first, so the generator has a random part to increment.
    Str::ulid();
    $park('99999999999999');

    try {
        $rival = ForkedRival::start(fn () => (string) Str::ulid());
        $engine = (string) Str::ulid();

        expect($rival->finish())->not->toBe($engine);
    } finally {
        $park('');
    }
});

it('frees the rows a rival held once it is reaped', function () {
    $run = harnessRun();

    $rival = ForkedRival::start(function (ForkedRival $parent) use ($run) {
        rivalSession()->beginTransaction();
        FlowRun::on('testing_rival')->whereKey($run->id)->update(['status' => FlowStatus::Cancelling]);
        $parent->arrive('rival holds the row');
        $parent->await('never', 30);
    });

    $rival->await('rival holds the row');
    ForkedRival::reapAll();

    lockTimeout(DB::connection('testing'), 1);
    FlowRun::query()->whereKey($run->id)->update(['status' => FlowStatus::Running]);

    expect(FlowRun::query()->find($run->id)->status)->toBe(FlowStatus::Running);
});
