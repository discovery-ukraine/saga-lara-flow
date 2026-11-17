<?php

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Harness\ForkedRival;
use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Carbon;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\AssertionFailedError;

/**
 * Shared test helpers, loaded once via tests/Pest.php so every test file can use
 * them regardless of execution order.
 */
if (!function_exists('useDatabaseQueue')) {
    /**
     * Switch to a real database queue and a worker that drains it. This exercises
     * the genuine async path — RunWorkflowJob → RunActionJob → ResumeWorkflowJob,
     * and the child/saga jobs — one job at a time, without the inline recursion the
     * sync driver would cause.
     */
    function useDatabaseQueue(): void
    {
        config()->set('queue.default', 'database');
        config()->set('queue.connections.database', [
            'driver' => 'database',
            'connection' => 'testing',
            'table' => 'jobs',
            'queue' => 'default',
            'retry_after' => 90,
        ]);
        config()->set('saga-lara-flow.queue.after_commit', false);
        config()->set('saga-lara-flow.locks.enabled', false);
        config()->set('queue.failed.driver', 'null');
        config()->set('queue.batching', [
            'driver' => 'database',
            'connection' => 'testing',
            'database' => 'testing',
            'table' => 'job_batches',
        ]);

        if (!Schema::connection('testing')->hasTable('jobs')) {
            Schema::connection('testing')->create('jobs', function (Blueprint $table) {
                $table->bigIncrements('id');
                $table->string('queue')->index();
                $table->longText('payload');
                $table->unsignedTinyInteger('attempts');
                $table->unsignedInteger('reserved_at')->nullable();
                $table->unsignedInteger('available_at');
                $table->unsignedInteger('created_at');
            });
        }

        if (!Schema::connection('testing')->hasTable('job_batches')) {
            Schema::connection('testing')->create('job_batches', function (Blueprint $table) {
                $table->string('id')->primary();
                $table->string('name');
                $table->integer('total_jobs');
                $table->integer('pending_jobs');
                $table->integer('failed_jobs');
                $table->longText('failed_job_ids');
                $table->mediumText('options')->nullable();
                $table->integer('cancelled_at')->nullable();
                $table->integer('created_at');
                $table->integer('finished_at')->nullable();
            });
        }
    }

    function drainQueue(): void
    {
        $waitedOut = null;

        while (true) {
            // --sleep=0 matters more than it looks: Worker::daemon() sleeps BEFORE it
            // checks stopWhenEmpty, so every drain would otherwise pay the default three
            // seconds after the queue is already empty.
            //
            // --memory is measured against the whole PHP process, not the jobs. The worker
            // runs inside the test run, so at the default 128 it stops silently the moment
            // the suite's own footprint passes it, leaving the queue full and the run parked.
            Artisan::call('queue:work', [
                '--stop-when-empty' => true,
                '--sleep' => 0,
                '--memory' => 4096,
                '--no-interaction' => true,
            ]);

            $left = DB::connection('testing')->table('jobs')->orderBy('id')->get();

            if ($left->isEmpty()) {
                return;
            }

            // The clock DatabaseQueue compares available_at and reserved_at against.
            $now = Carbon::now()->getTimestamp();

            $wait = drainQueueClockStepBack($left, $now);

            // The same jobs still there after a wait: the clock did not catch up with them.
            if ($wait === null || $left->pluck('id')->all() === $waitedOut) {
                drainQueueFailWith($left, $now);
            }

            $waitedOut = $left->pluck('id')->all();

            // Bounded by hrtime, which never steps: a clock that stalls or steps back again
            // must end in the failure above, not in a wait on itself.
            $until = hrtime(true) + $wait * 1_000_000_000;

            while (Carbon::now()->getTimestamp() < $now + $wait && hrtime(true) < $until) {
                usleep(50_000);
            }
        }
    }

    /**
     * Nothing in the engine delays a job, so a job the worker could not take yet was
     * pushed before the wall clock stepped back: available_at is stored to the second,
     * and a step of a second or more hides it until the clock catches up. WSL2's clock
     * has been measured stepping back by up to 1.27 s, which with the second's rounding
     * puts such a job at most 3 s ahead. Anything else is a real leftover.
     *
     * @param  Collection<int, object>  $left
     * @return ?int seconds to wait, or null when the leftovers are not a clock step
     */
    function drainQueueClockStepBack(Collection $left, int $now): ?int
    {
        $ahead = $left->map(fn(object $row): int => $row->available_at - $now);

        if ($ahead->contains(fn(int $seconds): bool => $seconds < 1 || $seconds > 3)) {
            return null;
        }

        return $ahead->max();
    }

    /**
     * The worker stops at the first pop that finds nothing it may take and says nothing
     * about what it left. Fail where it stopped, naming each job, rather than at a status
     * assertion further down that only shows a run come up short.
     *
     * @param  Collection<int, object>  $left
     */
    function drainQueueFailWith(Collection $left, int $now): never
    {
        $lines = $left->map(function (object $row) use ($now): string {
            $payload = json_decode((string) $row->payload, true);

            return sprintf(
                '#%d %s on "%s": attempts %d, reserved_at %s, available_at %d (%+d s from now)',
                $row->id,
                is_array($payload) ? ($payload['displayName'] ?? '?') : '?',
                $row->queue,
                $row->attempts,
                $row->reserved_at === null ? 'null' : sprintf('%d (%+d s from now)', $row->reserved_at,
                    $row->reserved_at - $now),
                $row->available_at,
                $row->available_at - $now,
            );
        });

        Assert::fail(sprintf(
            "drainQueue() stopped with %d job(s) still on the queue at %d:\n%s",
            $left->count(),
            $now,
            $lines->implode("\n"),
        ));
    }

    /**
     * Process exactly one queued job, so a test can inspect the run between two steps
     * of the async path instead of after the whole queue has drained.
     */
    function workOneJob(): void
    {
        // The flags carry the same weight as in drainQueue(): --sleep=0 so an empty
        // queue costs nothing, --memory so the suite's own footprint cannot stop the
        // worker silently.
        Artisan::call('queue:work', [
            '--once' => true,
            '--sleep' => 0,
            '--memory' => 4096,
            '--no-interaction' => true,
        ]);
    }

    /**
     * Snapshot of the durable state a run leaves behind, for cross-mode comparison.
     *
     * @return array<string, mixed>
     */
    function runStateSnapshot(string $flowRunId): array
    {
        $run = SagaFlow::findRun($flowRunId);

        $actions = $run->actions()
            ->orderBy('sequence')
            ->get()
            ->map(fn($action) => [
                'sequence' => $action->sequence,
                'status' => $action->status,
                'action_class' => $action->action_class,
                'result' => $action->result,
            ])
            ->all();

        return ['status' => $run->status, 'actions' => $actions];
    }
}

if (!function_exists('logToFile')) {
    /**
     * Point the package's anomaly log at a real single-file channel. The project uses
     * no mocks, so the assertions read the file an operator would actually grep.
     */
    function logToFile(string $path): void
    {
        config()->set('logging.channels.saga_test', [
            'driver' => 'single',
            'path' => $path,
            'level' => 'debug',
        ]);

        config()->set('saga-lara-flow.logging.channel', 'saga_test');
    }
}

if (!function_exists('readsFromWriter')) {
    /**
     * Whether the connection has been told to read from the writer, which it keeps to itself.
     */
    function readsFromWriter(Connection $connection): bool
    {
        return (bool) (new ReflectionProperty($connection, 'readOnWriteConnection'))->getValue($connection);
    }
}

if (!function_exists('rivalSession')) {
    /**
     * The rival's session: its own server session, transaction, locks and snapshot.
     * In a forked rival it is a session of the child's own.
     */
    function rivalSession(): Connection
    {
        return DB::connection('testing_rival');
    }

    /**
     * The server's id for a session, as its lock catalog names it.
     */
    function sessionId(Connection $session): int
    {
        return (int) $session->scalar(match ($session->getDriverName()) {
            'pgsql' => 'select pg_backend_pid()',
            'mysql' => 'select connection_id()',
        });
    }

    /**
     * How long a statement in this session waits on another session's lock before it
     * fails. A test that stages a lock fails on it instead of hanging.
     */
    function lockTimeout(Connection $session, int $seconds): void
    {
        $session->statement(match ($session->getDriverName()) {
            'pgsql' => "set lock_timeout = '{$seconds}s'",
            'mysql' => "set session innodb_lock_wait_timeout = {$seconds}",
        });
    }

    /**
     * The isolation level of this session's next transactions, read back so a level the
     * server did not take fails here rather than in what the test goes on to measure.
     */
    function isolation(Connection $session, string $level): void
    {
        $session->statement(match ($session->getDriverName()) {
            'pgsql' => "set session characteristics as transaction isolation level {$level}",
            'mysql' => "set session transaction isolation level {$level}",
        });

        $taken = (string) $session->scalar(match ($session->getDriverName()) {
            'pgsql' => 'show default_transaction_isolation',
            'mysql' => 'select @@session.transaction_isolation',
        });

        Assert::assertSame($level, strtolower(str_replace('-', ' ', $taken)),
            'The session did not take the isolation level.');
    }

    /**
     * Run $move once, immediately before the first statement on $connection whose SQL
     * matches $pattern. The engine's statement then runs against the state $move left.
     *
     * @return Closure(): bool whether the break point was reached
     */
    function beforeStatement(string $pattern, Closure $move, string $connection = 'testing'): Closure
    {
        $reached = false;

        DB::connection($connection)->beforeExecuting(function (string $query) use ($pattern, $move, &$reached): void {
            if ($reached || preg_match($pattern, $query) !== 1) {
                return;
            }

            $reached = true;
            $move();
        });

        return function () use (&$reached): bool {
            return $reached;
        };
    }

    /**
     * Whether $session is waiting right now on a lock $holder holds, by the server's own
     * catalog. Naming the holder keeps a brief wait on anything else from counting.
     */
    function waitsOnLock(int $session, int $holder): bool
    {
        $observer = DB::connection('testing_observer');

        return (bool) $observer->scalar(match ($observer->getDriverName()) {
            'pgsql' => 'select count(*) from unnest(pg_blocking_pids(?)) as blocker where blocker = ?',
            'mysql' => 'select count(*) from performance_schema.data_lock_waits w '
                .'join performance_schema.threads waiting on waiting.thread_id = w.requesting_thread_id '
                .'join performance_schema.threads holding on holding.thread_id = w.blocking_thread_id '
                .'where waiting.processlist_id = ? and holding.processlist_id = ?',
        }, [$session, $holder]);
    }

    /**
     * Run $engineWrite against $rivalMove held uncommitted in a forked rival on a session
     * of its own. The rival commits the moment the engine is seen waiting on it, or once
     * the engine has finished without waiting, so either order is reached without a sleep
     * and the result says which one the server chose. $isolation, when given, is set on
     * both sessions: the rival's is a fresh one and would otherwise run at the server's
     * default, whatever the test set on the engine's.
     *
     * @return array{waited: bool, returned: mixed, threw: ?Throwable}
     */
    function againstUncommitted(
        Closure $rivalMove,
        Closure $engineWrite,
        ?string $isolation = null,
        float $seconds = 5.0
    ): array {
        if ($isolation !== null) {
            isolation(DB::connection('testing'), $isolation);
        }

        $engineSession = sessionId(DB::connection('testing'));

        $rival = ForkedRival::start(function (ForkedRival $engine) use (
            $rivalMove,
            $engineSession,
            $isolation,
            $seconds
        ): bool {
            $session = DB::connection('testing');
            lockTimeout($session, 10);

            if ($isolation !== null) {
                isolation($session, $isolation);
            }

            $rivalSession = sessionId($session);
            $session->beginTransaction();

            $rivalMove();

            $engine->arrive('rival move in flight');

            $deadline = hrtime(true) + (int) ($seconds * 1e9);

            while (!$engine->reached('engine write done')) {
                if (waitsOnLock($engineSession, $rivalSession)) {
                    $session->commit();

                    return true;
                }

                if (hrtime(true) >= $deadline) {
                    throw new AssertionFailedError(sprintf(
                        "Barrier 'engine write done': the engine neither waited on the rival nor finished within %.1f s.",
                        $seconds,
                    ));
                }

                usleep(1000);
            }

            $session->commit();

            return false;
        });

        $rival->await('rival move in flight', $seconds);

        $returned = $threw = null;

        try {
            $returned = $engineWrite();
        } catch (Throwable $e) {
            $threw = $e;
        }

        $rival->arrive('engine write done');

        return ['waited' => $rival->finish($seconds), 'returned' => $returned, 'threw' => $threw];
    }

    /**
     * Wait until $session is seen waiting on a lock $holder holds, or fail naming the barrier.
     */
    function awaitLockWait(int $session, int $holder, string $barrier, float $seconds = 5.0): void
    {
        $deadline = hrtime(true) + (int) ($seconds * 1e9);

        while (!waitsOnLock($session, $holder)) {
            if (hrtime(true) >= $deadline) {
                throw new AssertionFailedError(sprintf(
                    "Barrier '%s': session %d was not seen waiting on session %d within %.1f s.", $barrier, $session,
                    $holder, $seconds,
                ));
            }

            usleep(1000);
        }
    }
}
