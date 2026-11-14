<?php

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Carbon;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use PHPUnit\Framework\Assert;

/**
 * Shared test helpers, loaded once via tests/Pest.php so every test file can use
 * them regardless of execution order.
 */
if (! function_exists('useDatabaseQueue')) {
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

        if (! Schema::connection('testing')->hasTable('jobs')) {
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

        if (! Schema::connection('testing')->hasTable('job_batches')) {
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
        $ahead = $left->map(fn (object $row): int => $row->available_at - $now);

        if ($ahead->contains(fn (int $seconds): bool => $seconds < 1 || $seconds > 3)) {
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
                $row->reserved_at === null ? 'null' : sprintf('%d (%+d s from now)', $row->reserved_at, $row->reserved_at - $now),
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
            ->map(fn ($action) => [
                'sequence' => $action->sequence,
                'status' => $action->status,
                'action_class' => $action->action_class,
                'result' => $action->result,
            ])
            ->all();

        return ['status' => $run->status, 'actions' => $actions];
    }
}

if (! function_exists('logToFile')) {
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
