<?php

use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Schema;

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
        Artisan::call('queue:work', [
            '--stop-when-empty' => true,
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
