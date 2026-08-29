<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * An index whose name Laravel derives carries the table prefix, and the longest of them
 * decides how long a prefix may be: MySQL refuses an identifier past 64 characters, and
 * PostgreSQL truncates at 63 bytes, where two names on one table can meet. The four
 * longest are therefore named here rather than derived; with the two in
 * index_signal_waits that leaves 24 bytes for a prefix, which the documentation states.
 */
return new class extends Migration
{
    public function up(): void
    {
        $schema = Schema::connection($this->connection());

        $prefix = $this->prefix();

        $schema->create($prefix.'flow_runs', function (Blueprint $table) use ($prefix): void {
            $table->ulid('id')->primary();
            $table->string('workflow_class');
            $table->string('workflow_name')->nullable();
            $table->string('workflow_version')->nullable();
            $table->string('status')->index();
            $table->json('arguments')->nullable();
            $table->json('result')->nullable();
            $table->json('exception')->nullable();
            $table->json('tenancy_context')->nullable();
            $table->string('connection')->nullable();
            $table->string('queue')->nullable();
            $table->ulid('parent_id')->nullable()->index();
            $table->string('parent_close_policy')->nullable();
            $table->unsignedInteger('current_sequence')->default(0);
            $table->timestamp('started_at')->nullable();
            $table->timestamp('finished_at')->nullable();
            $table->timestamp('expires_at')->nullable();
            $table->timestamp('cancelled_at')->nullable();
            $table->unsignedInteger('repair_attempts')->default(0);
            $table->timestamp('repair_available_at')->nullable();
            $table->timestamps();

            $table->index(['status', 'expires_at']);
            $table->index(['status', 'repair_available_at'], $prefix.'flow_runs_status_repair_index');
            $table->index(['workflow_class', 'status']);
        });

        $schema->create($prefix.'action_runs', function (Blueprint $table): void {
            $table->ulid('id')->primary();
            $table->ulid('flow_run_id')->index();
            $table->unsignedInteger('sequence');
            $table->string('action_class');
            $table->string('action_name')->nullable();
            $table->string('status')->index();
            $table->boolean('continue_on_failure')->default(false);
            $table->boolean('has_compensation')->default(false);
            $table->unsignedSmallInteger('parallel_group')->nullable();
            $table->json('arguments')->nullable();
            $table->json('result')->nullable();
            $table->json('exception')->nullable();
            $table->unsignedInteger('attempts')->default(0);
            $table->timestamp('started_at')->nullable();
            $table->timestamp('finished_at')->nullable();
            $table->timestamp('expires_at')->nullable();
            $table->unsignedInteger('repair_attempts')->default(0);
            $table->timestamp('repair_available_at')->nullable();
            $table->timestamps();

            $table->unique(['flow_run_id', 'sequence']);
        });

        $schema->create($prefix.'flow_events', function (Blueprint $table): void {
            $table->ulid('id')->primary();
            $table->ulid('flow_run_id')->index();
            $table->unsignedInteger('sequence')->nullable();
            $table->string('type')->index();
            $table->string('subject_type')->nullable();
            $table->string('subject_id')->nullable();
            $table->json('payload')->nullable();
            $table->timestamp('recorded_at')->index();
            $table->timestamps();
        });

        $schema->create($prefix.'flow_signals', function (Blueprint $table) use ($prefix): void {
            $table->ulid('id')->primary();
            $table->ulid('flow_run_id')->index();
            $table->string('name');
            $table->json('payload')->nullable();
            $table->string('status')->index();
            $table->unsignedInteger('wait_sequence')->nullable();
            $table->timestamp('timeout_at')->nullable()->index();
            $table->timestamp('received_at')->nullable();
            $table->timestamp('consumed_at')->nullable();
            $table->timestamps();

            $table->index(['flow_run_id', 'name', 'status'], $prefix.'flow_signals_run_name_status_index');
        });

        $schema->create($prefix.'compensation_runs', function (Blueprint $table): void {
            $table->ulid('id')->primary();
            $table->ulid('flow_run_id')->index();
            $table->ulid('action_run_id')->nullable()->index();
            $table->unsignedInteger('sequence');
            $table->string('compensation_type');
            $table->string('compensation_class')->nullable();
            $table->string('status')->index();
            $table->boolean('continue_on_failure')->default(false);
            $table->json('arguments')->nullable();
            $table->json('result')->nullable();
            $table->json('exception')->nullable();
            $table->timestamp('started_at')->nullable();
            $table->timestamp('finished_at')->nullable();
            $table->timestamps();
        });

        $schema->create($prefix.'side_effects', function (Blueprint $table): void {
            $table->ulid('id')->primary();
            $table->ulid('flow_run_id')->index();
            $table->unsignedInteger('sequence');
            $table->string('key')->nullable();
            $table->json('value')->nullable();
            $table->timestamps();

            $table->unique(['flow_run_id', 'sequence']);
        });

        $schema->create($prefix.'flow_tags', function (Blueprint $table): void {
            $table->id();
            $table->ulid('flow_run_id')->index();
            $table->string('key');
            $table->string('value')->nullable();
            $table->timestamps();

            $table->index(['key', 'value']);
            $table->unique(['flow_run_id', 'key', 'value']);
        });

        $schema->create($prefix.'flow_children', function (Blueprint $table) use ($prefix): void {
            $table->id();
            $table->ulid('parent_flow_run_id')->index();
            $table->ulid('child_flow_run_id')->index();
            $table->unsignedInteger('sequence');
            $table->string('child_workflow_class');
            $table->string('close_policy');
            $table->boolean('continue_parent_on_failure')->default(false);
            $table->string('status')->index();
            $table->timestamps();

            $table->unique(['parent_flow_run_id', 'sequence'], $prefix.'flow_children_parent_sequence_unique');
            $table->unique(['parent_flow_run_id', 'child_flow_run_id'], $prefix.'flow_children_parent_child_unique');
        });
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection());
        $prefix = $this->prefix();

        foreach ([
            'flow_children',
            'flow_tags',
            'side_effects',
            'compensation_runs',
            'flow_signals',
            'flow_events',
            'action_runs',
            'flow_runs',
        ] as $table) {
            $schema->dropIfExists($prefix.$table);
        }
    }

    private function connection(): ?string
    {
        return config('saga-lara-flow.database.connection');
    }

    private function prefix(): string
    {
        return (string) config('saga-lara-flow.database.table_prefix', '');
    }
};
