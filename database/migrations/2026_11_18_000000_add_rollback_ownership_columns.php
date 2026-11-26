<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    private const string STEP_UNIQUE = 'compensation_runs_run_step_unique';

    /**
     * Each change asks first: MySQL commits DDL on its own, so a migration that died midway reruns.
     */
    public function up(): void
    {
        $schema = Schema::connection($this->getConnection());
        $runs = $this->prefix().'flow_runs';
        $compensations = $this->prefix().'compensation_runs';

        $schema->table($runs, function (Blueprint $blueprint) use ($schema, $runs): void {
            if (! $schema->hasColumn($runs, 'rollback_token')) {
                $blueprint->ulid('rollback_token')->nullable()->after('expiry_available_at');
            }

            if (! $schema->hasColumn($runs, 'rollback_target')) {
                $blueprint->string('rollback_target')->nullable()->after('rollback_token');
            }

            if (! $schema->hasColumn($runs, 'rollback_cause')) {
                $blueprint->json('rollback_cause')->nullable()->after('rollback_target');
            }

            if (! $schema->hasColumn($runs, 'rollback_compensates')) {
                $blueprint->boolean('rollback_compensates')->default(true)->after('rollback_cause');
            }
        });

        if (! $schema->hasColumn($compensations, 'step_sequence')) {
            $schema->table($compensations, function (Blueprint $blueprint): void {
                $blueprint->unsignedInteger('step_sequence')->nullable()->after('sequence');
            });
        }

        if ($this->existingUnique() === null) {
            $schema->table($compensations, function (Blueprint $blueprint): void {
                $blueprint->unique(['flow_run_id', 'step_sequence'], $this->prefix().self::STEP_UNIQUE);
            });
        }
    }

    /**
     * Removes only what is there, so it can also undo a run that died part of the way.
     */
    public function down(): void
    {
        $schema = Schema::connection($this->getConnection());
        $runs = $this->prefix().'flow_runs';
        $compensations = $this->prefix().'compensation_runs';

        $unique = $this->existingUnique();

        if ($unique !== null) {
            $schema->table($compensations, fn (Blueprint $blueprint) => $blueprint->dropUnique($unique));
        }

        if ($schema->hasColumn($compensations, 'step_sequence')) {
            $schema->table($compensations, fn (Blueprint $blueprint) => $blueprint->dropColumn('step_sequence'));
        }

        $present = array_values(array_filter(
            ['rollback_token', 'rollback_target', 'rollback_cause', 'rollback_compensates'],
            fn (string $column): bool => $schema->hasColumn($runs, $column),
        ));

        if ($present !== []) {
            $schema->table($runs, fn (Blueprint $blueprint) => $blueprint->dropColumn($present));
        }
    }

    /**
     * PostgreSQL truncates an identifier past 63 bytes; only that truncation, over the same columns,
     * stands in for the full name.
     */
    private function existingUnique(): ?string
    {
        $schema = Schema::connection($this->getConnection());

        $wanted = strtolower($this->prefix().self::STEP_UNIQUE);
        $truncated = $schema->getConnection()->getDriverName() === 'pgsql' && strlen($wanted) > 63
            ? substr($wanted, 0, 63)
            : null;

        foreach ($schema->getIndexes($this->prefix().'compensation_runs') as $index) {
            $stored = (string) $index['name'];

            $sameName = strcasecmp($stored, $wanted) === 0
                || ($truncated !== null && strcasecmp($stored, $truncated) === 0);

            if ($sameName && $index['columns'] === ['flow_run_id', 'step_sequence'] && $index['unique']) {
                return $stored;
            }
        }

        return null;
    }

    /**
     * Named, or the migrator's transaction opens on the default connection rather than the package's.
     */
    public function getConnection(): ?string
    {
        return config('saga-lara-flow.database.connection');
    }

    private function prefix(): string
    {
        return (string) config('saga-lara-flow.database.table_prefix', '');
    }
};
