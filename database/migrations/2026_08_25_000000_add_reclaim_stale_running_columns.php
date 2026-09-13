<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Table => the columns this migration adds to it.
     */
    private const array COLUMNS = [
        'action_runs' => ['reclaim_stale_after_seconds', 'reclaim_stale_at'],
        'compensation_runs' => ['reclaim_stale_after_seconds', 'reclaim_stale_at', 'attempts'],
    ];

    /**
     * Each column and index asks first. MySQL commits DDL on its own, so a run that
     * died part of the way through has to be repeatable rather than trip over what it
     * added.
     */
    public function up(): void
    {
        $schema = Schema::connection($this->getConnection());

        $actionRuns = $this->prefix().'action_runs';

        $schema->table($actionRuns, function (Blueprint $table) use ($schema, $actionRuns): void {
            if (! $schema->hasColumn($actionRuns, 'reclaim_stale_after_seconds')) {
                $table->unsignedInteger('reclaim_stale_after_seconds')->nullable()->after('started_at');
            }

            if (! $schema->hasColumn($actionRuns, 'reclaim_stale_at')) {
                $table->timestamp('reclaim_stale_at')->nullable()->after('reclaim_stale_after_seconds');
            }
        });

        $compensationRuns = $this->prefix().'compensation_runs';

        $schema->table($compensationRuns, function (Blueprint $table) use ($schema, $compensationRuns): void {
            if (! $schema->hasColumn($compensationRuns, 'reclaim_stale_after_seconds')) {
                $table->unsignedInteger('reclaim_stale_after_seconds')->nullable()->after('started_at');
            }

            if (! $schema->hasColumn($compensationRuns, 'reclaim_stale_at')) {
                $table->timestamp('reclaim_stale_at')->nullable()->after('reclaim_stale_after_seconds');
            }

            if (! $schema->hasColumn($compensationRuns, 'attempts')) {
                $table->unsignedInteger('attempts')->default(0)->after('continue_on_failure');
            }
        });

        foreach (array_keys(self::COLUMNS) as $name) {
            $table = $this->prefix().$name;

            if ($this->ownedIndex($table) === null) {
                $schema->table($table, fn (Blueprint $blueprint) => $blueprint->index('reclaim_stale_at'));
            }
        }
    }

    /**
     * Removes only what is there, so it can also undo a run that died part of the way.
     *
     * Dropping a column takes every index over it along — MySQL and PostgreSQL do so
     * silently. So before anything is dropped, an index that covers one of these
     * columns and is not this migration's own stops the rollback: it is the host's to
     * remove, not ours to lose.
     */
    public function down(): void
    {
        $schema = Schema::connection($this->getConnection());

        foreach (self::COLUMNS as $name => $columns) {
            $table = $this->prefix().$name;
            $owned = $this->ownedIndex($table);

            foreach ($schema->getIndexes($table) as $index) {
                if ((string) $index['name'] !== $owned && ! $index['primary']
                    && array_intersect($index['columns'], $columns) !== []) {
                    throw new RuntimeException(
                        "Index [{$index['name']}] on [{$table}] covers a column this rollback would drop, and "
                        .'the package did not create it. Drop it first, or it would go with the column.'
                    );
                }
            }
        }

        foreach (self::COLUMNS as $name => $columns) {
            $table = $this->prefix().$name;

            $index = $this->ownedIndex($table);

            if ($index !== null) {
                $schema->table($table, fn (Blueprint $blueprint) => $blueprint->dropIndex($index));
            }

            $present = array_values(array_filter(
                $columns,
                fn (string $column): bool => $schema->hasColumn($table, $column),
            ));

            if ($present !== []) {
                $schema->table($table, fn (Blueprint $blueprint) => $blueprint->dropColumn($present));
            }
        }
    }

    /**
     * The migrator opens its transaction on the connection a migration names. Left
     * unnamed, that is the default connection while the columns go to the package's
     * own, and a failure there rolls nothing back.
     */
    public function getConnection(): ?string
    {
        return config('saga-lara-flow.database.connection');
    }

    /**
     * The reclaim_stale_at index this migration owns, under the name the driver
     * actually stored. The name is the derived one exactly, or — past 63 bytes — the
     * 63 bytes PostgreSQL truncates it to; any other shorter name is a host's. The
     * column and the shape have to match too: a host index over the same column under
     * another name, or a unique one, is not ours to count as created or to drop.
     */
    private function ownedIndex(string $table): ?string
    {
        $schema = Schema::connection($this->getConnection());
        $connection = $schema->getConnection();

        // The name Blueprint derives for $table->index('reclaim_stale_at').
        $derivedFrom = $connection->getConfig('prefix_indexes') ? $connection->getTablePrefix().$table : $table;
        $wanted = str_replace(['-', '.'], '_', strtolower($derivedFrom.'_reclaim_stale_at_index'));

        foreach ($schema->getIndexes($table) as $index) {
            $stored = (string) $index['name'];

            $sameName = strcasecmp($stored, $wanted) === 0
                || (strlen($wanted) > 63 && strcasecmp($stored, substr($wanted, 0, 63)) === 0);

            if ($sameName && $index['columns'] === ['reclaim_stale_at'] && ! $index['unique']) {
                return $stored;
            }
        }

        return null;
    }

    private function prefix(): string
    {
        return (string) config('saga-lara-flow.database.table_prefix', '');
    }
};
