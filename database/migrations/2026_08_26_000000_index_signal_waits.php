<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Table => columns, index name. The name is given rather than derived: a derived
     * one here would be the longest identifier the package asks for, and MySQL
     * refuses one past 64 characters.
     */
    private const array INDEXES = [
        'flow_signals' => [['status', 'name', 'flow_run_id'], 'flow_signals_status_name_run_index'],
        'action_runs' => [['status', 'retry_signal', 'flow_run_id'], 'action_runs_status_signal_run_index'],
    ];

    /**
     * The shipped indexes on both tables lead with flow_run_id, which answers "what
     * is this run waiting for" — the opposite of what the FlowQuery wait filters
     * ask.
     */
    public function up(): void
    {
        foreach (self::INDEXES as $table => [$columns, $name]) {
            $this->add($table, $columns, $name);
        }
    }

    public function down(): void
    {
        foreach (array_reverse(self::INDEXES, preserve_keys: true) as $table => [$columns, $name]) {
            $this->drop($table, $columns, $name);
        }
    }

    /**
     * MySQL runs each statement on its own, so both directions ask first: a run
     * that died after the first index can simply be repeated.
     *
     * @param  list<string>  $columns
     */
    private function add(string $table, array $columns, string $name): void
    {
        if ($this->existing($table, $columns, $name) === null) {
            $this->change($table, fn (Blueprint $blueprint) => $blueprint->index($columns, $this->prefix().$name));
        }
    }

    /**
     * @param  list<string>  $columns
     */
    private function drop(string $table, array $columns, string $name): void
    {
        $existing = $this->existing($table, $columns, $name);

        if ($existing !== null) {
            $this->change($table, fn (Blueprint $blueprint) => $blueprint->dropIndex($existing));
        }
    }

    private function change(string $table, Closure $change): void
    {
        Schema::connection($this->connection())->table($this->prefix().$table, $change);
    }

    /**
     * The index this migration owns, under the name the driver actually stored —
     * PostgreSQL truncates an identifier past 63 bytes, which a long table prefix
     * reaches. Null when it is absent, and also when the name belongs to something
     * shaped differently: creating ours then fails loudly instead of quietly
     * standing down.
     *
     * @param  list<string>  $columns
     */
    private function existing(string $table, array $columns, string $name): ?string
    {
        $wanted = strtolower($this->prefix().$name);

        foreach (Schema::connection($this->connection())->getIndexes($this->prefix().$table) as $index) {
            $stored = (string) $index['name'];

            $sameName = strcasecmp($stored, $wanted) === 0
                || (strlen($stored) < strlen($wanted) && str_starts_with($wanted, strtolower($stored)));

            if ($sameName && $index['columns'] === $columns && ! $index['unique']) {
                return $stored;
            }
        }

        return null;
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
