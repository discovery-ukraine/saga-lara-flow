<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Narrow the tag unique to the pair every writer matches on. The shipped
     * (flow_run_id, key, value) let two concurrent first writes for one key insert
     * two rows with different values.
     *
     * The narrower unique goes on before the wider one comes off, and each step
     * asks first. MySQL commits each ALTER on its own, so whatever point this dies
     * at leaves a state the next run carries on from — never one where the table
     * holds no unique at all.
     */
    public function up(): void
    {
        if ($this->existing($this->narrow(), ['flow_run_id', 'key']) === null) {
            $this->collapseDuplicateTagKeys();

            $this->change(fn (Blueprint $table) => $table->unique(['flow_run_id', 'key'], $this->narrow()));
        }

        $wide = $this->existing($this->wide(), ['flow_run_id', 'key', 'value']);

        if ($wide !== null) {
            $this->change(fn (Blueprint $table) => $table->dropUnique($wide));
        }
    }

    public function down(): void
    {
        if ($this->existing($this->wide(), ['flow_run_id', 'key', 'value']) === null) {
            $this->change(fn (Blueprint $table) => $table->unique(['flow_run_id', 'key', 'value'], $this->wide()));
        }

        $narrow = $this->existing($this->narrow(), ['flow_run_id', 'key']);

        if ($narrow !== null) {
            $this->change(fn (Blueprint $table) => $table->dropUnique($narrow));
        }
    }

    /**
     * For each diverged key keep the highest updated_at, breaking a tie on the
     * highest id, and remove the rest. Groups are re-queried a page at a time
     * rather than streamed: every pass leaves the keys it saw with one row each, so
     * the set shrinks to nothing, no cursor stays open across the deletes, and
     * memory does not grow with the damage.
     */
    private function collapseDuplicateTagKeys(): void
    {
        $connection = DB::connection($this->getConnection());

        while (true) {
            $duplicates = $connection->table($this->table())
                ->useWritePdo()
                ->select('flow_run_id', 'key')
                ->groupBy('flow_run_id', 'key')
                ->havingRaw('COUNT(*) > 1')
                ->limit(500)
                ->get();

            if ($duplicates->isEmpty()) {
                return;
            }

            foreach ($duplicates as $duplicate) {
                // updateOrCreate rewrites a row in place, so the highest id is not
                // necessarily the value written last. A row without a timestamp is
                // ranked oldest explicitly, since drivers disagree on where a null
                // sorts. Timestamps are second-precision, so writes inside one
                // second fall back to insertion order.
                $keep = $connection->table($this->table())
                    ->useWritePdo()
                    ->where('flow_run_id', $duplicate->flow_run_id)
                    ->where('key', $duplicate->key)
                    ->orderByRaw('CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END')
                    ->orderByDesc('updated_at')
                    ->orderByDesc('id')
                    ->value('id');

                $connection->table($this->table())
                    ->where('flow_run_id', $duplicate->flow_run_id)
                    ->where('key', $duplicate->key)
                    ->where('id', '!=', $keep)
                    ->delete();
            }
        }
    }

    private function change(Closure $change): void
    {
        Schema::connection($this->getConnection())->table($this->table(), $change);
    }

    /**
     * The unique this migration owns, under the name the driver actually stored —
     * PostgreSQL truncates an identifier past 63 bytes, and only that exact truncation
     * stands in for the full name: any other shorter name is a host's. Both the name
     * and the shape have to match: a host index over the same columns may be partial
     * or functional and is not ours to drop, and a differently shaped index holding
     * the name must make the create fail loudly rather than leave the table with no
     * unique at all.
     *
     * @param  list<string>  $columns
     */
    private function existing(string $wanted, array $columns): ?string
    {
        $schema = Schema::connection($this->getConnection());

        $truncated = $schema->getConnection()->getDriverName() === 'pgsql' && strlen($wanted) > 63
            ? substr($wanted, 0, 63)
            : null;

        foreach ($schema->getIndexes($this->table()) as $index) {
            $name = (string) $index['name'];

            $sameName = strcasecmp($name, $wanted) === 0
                || ($truncated !== null && strcasecmp($name, $truncated) === 0);

            if ($sameName && $index['columns'] === $columns && $index['unique']) {
                return $name;
            }
        }

        return null;
    }

    private function narrow(): string
    {
        return strtolower($this->table().'_flow_run_id_key_unique');
    }

    /**
     * The name Laravel derived for the unique the initial migration created.
     */
    private function wide(): string
    {
        return strtolower($this->table().'_flow_run_id_key_value_unique');
    }

    private function table(): string
    {
        return (string) config('saga-lara-flow.database.table_prefix', '').'flow_tags';
    }

    /**
     * The migrator opens its transaction on the connection a migration names. Left
     * unnamed, that is the default connection while the uniques go to the package's
     * own, and a failure there rolls nothing back.
     */
    public function getConnection(): ?string
    {
        return config('saga-lara-flow.database.connection');
    }
};
