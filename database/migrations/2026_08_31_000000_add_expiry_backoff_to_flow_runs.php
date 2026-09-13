<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Each column asks first. MySQL commits DDL on its own, so a run that died part
     * of the way through has to be repeatable rather than trip over what it added.
     */
    public function up(): void
    {
        $schema = Schema::connection($this->getConnection());
        $table = $this->prefix().'flow_runs';

        $schema->table($table, function (Blueprint $blueprint) use ($schema, $table): void {
            if (! $schema->hasColumn($table, 'expiry_attempts')) {
                $blueprint->unsignedInteger('expiry_attempts')->default(0)->after('repair_available_at');
            }

            if (! $schema->hasColumn($table, 'expiry_available_at')) {
                $blueprint->timestamp('expiry_available_at')->nullable()->after('expiry_attempts');
            }
        });
    }

    /**
     * Removes only what is there, so it can also undo a run that died part of the way.
     */
    public function down(): void
    {
        $schema = Schema::connection($this->getConnection());
        $table = $this->prefix().'flow_runs';

        $present = array_values(array_filter(
            ['expiry_attempts', 'expiry_available_at'],
            fn (string $column): bool => $schema->hasColumn($table, $column),
        ));

        if ($present !== []) {
            $schema->table($table, fn (Blueprint $blueprint) => $blueprint->dropColumn($present));
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

    private function prefix(): string
    {
        return (string) config('saga-lara-flow.database.table_prefix', '');
    }
};
