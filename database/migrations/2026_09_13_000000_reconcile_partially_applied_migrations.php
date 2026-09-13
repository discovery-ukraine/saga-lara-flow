<?php

use Illuminate\Database\Migrations\Migration;

/**
 * Applies every migration that came after the initial tables once more. A host that
 * got past a failed migrate by recording those migrations itself is left with
 * whatever the failed run did not reach, and Laravel never runs a recorded migration
 * again. Each of them now adds only what is still missing, so on a complete schema
 * this changes nothing.
 */
return new class extends Migration
{
    private const array MIGRATIONS = [
        '2026_08_21_000000_add_retry_on_signal_to_action_runs',
        '2026_08_25_000000_add_reclaim_stale_running_columns',
        '2026_08_26_000000_index_signal_waits',
        '2026_08_26_000001_unique_flow_tag_keys',
        '2026_08_31_000000_add_expiry_backoff_to_flow_runs',
    ];

    public function up(): void
    {
        foreach (self::MIGRATIONS as $name) {
            (require __DIR__."/{$name}.php")->up();
        }
    }

    /**
     * Owns nothing: what it filled in belongs to the migrations it ran, and their own
     * down() takes it away.
     */
    public function down(): void {}

    /**
     * The migrator opens its transaction on the connection a migration names. Left
     * unnamed, that is the default connection while the schema lives on the package's
     * own, and a failure there rolls nothing back.
     */
    public function getConnection(): ?string
    {
        return config('saga-lara-flow.database.connection');
    }
};
