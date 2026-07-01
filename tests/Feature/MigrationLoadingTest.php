<?php

use Illuminate\Support\Facades\Schema;

// The provider calls runsMigrations(), so a host app installs with just
// `php artisan migrate` — no vendor:publish step. This guards that end to end:
// the suite TestCase pre-creates the schema by calling the migration's up()
// directly, so we drop it first, then prove `artisan migrate` — which only knows
// about the package migration because runsMigrations() registered it — recreates
// the tables. Regression guard for the .php.stub bug where the migrator silently
// globbed a non-directory and found nothing (v1.0.1).
it('creates the engine tables via artisan migrate, with no publish step', function (): void {
    $migration = include __DIR__.'/../../database/migrations/create_saga_lara_flow_initial_tables.php';
    $migration->down();

    expect(Schema::hasTable('saga_flow_runs'))->toBeFalse();

    $this->artisan('migrate')->assertSuccessful();

    expect(Schema::hasTable('saga_flow_runs'))->toBeTrue()
        ->and(Schema::hasTable('saga_action_runs'))->toBeTrue()
        ->and(Schema::hasTable('saga_flow_events'))->toBeTrue();
});
