<?php

use Illuminate\Support\Facades\Schema;

// The provider calls runsMigrations(), so a host app installs with just
// `php artisan migrate` — no vendor:publish step. Two things must hold, and each
// has bitten us before:
//   1. The migration must actually load. It ships as a real `.php` (not `.php.stub`),
//      because Laravel's migrator only treats a registered path as a migration file
//      when it ends in `.php` — a `.php.stub` path is silently globbed as a directory
//      and skipped (the v1.0.1 bug).
//   2. Its name must carry a timestamp prefix, like every first-party Laravel package
//      migration, so it reads as `2026_07_02_000000_create_...` in the migrations table
//      and `migrate:status` — not a bare, dateless `create_...` (the v1.0.2 wart).
it('resolves the migration with a timestamped name so migrate:status is well-formed', function (): void {
    $names = collect(app('migrator')->getMigrationFiles(app('migrator')->paths()))->keys();

    $engineMigration = $names->first(fn (string $name): bool => str_contains($name,
        'create_saga_lara_flow_initial_tables'));

    expect($engineMigration)->not->toBeNull()
        ->and($engineMigration)->toMatch('/^\d{4}_\d{2}_\d{2}_\d{6}_create_saga_lara_flow_initial_tables$/');
});

it('creates the engine tables via artisan migrate, with no publish step', function (): void {
    $migration = include __DIR__.'/../../database/migrations/2026_07_02_000000_create_saga_lara_flow_initial_tables.php';
    $migration->down();

    expect(Schema::hasTable('saga_flow_runs'))->toBeFalse();

    $this->artisan('migrate')->assertSuccessful();

    expect(Schema::hasTable('saga_flow_runs'))->toBeTrue()
        ->and(Schema::hasTable('saga_action_runs'))->toBeTrue()
        ->and(Schema::hasTable('saga_flow_events'))->toBeTrue();
});
