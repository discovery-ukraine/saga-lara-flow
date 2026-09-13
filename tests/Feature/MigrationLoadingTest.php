<?php

use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

// The only file that changes the schema rather than reading it: it rolls single
// migrations up and down, drops the tables, and adds and removes uniques. On SQLite
// whatever it leaves behind dies with the in-memory database. On a server the schema
// outlives the test, so hand the next one a rebuild.
afterEach(fn () => TestCase::forgetSchema());

// flow_run_id is a ulid, which is char(26). PostgreSQL pads a shorter value out to the
// full width and hands the padding back on the way out; MySQL trims it on read and
// SQLite ignores the width. Nothing in the engine can put a short value there — every
// id comes from HasUlids — so these stand in for one at its real length rather than
// leaning on a value the column cannot hold faithfully.
const RUN_ONE = 'run-1-00000000000000000000';
const RUN_TWO = 'run-2-00000000000000000000';

// The provider calls runsMigrations(), so a host app installs with just
// `php artisan migrate` — no vendor:publish step. Two things must hold, and each
// has bitten us before:
//   1. Every migration must actually load. They ship as real `.php` files (not
//      `.php.stub`), because Laravel's migrator only treats a registered path as a
//      migration file when it ends in `.php` — a `.php.stub` path is silently
//      globbed as a directory and skipped (the v1.0.1 bug).
//   2. Each name must carry a timestamp prefix, like every first-party Laravel
//      package migration, so it reads as `2026_07_02_000000_create_...` in the
//      migrations table and `migrate:status` — not a bare, dateless `create_...`
//      (the v1.0.2 wart).
// And since 1.1.0 there is a third: the package ships MORE THAN ONE migration, so
// each additional file needs its own ->hasMigration() call or it never loads.
it('resolves every shipped migration with a timestamped name so migrate:status is well-formed', function (): void {
    $registered = collect(app('migrator')->getMigrationFiles(app('migrator')->paths()))->keys();

    $shipped = collect(glob(__DIR__.'/../../database/migrations/*.php') ?: [])
        ->map(fn (string $path): string => basename($path, '.php'));

    expect($shipped)->not->toBeEmpty();

    foreach ($shipped as $name) {
        expect($name)->toMatch('/^\d{4}_\d{2}_\d{2}_\d{6}_[a-z0-9_]+$/')
            ->and($registered)->toContain($name);
    }
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

it('adds the retry-on-signal columns to action_runs', function (): void {
    expect(Schema::hasColumn('saga_action_runs', 'retry_signal'))->toBeTrue()
        ->and(Schema::hasColumn('saga_action_runs', 'retry_signal_attempts'))->toBeTrue()
        ->and(Schema::hasColumn('saga_action_runs', 'retry_signal_max_attempts'))->toBeTrue()
        ->and(Schema::hasColumn('saga_action_runs', 'queue_attempts_exhausted'))->toBeTrue();
});

it('rolls the retry-on-signal columns back down again', function (): void {
    // The wait index references retry_signal; drop it first (same order
    // migrate:rollback would use) or SQLite refuses the column drop.
    $index = include __DIR__.'/../../database/migrations/2026_08_26_000000_index_signal_waits.php';
    $index->down();

    $migration = include __DIR__.'/../../database/migrations/2026_08_21_000000_add_retry_on_signal_to_action_runs.php';
    $migration->down();

    expect(Schema::hasColumn('saga_action_runs', 'retry_signal'))->toBeFalse()
        ->and(Schema::hasColumn('saga_action_runs', 'retry_signal_attempts'))->toBeFalse()
        ->and(Schema::hasColumn('saga_action_runs', 'retry_signal_max_attempts'))->toBeFalse()
        ->and(Schema::hasColumn('saga_action_runs', 'queue_attempts_exhausted'))->toBeFalse()
        ->and(Schema::hasColumn('saga_action_runs', 'attempts'))->toBeTrue();

    $migration->up();
    $index->up();

    expect(Schema::hasColumn('saga_action_runs', 'retry_signal'))->toBeTrue();
});

// The migrator wraps a migration in a transaction on the connection the migration
// names. One that names none gets the default connection's transaction while its DDL
// goes to the package's own, so a host with a dedicated connection was left holding
// the columns of a migration that failed and was never recorded.
it('names the package connection to the migrator, so its transaction wraps the change', function (): void {
    config()->set('saga-lara-flow.database.connection', 'testing');

    foreach (glob(__DIR__.'/../../database/migrations/*.php') ?: [] as $path) {
        expect((include $path)->getConnection())->toBe('testing', basename($path));
    }
});

it('re-runs each column migration cleanly over a schema that already has its work', function (): void {
    // What a host is left with when a migrate applied the DDL but never recorded the
    // migration: MySQL commits every ALTER on its own.
    foreach ([
        '2026_08_21_000000_add_retry_on_signal_to_action_runs',
        '2026_08_25_000000_add_reclaim_stale_running_columns',
        '2026_08_31_000000_add_expiry_backoff_to_flow_runs',
    ] as $name) {
        (include __DIR__."/../../database/migrations/{$name}.php")->up();
    }

    expect(Schema::hasColumn('saga_action_runs', 'retry_signal'))->toBeTrue()
        ->and(Schema::hasIndex('saga_action_runs', ['reclaim_stale_at']))->toBeTrue()
        ->and(Schema::hasColumn('saga_compensation_runs', 'attempts'))->toBeTrue()
        ->and(Schema::hasColumn('saga_flow_runs', 'expiry_available_at'))->toBeTrue();
});

it('adds only what a partly applied column migration is still missing', function (): void {
    Schema::table('saga_action_runs', fn (Blueprint $table) => $table->dropColumn('queue_attempts_exhausted'));
    Schema::table('saga_compensation_runs', fn (Blueprint $table) => $table->dropIndex(['reclaim_stale_at']));
    Schema::table('saga_flow_runs', fn (Blueprint $table) => $table->dropColumn('expiry_available_at'));

    (include __DIR__.'/../../database/migrations/2026_08_21_000000_add_retry_on_signal_to_action_runs.php')->up();
    (include __DIR__.'/../../database/migrations/2026_08_25_000000_add_reclaim_stale_running_columns.php')->up();
    (include __DIR__.'/../../database/migrations/2026_08_31_000000_add_expiry_backoff_to_flow_runs.php')->up();

    expect(Schema::hasColumn('saga_action_runs', 'queue_attempts_exhausted'))->toBeTrue()
        ->and(Schema::hasIndex('saga_compensation_runs', ['reclaim_stale_at']))->toBeTrue()
        ->and(Schema::hasColumn('saga_flow_runs', 'expiry_available_at'))->toBeTrue();
});

it('fills in what a migration recorded by hand never applied', function (): void {
    $initial = include __DIR__.'/../../database/migrations/2026_07_02_000000_create_saga_lara_flow_initial_tables.php';
    $initial->down();

    $this->artisan('migrate')->assertSuccessful();

    // A host that got past a failed migrate by inserting the migration rows itself:
    // Laravel will not run those migrations again, whatever they left undone.
    Schema::table('saga_action_runs', fn (Blueprint $table) => $table->dropColumn('queue_attempts_exhausted'));
    Schema::table('saga_compensation_runs', fn (Blueprint $table) => $table->dropIndex(['reclaim_stale_at']));
    Schema::table('saga_flow_runs', fn (Blueprint $table) => $table->dropColumn('expiry_available_at'));
    DB::table('migrations')->where('migration', '2026_09_13_000000_reconcile_partially_applied_migrations')->delete();

    $this->artisan('migrate')->assertSuccessful();

    expect(Schema::hasColumn('saga_action_runs', 'queue_attempts_exhausted'))->toBeTrue()
        ->and(Schema::hasIndex('saga_compensation_runs', 'saga_compensation_runs_reclaim_stale_at_index'))->toBeTrue()
        ->and(Schema::hasColumn('saga_flow_runs', 'expiry_available_at'))->toBeTrue();
});

it('creates its own reclaim index beside a host index on the same column, and drops only its own', function (): void {
    $migration = include __DIR__.'/../../database/migrations/2026_08_25_000000_add_reclaim_stale_running_columns.php';
    $owned = 'saga_compensation_runs_reclaim_stale_at_index';

    Schema::table('saga_compensation_runs', function (Blueprint $table) use ($owned): void {
        $table->dropIndex($owned);
        $table->index('reclaim_stale_at', 'host_reclaim_lookup');
    });

    $migration->up();

    expect(Schema::hasIndex('saga_compensation_runs', $owned))->toBeTrue()
        ->and(Schema::hasIndex('saga_compensation_runs', 'host_reclaim_lookup'))->toBeTrue();

    // SQLite will not drop a column a host index still covers; that one is the host's
    // to remove.
    Schema::table('saga_compensation_runs', fn (Blueprint $table) => $table->dropIndex('host_reclaim_lookup'));

    $migration->down();
    $migration->down();

    expect(Schema::hasIndex('saga_compensation_runs', $owned))->toBeFalse()
        ->and(Schema::hasColumn('saga_compensation_runs', 'reclaim_stale_at'))->toBeFalse();

    $migration->up();

    expect(Schema::hasIndex('saga_compensation_runs', $owned))->toBeTrue();
});

it('rolls back a column migration that was only partly applied', function (): void {
    Schema::table('saga_flow_runs', fn (Blueprint $table) => $table->dropColumn('expiry_available_at'));

    $migration = include __DIR__.'/../../database/migrations/2026_08_31_000000_add_expiry_backoff_to_flow_runs.php';
    $migration->down();

    expect(Schema::hasColumn('saga_flow_runs', 'expiry_attempts'))->toBeFalse();

    $migration->up();

    expect(Schema::hasColumn('saga_flow_runs', 'expiry_available_at'))->toBeTrue();
});

it('indexes the wait lookups and narrows the tag key unique', function (): void {
    $columns = fn (string $table) => collect(Schema::getIndexes($table))->pluck('columns');
    $uniques = fn (string $table) => collect(Schema::getIndexes($table))
        ->where('unique', true)
        ->pluck('columns');

    // Both wait filters read across runs, so the signal name and the retry signal
    // have to lead their index — the shipped ones lead with flow_run_id.
    expect($columns('saga_flow_signals'))->toContain(['status', 'name', 'flow_run_id'])
        ->and($columns('saga_action_runs'))->toContain(['status', 'retry_signal', 'flow_run_id'])
        ->and($uniques('saga_flow_tags'))->toContain(['flow_run_id', 'key'])
        ->and($uniques('saga_flow_tags'))->not->toContain(['flow_run_id', 'key', 'value']);
});

it('rolls the wait indexes back down again, and re-runs cleanly over its own work', function (): void {
    $migration = include __DIR__.'/../../database/migrations/2026_08_26_000000_index_signal_waits.php';
    $columns = fn (string $table) => collect(Schema::getIndexes($table))->pluck('columns');

    $migration->down();

    expect($columns('saga_flow_signals'))->not->toContain(['status', 'name', 'flow_run_id'])
        ->and($columns('saga_action_runs'))->not->toContain(['status', 'retry_signal', 'flow_run_id']);

    // MySQL applies neither statement inside a transaction, so a run that died
    // between them has to be repeatable.
    $migration->down();
    $migration->up();
    $migration->up();

    expect($columns('saga_flow_signals'))->toContain(['status', 'name', 'flow_run_id'])
        ->and($columns('saga_action_runs'))->toContain(['status', 'retry_signal', 'flow_run_id']);
});

it('rolls the tag key unique back down again, and re-runs cleanly over its own work', function (): void {
    $migration = include __DIR__.'/../../database/migrations/2026_08_26_000001_unique_flow_tag_keys.php';
    $uniques = fn () => collect(Schema::getIndexes('saga_flow_tags'))->where('unique', true)->pluck('columns');

    $migration->down();

    expect($uniques())->toContain(['flow_run_id', 'key', 'value']);

    $migration->down();
    $migration->up();
    $migration->up();

    expect($uniques())->toContain(['flow_run_id', 'key'])
        ->and($uniques())->not->toContain(['flow_run_id', 'key', 'value']);
});

it('collapses tag keys that the wider unique had let diverge', function (): void {
    $migration = include __DIR__.'/../../database/migrations/2026_08_26_000001_unique_flow_tag_keys.php';
    $migration->down();

    DB::table('saga_flow_tags')->insert([
        ['flow_run_id' => RUN_ONE, 'key' => 'stage', 'value' => 'charged'],
        ['flow_run_id' => RUN_ONE, 'key' => 'stage', 'value' => 'shipped'],
        ['flow_run_id' => RUN_ONE, 'key' => 'tenant', 'value' => 'acme'],
        ['flow_run_id' => RUN_TWO, 'key' => 'stage', 'value' => 'charged'],
    ]);

    $migration->up();

    // Only the diverged key loses a row, and the newest write is the one kept.
    expect(DB::table('saga_flow_tags')->orderBy('id')->get()->map(
        fn ($tag): string => "{$tag->flow_run_id}:{$tag->key}={$tag->value}",
    )->all())->toBe([
        RUN_ONE.':stage=shipped',
        RUN_ONE.':tenant=acme',
        RUN_TWO.':stage=charged',
    ]);
});

it('carries on from whichever half of the tag key swap already happened', function (): void {
    $migration = include __DIR__.'/../../database/migrations/2026_08_26_000001_unique_flow_tag_keys.php';
    $uniques = fn () => collect(Schema::getIndexes('saga_flow_tags'))
        ->where('unique', true)
        ->where('primary', false)
        ->pluck('name')
        ->all();

    // Died after the narrow unique went on, before the wide one came off.
    Schema::table('saga_flow_tags', function (Blueprint $table): void {
        $table->unique(['flow_run_id', 'key', 'value'], 'saga_flow_tags_flow_run_id_key_value_unique');
    });

    $migration->up();

    expect($uniques())->toBe(['saga_flow_tags_flow_run_id_key_unique']);

    // Died with neither on — the state the reverse order used to strand a table in.
    Schema::table('saga_flow_tags', function (Blueprint $table): void {
        $table->dropUnique('saga_flow_tags_flow_run_id_key_unique');
    });

    $migration->up();

    expect($uniques())->toBe(['saga_flow_tags_flow_run_id_key_unique']);
});

it('keeps the tag value written last, not the row inserted last', function (): void {
    $migration = include __DIR__.'/../../database/migrations/2026_08_26_000001_unique_flow_tag_keys.php';
    $migration->down();

    DB::table('saga_flow_tags')->insert([
        ['id' => 1, 'flow_run_id' => RUN_ONE, 'key' => 'stage', 'value' => 'charged', 'updated_at' => now()],
        ['id' => 2, 'flow_run_id' => RUN_ONE, 'key' => 'stage', 'value' => 'shipped', 'updated_at' => now()],
    ]);

    // updateOrCreate rewrites the row it matched instead of inserting a new one, so
    // the newest value can sit on the lower id.
    DB::table('saga_flow_tags')->where('id', 1)->update(['value' => 'delivered', 'updated_at' => now()->addMinute()]);

    $migration->up();

    expect(DB::table('saga_flow_tags')->pluck('value', 'id')->all())->toBe([1 => 'delivered']);
});
