<?php

use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Support\Facades\Schema;

/**
 * Both index migrations match an index by the name the driver actually stored, not by
 * the name they asked for, because PostgreSQL truncates an identifier past 63 bytes and
 * a long table prefix reaches that. Every one of those `existing()` helpers was written
 * from the documentation and had never been run.
 *
 * This 30-character prefix reaches both halves: index_signal_waits' two names cross the
 * limit at 65 and 64 bytes, and so does the wide unique that unique_flow_tag_keys drops,
 * at 68. It is past the documented ceiling of 24 bytes, which PostgreSQL alone has the
 * room for — the point here is truncation, not a supported install.
 *
 * PostgreSQL only, deliberately. MySQL does not truncate a long identifier, it refuses
 * it outright at 64 characters, and SQLite has no limit worth reaching: there is no
 * truncation to exercise on either. Do not "fix" the skip.
 */
beforeEach(function () {
    // Guarded, not left to the per-test skip: a skip stops the test body, not this, and
    // MySQL rejects an identifier this long outright rather than truncating it.
    if (TestCase::driver() !== 'pgsql') {
        return;
    }

    config()->set('saga-lara-flow.database.table_prefix', 'saga_thirty_characters_prefix_');

    // Read afresh on every call in UsesSagaFlowConnection::getTable(), so the prefix
    // above is already in force for the migrations this builds the schema with.
    foreach ($this->packageMigrations() as $path) {
        (include $path)->up();
    }
});

// The schema this leaves behind is not the one the rest of the suite expects.
afterEach(fn () => TestCase::forgetSchema());

it('finds the indexes it created under the truncated names the driver stored', function () {
    $names = fn (string $table) => collect(Schema::getIndexes('saga_thirty_characters_prefix_'.$table))
        ->pluck('name')
        ->all();

    foreach ($names('action_runs') as $name) {
        expect(strlen($name))->toBeLessThanOrEqual(63);
    }

    // Stored under a name shorter than the one asked for, and still recognisably ours.
    expect($names('action_runs'))->toContain(substr('saga_thirty_characters_prefix_action_runs_status_signal_run_index', 0, 63))
        ->and($names('flow_signals'))->toContain(substr('saga_thirty_characters_prefix_flow_signals_status_name_run_index', 0, 63))
        ->and(strlen('saga_thirty_characters_prefix_action_runs_status_signal_run_index'))->toBeGreaterThan(63);
})->skip(fn () => TestCase::driver() !== 'pgsql', 'Only PostgreSQL truncates an identifier past 63 bytes.');

it('re-runs over its own work rather than creating a second index under the same name', function () {
    $indexes = fn (string $table) => collect(Schema::getIndexes('saga_thirty_characters_prefix_'.$table))
        ->pluck('columns');

    $waits = include __DIR__.'/../../database/migrations/2026_08_26_000000_index_signal_waits.php';
    $tags = include __DIR__.'/../../database/migrations/2026_08_26_000001_unique_flow_tag_keys.php';

    // Idempotent in both directions, which is what the name matching is for: a truncated
    // name that failed to match would make the second up() try to create an index the
    // catalogue already holds.
    $waits->up();
    $waits->up();
    $tags->up();
    $tags->up();

    // The wide unique is gone, which is the tag migration's own half of the matching:
    // its name truncates at this prefix, so a literal comparison would have left it.
    expect($indexes('flow_signals'))->toContain(['status', 'name', 'flow_run_id'])
        ->and($indexes('action_runs'))->toContain(['status', 'retry_signal', 'flow_run_id'])
        ->and($indexes('flow_tags')->all())->toContain(['flow_run_id', 'key'])
        ->and($indexes('flow_tags')->all())->not->toContain(['flow_run_id', 'key', 'value']);

    $waits->down();
    $waits->down();

    expect($indexes('flow_signals'))->not->toContain(['status', 'name', 'flow_run_id']);
})->skip(fn () => TestCase::driver() !== 'pgsql', 'Only PostgreSQL truncates an identifier past 63 bytes.');
