<?php

use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Support\Facades\Schema;

/**
 * Both index migrations match an index by the name the driver actually stored, not by
 * the name Laravel derived, because PostgreSQL truncates an identifier past 63 bytes
 * and a long table prefix reaches that. Every one of those `existing()` helpers was
 * written from the documentation and had never been run.
 *
 * With this 24-character prefix both names that index_signal_waits owns cross the limit
 * — 73 and 66 bytes — so what the catalogue holds is not what `$table->index(..., $name)`
 * asked for, and a migration that compared the two literally would try to create its
 * index a second time.
 *
 * The prefix stops at 24 on purpose. unique_flow_tag_keys needs 32 before its own names
 * truncate, and at 30 the initial migration cannot install at all: three of
 * flow_children's index names truncate onto each other. That is issue #42, and until it
 * is fixed the tag migration's half of this cannot be reached.
 *
 * PostgreSQL only, deliberately. MySQL does not truncate a long identifier, it refuses
 * it outright at 64 bytes, and SQLite has no limit worth reaching: there is no
 * truncation to exercise on either. Do not "fix" the skip.
 */
beforeEach(function () {
    // Guarded, not left to the per-test skip: a skip stops the test body, not this, and
    // MySQL rejects an identifier this long outright rather than truncating it.
    if (TestCase::driver() !== 'pgsql') {
        return;
    }

    config()->set('saga-lara-flow.database.table_prefix', 'saga_flow_long_prefix_1_');

    // Read afresh on every call in UsesSagaFlowConnection::getTable(), so the prefix
    // above is already in force for the migrations this builds the schema with.
    foreach ($this->packageMigrations() as $path) {
        (include $path)->up();
    }
});

// The schema this leaves behind is not the one the rest of the suite expects.
afterEach(fn () => TestCase::forgetSchema());

it('finds the indexes it created under the truncated names the driver stored', function () {
    $names = fn (string $table) => collect(Schema::getIndexes('saga_flow_long_prefix_1_'.$table))
        ->pluck('name')
        ->all();

    foreach ($names('action_runs') as $name) {
        expect(strlen($name))->toBeLessThanOrEqual(63);
    }

    // Stored under a name shorter than the one asked for, and still recognisably ours.
    expect($names('action_runs'))->toContain(substr('saga_flow_long_prefix_1_action_runs_status_retry_signal_flow_run_id_index', 0, 63))
        ->and($names('flow_signals'))->toContain(substr('saga_flow_long_prefix_1_flow_signals_status_name_flow_run_id_index', 0, 63))
        ->and(strlen('saga_flow_long_prefix_1_action_runs_status_retry_signal_flow_run_id_index'))->toBeGreaterThan(63);
})->skip(fn () => TestCase::driver() !== 'pgsql', 'Only PostgreSQL truncates an identifier past 63 bytes.');

it('re-runs over its own work rather than creating a second index under the same name', function () {
    $indexes = fn (string $table) => collect(Schema::getIndexes('saga_flow_long_prefix_1_'.$table))
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

    expect($indexes('flow_signals'))->toContain(['status', 'name', 'flow_run_id'])
        ->and($indexes('action_runs'))->toContain(['status', 'retry_signal', 'flow_run_id'])
        ->and($indexes('flow_tags')->all())->toContain(['flow_run_id', 'key']);

    $waits->down();
    $waits->down();

    expect($indexes('flow_signals'))->not->toContain(['status', 'name', 'flow_run_id']);
})->skip(fn () => TestCase::driver() !== 'pgsql', 'Only PostgreSQL truncates an identifier past 63 bytes.');
