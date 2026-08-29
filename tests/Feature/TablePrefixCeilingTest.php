<?php

use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Support\Facades\Schema;

/**
 * The ceiling the configuration documentation states, enforced rather than trusted. It is
 * measured in bytes, because the two servers cap different things: MySQL refuses an
 * identifier past 64 characters, PostgreSQL truncates one past 63 bytes. The longest name
 * the schema asks for is 40, leaving 24.
 *
 * A prefix over the ceiling fails part-way through the initial migration, so these cover
 * the whole install rather than one table.
 */
const SAGA_PREFIX_ASCII = 'saga_documented_ceiling_';

// Thirteen characters, twenty-four bytes: the same ceiling, spent differently.
const SAGA_PREFIX_MULTIBYTE = 'сага_вимірка_';

// The schema these leave behind is not the one the rest of the suite expects.
afterEach(fn () => TestCase::forgetSchema());

function installAtPrefix(string $prefix): void
{
    Schema::connection('testing')->dropAllTables();

    config()->set('saga-lara-flow.database.table_prefix', $prefix);

    foreach ((fn () => $this->packageMigrations())->call(test()) as $path) {
        (include $path)->up();
    }
}

function tablesExistUnder(string $prefix): void
{
    foreach ([
        'flow_runs', 'action_runs', 'flow_events', 'flow_signals',
        'compensation_runs', 'side_effects', 'flow_tags', 'flow_children',
    ] as $table) {
        expect(Schema::connection('testing')->hasTable($prefix.$table))->toBeTrue();
    }
}

it('installs every table at the documented prefix ceiling', function () {
    expect(strlen(SAGA_PREFIX_ASCII))->toBe(24);

    installAtPrefix(SAGA_PREFIX_ASCII);

    tablesExistUnder(SAGA_PREFIX_ASCII);
});

it('spends the ceiling in bytes, not in characters', function () {
    expect(strlen(SAGA_PREFIX_MULTIBYTE))->toBe(24)
        ->and(mb_strlen(SAGA_PREFIX_MULTIBYTE))->toBe(13);

    installAtPrefix(SAGA_PREFIX_MULTIBYTE);

    tablesExistUnder(SAGA_PREFIX_MULTIBYTE);
});

it('names the indexes that would otherwise derive a name too long to install', function () {
    installAtPrefix(SAGA_PREFIX_ASCII);

    $names = fn (string $table) => collect(Schema::connection('testing')->getIndexes(SAGA_PREFIX_ASCII.$table))
        ->pluck('name')
        ->all();

    // Given rather than derived, and short enough that the prefix above still fits.
    expect($names('flow_runs'))->toContain(SAGA_PREFIX_ASCII.'flow_runs_status_repair_index')
        ->and($names('flow_signals'))->toContain(SAGA_PREFIX_ASCII.'flow_signals_run_name_status_index')
        ->and($names('flow_signals'))->toContain(SAGA_PREFIX_ASCII.'flow_signals_status_name_run_index')
        ->and($names('action_runs'))->toContain(SAGA_PREFIX_ASCII.'action_runs_status_signal_run_index')
        ->and($names('flow_children'))->toContain(SAGA_PREFIX_ASCII.'flow_children_parent_sequence_unique')
        ->and($names('flow_children'))->toContain(SAGA_PREFIX_ASCII.'flow_children_parent_child_unique');
});
