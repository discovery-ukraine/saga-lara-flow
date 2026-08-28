<?php

use DiscoveryUkraine\SagaLaraFlow\Tests\TestCase;
use Illuminate\Support\Facades\DB;

/**
 * The harness gives each test an empty database: on SQLite by opening a new :memory:
 * one, and on a server by building the schema once and deleting the rows before each
 * test. "The database" has to mean the same thing to both halves of that — the initial
 * dropAllTables() and the reset that follows it — and on PostgreSQL it very nearly did
 * not. Asked for a table listing without a schema, PostgreSQL answers with every schema
 * on the server, in bare names, while dropAllTables() only ever clears the search path.
 *
 * PostgreSQL only: it is the sole supported driver with more than one schema per
 * database, so there is nothing for the others to get wrong here.
 */
afterEach(function () {
    if (TestCase::driver() === 'pgsql') {
        DB::statement('drop schema if exists sibling cascade');
    }
});

it('empties the schema it drops and leaves another schema alone', function () {
    DB::statement('create schema sibling');
    DB::statement('create table sibling.sentinel (id integer)');
    // The nastier half: a name the search path already answers for. A bare-name delete
    // would empty public's copy twice and never touch this one.
    DB::statement('create table sibling.saga_flow_runs (id integer)');

    DB::table('sibling.sentinel')->insert(['id' => 1]);
    DB::table('sibling.saga_flow_runs')->insert(['id' => 1]);

    // The reset the harness runs between tests, called directly rather than waited for:
    // whether it throws is as much the point as what it leaves behind.
    $this->truncate();

    expect(DB::table('sibling.sentinel')->count())->toBe(1)
        ->and(DB::table('sibling.saga_flow_runs')->count())->toBe(1);
})->skip(
    fn () => TestCase::driver() !== 'pgsql',
    'Only PostgreSQL puts more than one schema in a database.',
);
