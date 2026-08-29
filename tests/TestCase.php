<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests;

use DiscoveryUkraine\SagaLaraFlow\SagaLaraFlowServiceProvider;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Orchestra\Testbench\TestCase as Orchestra;
use RuntimeException;

class TestCase extends Orchestra
{
    /**
     * Whether this process has already built the schema on a database that outlives a
     * test. SQLite never sets it: every test there opens its own :memory: database.
     *
     * The suite is run one process at a time on such a database; a parallel run is
     * refused rather than left to corrupt itself.
     */
    private static bool $schemaBuilt = false;

    protected function getPackageProviders($app): array
    {
        return [
            SagaLaraFlowServiceProvider::class,
        ];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', self::connectionConfig());
    }

    /**
     * SQLite is the default and needs nothing running; SAGA_TEST_DB=mysql or =pgsql
     * points the suite at a server instead — one whose database is the suite's own, since
     * the first test drops every table in it. Each server answers something the others do
     * not: MySQL counts the rows an update changed rather than the rows it matched, and
     * PostgreSQL refuses every statement after a failed one until the transaction is
     * rolled back. Neither is visible to a suite that has only ever run on SQLite.
     *
     * The name is the package's own rather than DB_CONNECTION, which Testbench and a
     * host .env both have opinions about.
     *
     * @return array<string, mixed>
     */
    private static function connectionConfig(): array
    {
        return match (self::driver()) {
            'mysql' => [
                'driver' => 'mysql',
                'host' => env('SAGA_TEST_DB_HOST', '127.0.0.1'),
                'port' => env('SAGA_TEST_DB_PORT', '3306'),
                'database' => env('SAGA_TEST_DB_DATABASE', 'saga_test'),
                'username' => env('SAGA_TEST_DB_USERNAME', 'saga'),
                'password' => env('SAGA_TEST_DB_PASSWORD', 'saga'),
                'charset' => 'utf8mb4',
                'collation' => 'utf8mb4_unicode_ci',
                'prefix' => '',
            ],
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => env('SAGA_TEST_DB_HOST', '127.0.0.1'),
                'port' => env('SAGA_TEST_DB_PORT', '5432'),
                'database' => env('SAGA_TEST_DB_DATABASE', 'saga_test'),
                'username' => env('SAGA_TEST_DB_USERNAME', 'saga'),
                'password' => env('SAGA_TEST_DB_PASSWORD', 'saga'),
                'charset' => 'utf8',
                // Named rather than left to the server's own: getTableListing() reads
                // every schema on the path, and the reset below empties what it lists.
                'search_path' => 'public',
                'sslmode' => 'prefer',
                'prefix' => '',
            ],
            default => [
                'driver' => 'sqlite',
                'database' => ':memory:',
                'prefix' => '',
            ],
        };
    }

    /**
     * The driver the harness was asked for, which is not the same question as the driver
     * a connection reports: a test asserts the two agree, so a MySQL run that quietly
     * fell back to SQLite fails instead of passing.
     */
    public static function driver(): string
    {
        return (string) env('SAGA_TEST_DB', 'sqlite');
    }

    /**
     * Hand the next test a freshly built schema. For a test that changes the schema
     * itself: on SQLite its damage dies with the in-memory database, on a server it
     * would be inherited by whatever random order runs next.
     */
    public static function forgetSchema(): void
    {
        self::$schemaBuilt = false;
    }

    /**
     * Every test boots its own Testbench app, and each one opens its own connection. On
     * :memory: that costs nothing and the database goes with it; against a server the
     * connections pile up until MySQL refuses the next one with "Too many connections"
     * somewhere past the two hundredth test. Hand the connection back before the app
     * that owns it is thrown away.
     */
    protected function tearDown(): void
    {
        if (self::driver() !== 'sqlite' && $this->app !== null) {
            $this->app['db']->purge('testing');
        }

        parent::tearDown();
    }

    protected function defineDatabaseMigrations(): void
    {
        if (self::driver() === 'sqlite') {
            $this->migrate();

            return;
        }

        // ParaTest gives every worker the same database while the flag below is per
        // process: one worker would rebuild the schema under another's feet, and the
        // symptom would be a random test failing for no reason it can name.
        //
        // PARATEST is the variable to ask for, not TEST_TOKEN: --no-test-tokens turns the
        // token off and leaves PARATEST set (Options::assembleEnvironmentVariables()), and
        // it is what Pest's own worker detection reads.
        if (env('PARATEST') !== null) {
            throw new RuntimeException(
                'The suite runs one process at a time on '.self::driver().'; re-run without --parallel.',
            );
        }

        // A server keeps the database between tests, so running the migrations again
        // would fail on the tables the last test left behind. Build the schema once and
        // empty it afterwards — what a test needs from a fresh database is no rows, and
        // rebuilding the schema for each of them costs a DDL round trip per table per
        // test.
        if (! self::$schemaBuilt) {
            // Every table, including any the database already held: the target is the
            // suite's own, as connectionConfig() and CONTRIBUTING both say.
            Schema::connection('testing')->dropAllTables();

            $this->migrate();

            self::$schemaBuilt = true;

            return;
        }

        $this->truncate();
    }

    private function migrate(): void
    {
        // Every shipped migration, in filename order — the package now ships more
        // than one, and a suite that only ran the first would be missing columns.
        foreach ($this->packageMigrations() as $path) {
            (include $path)->up();
        }
    }

    /**
     * Every table, not only the package's: useDatabaseQueue() creates `jobs` and
     * `job_batches` on first use and guards the create with hasTable(), so on a server
     * they outlive the test that made them — along with any job it left unworked, which
     * the next drainQueue() would happily run.
     *
     * Scoped to the schemas dropAllTables() clears, so the two halves of the reset agree
     * on what "the database" means. Left unscoped, PostgreSQL lists every schema on the
     * server and hands back bare names: a table outside the search path is then either
     * missing when the delete resolves the name, or shadowed by a same-named one in
     * public, which gets emptied twice while the real rows stay put.
     *
     * The schema declares no foreign keys, so nothing constrains the order.
     */
    protected function truncate(): void
    {
        $connection = DB::connection('testing');
        $schema = Schema::connection('testing');

        foreach ($schema->getTableListing($schema->getCurrentSchemaListing(), schemaQualified: false) as $table) {
            $connection->table($table)->delete();
        }
    }

    /**
     * @return list<string>
     */
    protected function packageMigrations(): array
    {
        $paths = glob(__DIR__.'/../database/migrations/*.php') ?: [];

        sort($paths);

        return array_values($paths);
    }
}
