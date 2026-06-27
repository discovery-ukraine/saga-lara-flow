<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests;

use DiscoveryUkraine\SagaLaraFlow\SagaLaraFlowServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [
            SagaLaraFlowServiceProvider::class,
        ];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);
    }

    protected function defineDatabaseMigrations(): void
    {
        $migration = include __DIR__.'/../database/migrations/create_saga_lara_flow_tables.php.stub';
        $migration->up();
    }
}
