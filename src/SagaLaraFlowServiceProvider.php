<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Commands\SagaLaraFlowCommand;
use Spatie\LaravelPackageTools\Package;
use Spatie\LaravelPackageTools\PackageServiceProvider;

class SagaLaraFlowServiceProvider extends PackageServiceProvider
{
    public function configurePackage(Package $package): void
    {
        /*
         * This class is a Package Service Provider
         *
         * More info: https://github.com/spatie/laravel-package-tools
         */
        $package
            ->name('saga-lara-flow')
            ->hasConfigFile()
            ->hasMigration('create_saga_lara_flow_table')
            ->hasCommand(SagaLaraFlowCommand::class);
    }
}
