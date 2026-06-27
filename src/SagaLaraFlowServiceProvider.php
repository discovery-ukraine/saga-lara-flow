<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Repositories\EloquentFlowRepository;
use DiscoveryUkraine\SagaLaraFlow\States\FlowStateMachine;
use Laravel\SerializableClosure\SerializableClosure;
use Spatie\LaravelPackageTools\Package;
use Spatie\LaravelPackageTools\PackageServiceProvider;

class SagaLaraFlowServiceProvider extends PackageServiceProvider
{
    public function configurePackage(Package $package): void
    {
        $package
            ->name('saga-lara-flow')
            ->hasConfigFile()
            ->hasMigration('create_saga_lara_flow_tables');
    }

    public function packageRegistered(): void
    {
        $this->app->singleton(FlowManager::class);
        $this->app->alias(FlowManager::class, 'saga-flow');

        $this->app->bind(StateMachine::class, FlowStateMachine::class);
        $this->app->bind(FlowRepository::class, EloquentFlowRepository::class);
    }

    public function packageBooted(): void
    {
        if (is_string($key = config('app.key')) && $key !== '') {
            SerializableClosure::setSecretKey($key);
        }
    }
}
