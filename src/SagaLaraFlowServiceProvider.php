<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowChildRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SideEffectRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Repositories\EloquentActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Repositories\EloquentFlowChildRepository;
use DiscoveryUkraine\SagaLaraFlow\Repositories\EloquentFlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Repositories\EloquentSideEffectRepository;
use DiscoveryUkraine\SagaLaraFlow\Repositories\EloquentSignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Serialization\LaravelSerializer;
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
            ->hasMigration('create_saga_lara_flow_initial_tables');
    }

    public function packageRegistered(): void
    {
        $this->app->singleton(FlowManager::class);
        $this->app->alias(FlowManager::class, 'saga-flow');

        $this->app->scoped(FlowRuntime::class);
        $this->app->singleton(FlowExecutor::class);

        $this->app->bind(StateMachine::class, FlowStateMachine::class);
        $this->app->bind(FlowRepository::class, EloquentFlowRepository::class);
        $this->app->bind(ActionRunRepository::class, EloquentActionRunRepository::class);
        $this->app->bind(FlowChildRepository::class, EloquentFlowChildRepository::class);
        $this->app->bind(SideEffectRepository::class, EloquentSideEffectRepository::class);
        $this->app->bind(SignalRepository::class, EloquentSignalRepository::class);
        $this->app->bind(Serializer::class, LaravelSerializer::class);
    }

    public function packageBooted(): void
    {
        if (is_string($key = config('app.key')) && $key !== '') {
            SerializableClosure::setSecretKey($key);
        }
    }
}
