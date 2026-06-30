<?php

namespace DiscoveryUkraine\SagaLaraFlow;

use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowCancelCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowKickCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowListCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowMonitorCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowPruneCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowRepairCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowShowCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\FlowSignalCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\MakeActionCommand;
use DiscoveryUkraine\SagaLaraFlow\Console\Commands\MakeWorkflowCommand;
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
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Serialization\LaravelSerializer;
use DiscoveryUkraine\SagaLaraFlow\States\FlowStateMachine;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Facades\Event;
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
            ->hasMigration('create_saga_lara_flow_initial_tables')
            ->hasCommands([
                MakeWorkflowCommand::class,
                MakeActionCommand::class,
                FlowListCommand::class,
                FlowShowCommand::class,
                FlowCancelCommand::class,
                FlowSignalCommand::class,
                FlowPruneCommand::class,
                FlowMonitorCommand::class,
                FlowRepairCommand::class,
                FlowKickCommand::class,
            ]);
    }

    public function packageRegistered(): void
    {
        $this->app->singleton(FlowManager::class);
        $this->app->alias(FlowManager::class, 'saga-flow');

        $this->app->scoped(FlowRuntime::class);
        $this->app->singleton(FlowExecutor::class);
        $this->app->singleton(FlowMonitor::class);
        $this->app->singleton(FlowDoctor::class);

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

        // Opt-in: drive the monitor off the queue worker's idle loop, throttled in
        // FlowMonitor. Off by default — prefer Schedule::command('saga-flow:monitor').
        if (config('saga-lara-flow.monitor.queue_looping.enabled')) {
            Event::listen(Looping::class, [FlowMonitor::class, 'onQueueLooping']);
        }

        // Opt-in: drive the doctor's repair pass off the queue worker's idle loop,
        // throttled independently of the expiration sweep. Off by default — prefer
        // Schedule::command('saga-flow:repair').
        if (config('saga-lara-flow.repair.queue_looping.enabled')) {
            Event::listen(Looping::class, [FlowDoctor::class, 'onQueueLooping']);
        }
    }
}
