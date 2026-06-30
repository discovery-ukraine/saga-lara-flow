<?php

namespace DiscoveryUkraine\SagaLaraFlow\Facades;

use DiscoveryUkraine\SagaLaraFlow\Builders\CreateWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\FlowHandle;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Queries\FlowQuery;
use Illuminate\Support\Facades\Facade;

/**
 * @method static CreateWorkflowBuilder create(string $workflowClass)
 * @method static FlowRun|null findRun(string $id)
 * @method static FlowHandle loadFlow(string $id)
 * @method static FlowQuery query()
 * @method static FlowRun kick(string $id)
 *
 * @see FlowManager
 */
class SagaFlow extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'saga-flow';
    }
}
