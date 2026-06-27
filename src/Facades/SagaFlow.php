<?php

namespace DiscoveryUkraine\SagaLaraFlow\Facades;

use DiscoveryUkraine\SagaLaraFlow\Builders\CreateWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\FlowHandle;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Facades\Facade;

/**
 * @method static CreateWorkflowBuilder create(string $workflowClass)
 * @method static FlowRun|null findRun(string $id)
 * @method static FlowHandle run(string $id)
 * @method static Builder query()
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
