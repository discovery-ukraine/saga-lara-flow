<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use Throwable;

/**
 * A step that starts a saga of its own. Under sync execution it runs inside the
 * calling run's drive() pass, so the second run is nested in the first.
 */
final class StartsASagaAction extends Action
{
    /**
     * @return array{started: string}
     *
     * @throws Throwable
     */
    public function handle(): array
    {
        return ['started' => SagaFlow::create(OneActionWorkflow::class)->runSync()->id];
    }
}
