<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Attributes\Flow;
use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowQueue;
use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowTimeout;
use DiscoveryUkraine\SagaLaraFlow\Attributes\Tag;
use DiscoveryUkraine\SagaLaraFlow\Workflow;

/**
 * Declares queue/timeout/tags purely via attributes. Used to prove the attribute
 * layer feeds create() (queue/connection/timeout/version/name/tags) when no
 * explicit builder option overrides it.
 */
#[Flow(name: 'orders.checkout', version: 'v2')]
#[FlowQueue(connection: 'redis', queue: 'high')]
#[FlowTimeout(seconds: 3600)]
#[Tag('orders')]
#[Tag('team', 'checkout')]
final class AttributedWorkflow extends Workflow
{
    public function handle(): void {}
}
