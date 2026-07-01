<?php

namespace Workbench\App\Actions;

use DiscoveryUkraine\SagaLaraFlow\Action;

class ChargeCard extends Action
{
    public int $tries = 3;

    /**
     * @return array{order: string, charge: string}
     */
    public function handle(string $orderId): array
    {
        return ['order' => $orderId, 'charge' => 'ch_'.$orderId];
    }
}
