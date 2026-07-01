<?php

namespace Workbench\App\Actions;

use DiscoveryUkraine\SagaLaraFlow\Action;

class ShipOrder extends Action
{
    public function handle(string $orderId): string
    {
        return 'shipped:'.$orderId;
    }
}
