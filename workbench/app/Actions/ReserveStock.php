<?php

namespace Workbench\App\Actions;

use DiscoveryUkraine\SagaLaraFlow\Action;

class ReserveStock extends Action
{
    public function handle(string $orderId): string
    {
        return 'reserved:'.$orderId;
    }
}
