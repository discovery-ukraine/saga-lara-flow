<?php

namespace Workbench\App\Actions;

use DiscoveryUkraine\SagaLaraFlow\Action;

class ReleaseStock extends Action
{
    public function handle(string $orderId): string
    {
        return 'released:'.$orderId;
    }
}
