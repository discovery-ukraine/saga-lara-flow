<?php

namespace Workbench\App\Actions;

use DiscoveryUkraine\SagaLaraFlow\Action;

class RefundCard extends Action
{
    public function handle(string $orderId): string
    {
        return 'refunded:'.$orderId;
    }
}
