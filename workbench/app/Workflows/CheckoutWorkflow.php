<?php

namespace Workbench\App\Workflows;

use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Throwable;
use Workbench\App\Actions\ChargeCard;
use Workbench\App\Actions\RefundCard;
use Workbench\App\Actions\ReleaseStock;
use Workbench\App\Actions\ReserveStock;
use Workbench\App\Actions\ShipOrder;

/**
 * A self-contained example saga used by `composer serve` and the docs. Each step
 * registers how to undo itself; if a later step throws, the engine rolls the
 * completed steps back in reverse order (ReleaseStock, then RefundCard).
 *
 * @return array{charge: mixed, shipment: mixed}
 */
class CheckoutWorkflow extends Workflow
{
    /**
     * @return array{charge: mixed, shipment: mixed}
     *
     * @throws Throwable
     */
    public function handle(string $orderId): array
    {
        $charge = $this->action(ChargeCard::class, $orderId)
            ->compensateWith(RefundCard::class, $orderId)
            ->run();

        $this->action(ReserveStock::class, $orderId)
            ->compensateWith(ReleaseStock::class, $orderId)
            ->run();

        $shipment = $this->action(ShipOrder::class, $orderId)->run();

        return ['charge' => $charge, 'shipment' => $shipment];
    }
}
