<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;

/**
 * Trivial action that echoes its label back as a result.
 */
final class MakeValueAction extends Action
{
    /**
     * @return array{label: string}
     */
    public function handle(string $label): array
    {
        return ['label' => $label];
    }
}
