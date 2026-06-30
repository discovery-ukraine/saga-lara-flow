<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;
use DiscoveryUkraine\SagaLaraFlow\Attributes\ActionName;
use DiscoveryUkraine\SagaLaraFlow\Attributes\ActionTimeout;

/**
 * Declares its display name and wall-clock timeout via attributes. Used to prove
 * #[ActionName] lands on action_name and #[ActionTimeout] on the step's expires_at.
 */
#[ActionName('charge-card')]
#[ActionTimeout(seconds: 120)]
final class AttributedAction extends Action
{
    /**
     * @return array{ok: bool}
     */
    public function handle(): array
    {
        return ['ok' => true];
    }
}
