<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Action;

/**
 * Class-based compensation that records that it ran for a given label.
 */
final class UndoAction extends Action
{
    /**
     * @return array{undone: string}
     */
    public function handle(string $label): array
    {
        CompensationLog::record('undo:'.$label);

        return ['undone' => $label];
    }
}
