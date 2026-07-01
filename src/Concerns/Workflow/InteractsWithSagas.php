<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

use DiscoveryUkraine\SagaLaraFlow\Builders\SagaBuilder;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CompensationFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;

/**
 * The saga() DSL: an explicit transactional group with shared compensation policy.
 */
trait InteractsWithSagas
{
    /**
     * Begin an explicit saga group: a transactional block of steps with shared
     * compensation policies (onCompensationFailure, compensateInParallel). Equivalent
     * in power to action-level compensation; use it for larger transactional blocks.
     *
     * The eventual ->run() rethrows the failing step's own business exception after
     * rolling the group back; a rollback that itself fails surfaces as
     * CompensationFailedException. You may catch either inside handle():
     *
     * @throws CompensationFailedException a compensation step failed during rollback
     * @throws HistoryContractMismatchException handle() diverged from recorded history
     */
    public function saga(): SagaBuilder
    {
        return new SagaBuilder($this->runtime);
    }
}
