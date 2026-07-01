<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

use DiscoveryUkraine\SagaLaraFlow\Builders\ParallelBuilder;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;

/**
 * The parallel() DSL: a block of steps dispatched together.
 */
trait InteractsWithParallelism
{
    /**
     * Begin a parallel block: its steps are dispatched together and the flow
     * continues only once they all finish, returning their results in declaration
     * order. Choose failFast() (fail on the first failure) or waitAllThenFail()
     * (let every step settle first); steps may carry their own compensations.
     *
     * The eventual ->run() rethrows the failing step's own business exception
     * (e.g. ActionFailedException), which you may catch inside handle():
     *
     * @throws HistoryContractMismatchException handle() diverged from recorded history
     */
    public function parallel(): ParallelBuilder
    {
        return new ParallelBuilder($this->runtime);
    }
}
