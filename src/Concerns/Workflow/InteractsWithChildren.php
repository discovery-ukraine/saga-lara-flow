<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

use DiscoveryUkraine\SagaLaraFlow\Builders\ChildWorkflowBuilder;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowCancelledException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ChildWorkflowFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;

/**
 * The child() DSL: nested workflows awaited by their (flow_run_id, sequence) identity.
 */
trait InteractsWithChildren
{
    /**
     * Begin a child workflow step. The returned builder starts the child and awaits
     * its completion, resolving by the operation's (flow_run_id, sequence) identity.
     *
     * The eventual ->run() throws these business exceptions (unless you call
     * ->continueParentOnFailure()), which you may catch inside handle():
     *
     * @param  array<int, mixed>  $arguments
     *
     * @throws ChildWorkflowFailedException the child ended in Failed
     * @throws ChildWorkflowCancelledException the child was cancelled
     * @throws HistoryContractMismatchException handle() diverged from recorded history
     */
    public function child(string $workflowClass, array $arguments = []): ChildWorkflowBuilder
    {
        return new ChildWorkflowBuilder($this->runtime, $workflowClass, array_values($arguments));
    }
}
