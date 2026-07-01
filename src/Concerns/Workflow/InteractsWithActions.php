<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns\Workflow;

use DiscoveryUkraine\SagaLaraFlow\Builders\ActionBuilder;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;

/**
 * The action() DSL: compensatable steps identified by (flow_run_id, sequence).
 */
trait InteractsWithActions
{
    /**
     * Begin a compensatable action step. The returned builder records and
     * replays the action by its (flow_run_id, sequence) identity.
     *
     * The eventual ->run() throws these business exceptions, which you may catch
     * inside handle() to react instead of letting the flow fail:
     *
     * @throws ActionFailedException the step exhausted its retries
     * @throws FlowExpiredException the step or run passed its deadline
     * @throws HistoryContractMismatchException handle() diverged from recorded history
     */
    public function action(string $actionClass, mixed ...$arguments): ActionBuilder
    {
        return new ActionBuilder(
            $this->runtime,
            $actionClass,
            array_values($arguments),
        );
    }

    /**
     * Convenience alias for action(...)->continueOnFailure(): a best-effort step
     * whose failure does not fail the flow (it lands OptionalFailed and run()
     * returns the fallback). Call ->fallbackValueOnFail() on the returned builder to set it.
     */
    public function optionalAction(string $actionClass, mixed ...$arguments): ActionBuilder
    {
        return $this->action($actionClass, ...$arguments)->continueOnFailure();
    }
}
