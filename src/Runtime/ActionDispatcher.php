<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\ResolvesMethodDependencies;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunActionJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Throwable;

/**
 * Schedules and executes action steps. In queued mode it persists a pending
 * ActionRun and dispatches a RunActionJob; in sync mode it runs the action
 * inline in the same process. Both paths persist identical ActionRun rows, so
 * the final database state is the same regardless of transport.
 */
class ActionDispatcher
{
    use ResolvesMethodDependencies;

    public function __construct(
        private readonly ActionRecorder $recorder,
        private readonly Serializer $serializer,
    ) {}

    /**
     * Queued mode: persist the pending step and dispatch its job.
     *
     * @param  array<int, mixed>  $arguments
     */
    public function dispatch(FlowRun $flowRun, int $sequence, string $actionClass, array $arguments): ActionRun
    {
        $actionRun = $this->recorder->scheduleAction($flowRun, $sequence, $actionClass, $arguments);

        $job = RunActionJob::dispatch($actionRun->id, $actionClass);

        if ($flowRun->connection !== null) {
            $job->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $job->onQueue($flowRun->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $job->afterCommit();
        }

        return $actionRun;
    }

    /**
     * Sync mode: persist the pending step and execute it inline.
     *
     * @param  array<int, mixed>  $arguments
     *
     * @throws Throwable
     */
    public function runInline(FlowRun $run, int $sequence, string $actionClass, array $arguments): ActionRun
    {
        $actionRun = $this->recorder->scheduleAction($run, $sequence, $actionClass, $arguments);

        $this->execute($actionRun);

        return $actionRun;
    }

    /**
     * Run a persisted action step to completion. Shared by sync inline execution
     * and the queued RunActionJob. A business failure marks the step Failed and
     * is rethrown so the workflow can react on replay.
     *
     * @throws Throwable
     */
    public function execute(ActionRun $actionRun): void
    {
        $this->recorder->startAction($actionRun);

        $instance = app()->make($actionRun->action_class);

        /** @var array<int, mixed> $arguments */
        $arguments = (array) $this->serializer->deserialize($actionRun->arguments ?? []);

        try {
            $result = $this->callWithDependencies($instance, 'handle', $arguments);
        } catch (Throwable $e) {
            $this->recorder->failAction($actionRun, $e);

            throw $e;
        }

        $this->recorder->completeAction($actionRun, $result);
    }
}
