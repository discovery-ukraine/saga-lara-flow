<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\ResolvesMethodDependencies;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\StepExecution;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionClaimFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunActionJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Illuminate\Foundation\Bus\PendingDispatch;
use Throwable;

class ActionDispatcher
{
    use ResolvesMethodDependencies;

    public function __construct(
        private readonly ActionRecorder $recorder,
        private readonly Serializer $serializer,
    ) {}

    /**
     * @throws FlowSuspended
     * @throws Throwable
     */
    public function dispatch(FlowRun $flowRun, int $sequence, ActionSchedule $schedule): ActionRun
    {
        $actionRun = $this->recorder->scheduleAction($flowRun, $sequence, $schedule);

        try {
            $this->route(
                RunActionJob::dispatch($actionRun->id, $schedule->actionClass, $actionRun->retry_signal_attempts),
                $flowRun,
            );
        } catch (Throwable $thrown) {
            $this->replayIfRecorded($actionRun, $thrown);
        }

        return $actionRun;
    }

    /**
     * The job carries its cycle, so one left over from an earlier cycle does nothing.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    public function redispatch(ActionRun $actionRun): void
    {
        try {
            $this->route(
                RunActionJob::dispatch(
                    $actionRun->id,
                    $actionRun->action_class,
                    $actionRun->retry_signal_attempts,
                ),
                $actionRun->flowRun,
            );
        } catch (Throwable $thrown) {
            $this->replayIfRecorded($actionRun, $thrown);
        }
    }

    /**
     * The sync queue runs the job inside the dispatch and rethrows its failure once failed()
     * has run. A step that hook settled is resolved by replay; any other keeps the throw.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function replayIfRecorded(ActionRun $step, Throwable $thrown): never
    {
        $row = $step->newQuery()->useWritePdo()->find($step->getKey());

        // How RunActionJob::failed() leaves a step: out of queue attempts, and an optional
        // one with no retry policy given up as OptionalFailed.
        $settled = match ($row?->status) {
            ActionStatus::OptionalFailed => true,
            ActionStatus::Failed => $row->queue_attempts_exhausted
                && ! ($row->continue_on_failure && $row->retry_signal === null),
            default => false,
        };

        if ($settled) {
            app(FlowSuspender::class)->suspendInline('action', $step->sequence);
        }

        throw $thrown;
    }

    private function route(PendingDispatch $job, FlowRun $flowRun): void
    {
        if ($flowRun->connection !== null) {
            $job->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $job->onQueue($flowRun->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $job->afterCommit();
        }
    }

    /**
     * @throws Throwable
     */
    public function runInline(FlowRun $run, int $sequence, ActionSchedule $schedule): StepExecution
    {
        $actionRun = $this->recorder->scheduleAction($run, $sequence, $schedule);

        $execution = $this->execute($actionRun, inline: true);

        // A row created in this call cannot be claimed by anyone else: losing it is a broken invariant.
        if ($execution === StepExecution::ClaimLost) {
            throw ActionClaimFailedException::forAction($schedule->actionClass, $sequence);
        }

        return $execution;
    }

    /**
     * ClaimLost means nothing ran; Superseded means the work happened and its outcome was dropped.
     *
     * @throws Throwable
     */
    public function execute(
        ActionRun $actionRun,
        ?int $expectedRetryGeneration = null,
        bool $inline = false,
    ): StepExecution {
        return app(TenancyManager::class)->for(
            $actionRun->flowRun,
            $actionRun->action_class,
            function () use ($actionRun, $expectedRetryGeneration, $inline): StepExecution {
                if (! $this->recorder->startAction($actionRun, $expectedRetryGeneration)) {
                    return StepExecution::ClaimLost;
                }

                $instance = app()->make($actionRun->action_class);

                /** @var array<int, mixed> $arguments */
                $arguments = (array) $this->serializer->deserialize($actionRun->arguments ?? []);

                try {
                    $result = $this->callWithDependencies($instance, 'handle', $arguments);
                } catch (Throwable $e) {
                    // Superseded: rethrowing would let failed() write into a row this worker no longer owns.
                    if (! $this->recorder->failAction($actionRun, $e, $inline)) {
                        return StepExecution::Superseded;
                    }

                    throw $e;
                }

                return $this->recorder->completeAction($actionRun, $result)
                    ? StepExecution::Executed
                    : StepExecution::Superseded;
            },
        );
    }
}
