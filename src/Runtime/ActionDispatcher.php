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
     * Send a fresh RunActionJob for a step that is already persisted and has just
     * been rewound to Pending by a signal-gated retry. The row — and therefore its
     * (flow_run_id, sequence) ordinal, arguments and history — is reused as is. The
     * job carries the cycle it belongs to, so a job left over from an earlier cycle
     * recognises itself as stale and does nothing.
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

    /**
     * Put an action job on the run's own connection/queue, honouring the package's
     * after_commit setting. Shared by the first dispatch and by every retry.
     */
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
     * Sync mode: persist the pending step and execute it inline. Returns how far the
     * step got, so the caller can tell a broken invariant from a lost race.
     *
     * @throws Throwable
     */
    public function runInline(FlowRun $run, int $sequence, ActionSchedule $schedule): StepExecution
    {
        $actionRun = $this->recorder->scheduleAction($run, $sequence, $schedule);

        $execution = $this->execute($actionRun);

        // A row created in this very call is not reachable by anyone else yet, so
        // losing the claim is not a race but a broken invariant. Being superseded
        // afterwards is a race, and a real one: the monitor and the doctor run in
        // their own processes against the same rows a sync run creates.
        if ($execution === StepExecution::ClaimLost) {
            throw ActionClaimFailedException::forAction($schedule->actionClass, $sequence);
        }

        return $execution;
    }

    /**
     * Claim and run a persisted action step to completion. Shared by sync inline
     * execution and the queued RunActionJob.
     *
     * The two non-Executed results are both "someone else is handling this row", never
     * a failure of the step, but they are not interchangeable: ClaimLost means nothing
     * ran, while Superseded means the work happened and only its outcome was dropped.
     * A business failure marks the step Failed and is rethrown so the workflow can
     * react on replay.
     *
     * @throws Throwable
     */
    public function execute(ActionRun $actionRun, ?int $expectedRetryGeneration = null): StepExecution
    {
        return app(TenancyManager::class)->for(
            $actionRun->flowRun,
            $actionRun->action_class,
            function () use ($actionRun, $expectedRetryGeneration): StepExecution {
                if (! $this->recorder->startAction($actionRun, $expectedRetryGeneration)) {
                    return StepExecution::ClaimLost;
                }

                $instance = app()->make($actionRun->action_class);

                /** @var array<int, mixed> $arguments */
                $arguments = (array) $this->serializer->deserialize($actionRun->arguments ?? []);

                try {
                    $result = $this->callWithDependencies($instance, 'handle', $arguments);
                } catch (Throwable $e) {
                    // A rejected outcome means this executor was superseded while it
                    // ran, so its failure settles nothing. Rethrowing would fail a job
                    // whose work is already discarded, and RunActionJob::failed() would
                    // then write queue bookkeeping into a row it no longer owns.
                    if (! $this->recorder->failAction($actionRun, $e)) {
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
